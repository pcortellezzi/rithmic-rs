use async_trait::async_trait;
use tracing::{event, Level};

use crate::{
    api::{
        RithmicConnectionInfo,
        receiver_api::{RithmicReceiverApi, RithmicResponse},
        sender_api::RithmicSenderApi,
    },
    request_handler::{RithmicRequest, RithmicRequestHandler},
    rti::{request_login::SysInfraType, request_pn_l_position_updates},
    ws::{get_heartbeat_interval, PlantActor, RithmicStream, connect},
};

use futures_util::{
    stream::{SplitSink, SplitStream},
    SinkExt, StreamExt,
};

use tokio::{
    net::TcpStream,
    sync::{broadcast::Sender, oneshot},
    time::Interval,
};

use tokio_tungstenite::{
    connect_async,
    tungstenite::{Error, Message},
    WebSocketStream,
    MaybeTlsStream
};

pub enum PnlPlantCommand {
    Close,
    Login {
        response_sender: oneshot::Sender<Result<Vec<RithmicResponse>, String>>,
    },
    SetLogin,
    Logout {
        response_sender: oneshot::Sender<Result<Vec<RithmicResponse>, String>>,
    },
    PnlPositionSnapshots {
        response_sender: oneshot::Sender<Result<Vec<RithmicResponse>, String>>,
    },
    SendHeartbeat {},
    SubscribePnlUpdates {
        response_sender: oneshot::Sender<Result<Vec<RithmicResponse>, String>>,
    },
}

pub struct RithmicPnlPlant {
    pub connection_handle: tokio::task::JoinHandle<()>,
    sender: tokio::sync::mpsc::Sender<PnlPlantCommand>,
    subscription_sender: Sender<RithmicResponse>,
}

impl RithmicPnlPlant {
    pub async fn new(conn_info: &RithmicConnectionInfo) -> RithmicPnlPlant {
        let (req_tx, req_rx) = tokio::sync::mpsc::channel::<PnlPlantCommand>(32);
        let (sub_tx, _sub_rx) = tokio::sync::broadcast::channel(1024);

        let mut pnl_plant = PnlPlant::new(req_rx, sub_tx.clone(), conn_info)
            .await
            .unwrap();

        let connection_handle = tokio::spawn(async move {
            pnl_plant.run().await;
        });

        RithmicPnlPlant {
            connection_handle,
            sender: req_tx,
            subscription_sender: sub_tx,
        }
    }
}

impl RithmicStream for RithmicPnlPlant {
    type Handle = RithmicPnlPlantHandle;

    fn get_handle(&self) -> Self::Handle {
        RithmicPnlPlantHandle {
            sender: self.sender.clone(),
            subscription_receiver: self.subscription_sender.subscribe(),
        }
    }
}

#[derive(Debug)]
pub struct PnlPlant {
    config: RithmicConnectionInfo,
    interval: Interval,
    logged_in: bool,
    request_handler: RithmicRequestHandler,
    request_receiver: tokio::sync::mpsc::Receiver<PnlPlantCommand>,
    rithmic_reader: SplitStream<WebSocketStream<MaybeTlsStream<TcpStream>>>,
    rithmic_receiver_api: RithmicReceiverApi,
    rithmic_sender: SplitSink<
        WebSocketStream<MaybeTlsStream<TcpStream>>,
        Message,
    >,
    rithmic_sender_api: RithmicSenderApi,
    subscription_sender: Sender<RithmicResponse>,
}

impl PnlPlant {
    async fn new(
        request_receiver: tokio::sync::mpsc::Receiver<PnlPlantCommand>,
        subscription_sender: Sender<RithmicResponse>,
        conn_info: &RithmicConnectionInfo,
    ) -> Result<PnlPlant, ()> {
        let config = conn_info.clone();

        let ws_stream = connect(&config.url).await.unwrap();
        let (rithmic_sender, rithmic_reader) = ws_stream.split();
        let rithmic_sender_api = RithmicSenderApi::new(&config);
        let rithmic_receiver_api = RithmicReceiverApi {
            source: "pnl_plant".to_string(),
        };

        let interval = get_heartbeat_interval();

        Ok(PnlPlant {
            config,
            interval,
            logged_in: false,
            request_handler: RithmicRequestHandler::new(),
            request_receiver,
            rithmic_reader,
            rithmic_receiver_api,
            rithmic_sender_api,
            rithmic_sender,
            subscription_sender,
        })
    }
}

#[async_trait]
impl PlantActor for PnlPlant {
    type Command = PnlPlantCommand;

    async fn run(&mut self) {
        loop {
            tokio::select! {
                _ = self.interval.tick() => {
                    self.handle_command(PnlPlantCommand::SendHeartbeat {}).await;
                }
                Some(message) = self.request_receiver.recv() => {
                    self.handle_command(message).await;
                }
                Some(message) = self.rithmic_reader.next() => {
                    let stop = self.handle_rithmic_message(message).await.unwrap();

                    if stop {
                        break;
                    }
                }
                else => { break; }
            }
        }
    }

    async fn handle_rithmic_message(
        &mut self,
        message: Result<Message, Error>,
    ) -> Result<bool, ()> {
        let mut stop = false;

        match message {
            Ok(Message::Close(frame)) => {
                event!(Level::INFO, "pnl_plant: Received close frame: {:?}", frame);
                stop = true;
            }
            Ok(Message::Binary(data)) => match self.rithmic_receiver_api.buf_to_message(data) {
                Ok(response) => {
                    if response.is_update {
                        match self.subscription_sender.send(response) {
                            Ok(_) => {}
                            Err(e) => {
                                event!(Level::ERROR, "failed to send response {:?}", e);
                            }
                        };
                    } else {
                        self.request_handler.handle_response(response);
                    }
                }
                Err(err) => {
                    event!(Level::ERROR, "received an error message {:?}", err);
                }
            },
            Err(Error::ConnectionClosed) => {
                event!(Level::INFO, "Connection closed");
                stop = true;
            }
            _ => {
                event!(Level::WARN, "Unhandled message: {:?}", message);
            }
        }

        Ok(stop)
    }

    async fn handle_command(&mut self, command: PnlPlantCommand) {
        match command {
            PnlPlantCommand::Close => {
                self.rithmic_sender
                    .send(Message::Close(None))
                    .await
                    .unwrap();
            }
            PnlPlantCommand::Login { response_sender } => {
                let (login_buf, id) = self.rithmic_sender_api.request_login(
                    &self.config.system_name,
                    SysInfraType::PnlPlant,
                    &self.config.user,
                    &self.config.password,
                );

                event!(Level::INFO, "pnl_plant: sending login request {}", id);

                self.request_handler.register_request(RithmicRequest {
                    request_id: id,
                    responder: response_sender,
                });

                self.rithmic_sender
                    .send(Message::Binary(login_buf))
                    .await
                    .unwrap();
            }
            PnlPlantCommand::SetLogin => {
                self.logged_in = true;
            }
            PnlPlantCommand::Logout { response_sender } => {
                let (logout_buf, id) = self.rithmic_sender_api.request_logout();

                self.request_handler.register_request(RithmicRequest {
                    request_id: id,
                    responder: response_sender,
                });

                self.rithmic_sender
                    .send(Message::Binary(logout_buf))
                    .await
                    .unwrap();
            }
            PnlPlantCommand::SendHeartbeat {} => {
                let (heartbeat_buf, _id) = self.rithmic_sender_api.request_heartbeat();

                let _ = self
                    .rithmic_sender
                    .send(Message::Binary(heartbeat_buf))
                    .await;
            }
            PnlPlantCommand::SubscribePnlUpdates { response_sender } => {
                let (subscribe_buf, id) = self.rithmic_sender_api.request_pnl_position_updates(
                    request_pn_l_position_updates::Request::Subscribe,
                );

                self.request_handler.register_request(RithmicRequest {
                    request_id: id,
                    responder: response_sender,
                });

                self.rithmic_sender
                    .send(Message::Binary(subscribe_buf))
                    .await
                    .unwrap();
            }
            PnlPlantCommand::PnlPositionSnapshots { response_sender } => {
                let (snapshot_buf, id) = self.rithmic_sender_api.request_pnl_position_snapshot();

                self.request_handler.register_request(RithmicRequest {
                    request_id: id,
                    responder: response_sender,
                });

                self.rithmic_sender
                    .send(Message::Binary(snapshot_buf))
                    .await
                    .unwrap();
            }
        }
    }
}

pub struct RithmicPnlPlantHandle {
    sender: tokio::sync::mpsc::Sender<PnlPlantCommand>,
    pub subscription_receiver: tokio::sync::broadcast::Receiver<RithmicResponse>,
}

impl RithmicPnlPlantHandle {
    pub async fn login(&self) -> Result<RithmicResponse, String> {
        event!(Level::INFO, "pnl_plant: logging in");

        let (tx, rx) = oneshot::channel::<Result<Vec<RithmicResponse>, String>>();

        let command = PnlPlantCommand::Login {
            response_sender: tx,
        };

        let _ = self.sender.send(command).await;
        let response = rx.await.unwrap().unwrap().remove(0);

        if response.error.is_none() {
            let _ = self.sender.send(PnlPlantCommand::SetLogin).await;

            event!(Level::INFO, "pnl_plant: logged in");

            Ok(response)
        } else {
            event!(Level::ERROR, "pnl_plant: login failed {:?}", response.error);

            Err(response.error.unwrap())
        }
    }

    pub async fn disconnect(&self) -> Result<RithmicResponse, String> {
        let (tx, rx) = oneshot::channel::<Result<Vec<RithmicResponse>, String>>();

        let command = PnlPlantCommand::Logout {
            response_sender: tx,
        };

        let _ = self.sender.send(command).await;
        let mut r = rx.await.unwrap().unwrap();
        let _ = self.sender.send(PnlPlantCommand::Close).await;

        Ok(r.remove(0))
    }

    pub async fn subscribe_pnl_updates(&self) -> Result<RithmicResponse, String> {
        let (tx, rx) = oneshot::channel::<Result<Vec<RithmicResponse>, String>>();

        let command = PnlPlantCommand::SubscribePnlUpdates {
            response_sender: tx,
        };

        let _ = self.sender.send(command).await;

        Ok(rx.await.unwrap().unwrap().remove(0))
    }

    pub async fn pnl_position_snapshots(&self) -> Result<RithmicResponse, String> {
        let (tx, rx) = oneshot::channel::<Result<Vec<RithmicResponse>, String>>();

        let command = PnlPlantCommand::PnlPositionSnapshots {
            response_sender: tx,
        };

        let _ = self.sender.send(command).await;

        Ok(rx.await.unwrap().unwrap().remove(0))
    }
}
