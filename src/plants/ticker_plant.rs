use async_trait::async_trait;
use tracing::{event, Level};

use crate::{
    api::{
        RithmicConnectionInfo,
        receiver_api::{RithmicReceiverApi, RithmicResponse},
        sender_api::RithmicSenderApi,
    },
    request_handler::{RithmicRequest, RithmicRequestHandler},
    rti::{
        request_login::SysInfraType,
        request_market_data_update::{Request, UpdateBits},
    },
    ws::{get_heartbeat_interval, PlantActor, RithmicStream, connect},
};

use futures_util::{
    stream::{SplitSink, SplitStream},
    SinkExt, StreamExt,
};

use tokio_tungstenite::{
    connect_async,
    tungstenite::{Error, Message},
    WebSocketStream,
    MaybeTlsStream
};

use tokio::{
    net::TcpStream,
    sync::{broadcast::Sender, oneshot},
    time::Interval,
};

pub enum TickerPlantCommand {
    Close,
    Login {
        response_sender: oneshot::Sender<Result<Vec<RithmicResponse>, String>>,
    },
    SetLogin,
    Logout {
        response_sender: oneshot::Sender<Result<Vec<RithmicResponse>, String>>,
    },
    SendHeartbeat {},
    Subscribe {
        symbol: String,
        exchange: String,
        fields: Vec<UpdateBits>,
        request_type: Request,
        response_sender: oneshot::Sender<Result<Vec<RithmicResponse>, String>>,
    },
}

pub struct RithmicTickerPlant {
    pub connection_handle: tokio::task::JoinHandle<()>,
    sender: tokio::sync::mpsc::Sender<TickerPlantCommand>,
    subscription_sender: Sender<RithmicResponse>,
}

impl RithmicTickerPlant {
    pub async fn new(conn_info: &RithmicConnectionInfo) -> RithmicTickerPlant {
        let (req_tx, req_rx) = tokio::sync::mpsc::channel::<TickerPlantCommand>(32);
        let (sub_tx, _sub_rx) = tokio::sync::broadcast::channel(1024);

        let mut ticker_plant = TickerPlant::new(req_rx, sub_tx.clone(), conn_info)
            .await
            .unwrap();

        let connection_handle = tokio::spawn(async move {
            ticker_plant.run().await;
        });

        RithmicTickerPlant {
            connection_handle,
            sender: req_tx,
            subscription_sender: sub_tx,
        }
    }
}

impl RithmicStream for RithmicTickerPlant {
    type Handle = RithmicTickerPlantHandle;

    fn get_handle(&self) -> RithmicTickerPlantHandle {
        RithmicTickerPlantHandle {
            sender: self.sender.clone(),
            subscription_sender: self.subscription_sender.clone(),
            subscription_receiver: self.subscription_sender.subscribe(),
        }
    }
}

#[derive(Debug)]
pub struct TickerPlant {
    config: RithmicConnectionInfo,
    interval: Interval,
    logged_in: bool,
    request_handler: RithmicRequestHandler,
    request_receiver: tokio::sync::mpsc::Receiver<TickerPlantCommand>,
    rithmic_reader: SplitStream<WebSocketStream<MaybeTlsStream<TcpStream>>>,
    rithmic_receiver_api: RithmicReceiverApi,
    rithmic_sender: SplitSink<
        WebSocketStream<MaybeTlsStream<TcpStream>>,
        Message,
    >,

    rithmic_sender_api: RithmicSenderApi,
    subscription_sender: Sender<RithmicResponse>,
}

impl TickerPlant {
    async fn new(
        request_receiver: tokio::sync::mpsc::Receiver<TickerPlantCommand>,
        subscription_sender: Sender<RithmicResponse>,
        conn_info: &RithmicConnectionInfo,
    ) -> Result<TickerPlant, ()> {
        let config = conn_info.clone();

        let ws_stream = connect(&config.url).await.unwrap();
        let (rithmic_sender, rithmic_reader) = ws_stream.split();
        let rithmic_sender_api = RithmicSenderApi::new(&config);
        let rithmic_receiver_api = RithmicReceiverApi {
            source: "ticker_plant".to_string(),
        };

        let interval = get_heartbeat_interval();

        Ok(TickerPlant {
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
impl PlantActor for TickerPlant {
    type Command = TickerPlantCommand;

    /// Execute the ticker plant in its own thread
    /// We will listen for messages from request_receiver and forward them to Rithmic
    /// while also listening for messages from Rithmic and forwarding them to subscription_sender
    /// or request handler
    async fn run(&mut self) {
        loop {
            tokio::select! {
                _ = self.interval.tick() => {
                    if self.logged_in {
                        self.handle_command(TickerPlantCommand::SendHeartbeat {}).await;
                    }
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
                else => { break }
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
                event!(
                    Level::INFO,
                    "ticker_plant received close frame: {:?}",
                    frame
                );

                stop = true;
            }
            Ok(Message::Binary(data)) => {
                let response = self.rithmic_receiver_api.buf_to_message(data).unwrap();

                if response.is_update {
                    self.subscription_sender.send(response).unwrap();
                } else {
                    self.request_handler.handle_response(response);
                }
            }
            Err(Error::ConnectionClosed) => {
                event!(Level::INFO, "ticker_plant connection closed");

                stop = true;
            }
            _ => {
                event!(
                    Level::WARN,
                    "ticker_plant received unknown message {:?}",
                    message
                );
            }
        }

        Ok(stop)
    }

    async fn handle_command(&mut self, command: TickerPlantCommand) {
        match command {
            TickerPlantCommand::Close => {
                self.rithmic_sender
                    .send(Message::Close(None))
                    .await
                    .unwrap();
            }
            TickerPlantCommand::Login { response_sender } => {
                let (login_buf, id) = self.rithmic_sender_api.request_login(
                    &self.config.system_name,
                    SysInfraType::TickerPlant,
                    &self.config.user,
                    &self.config.password,
                );

                event!(Level::INFO, "ticker_plant: sending login request {}", id);

                self.request_handler.register_request(RithmicRequest {
                    request_id: id,
                    responder: response_sender,
                });

                self.rithmic_sender
                    .send(Message::Binary(login_buf))
                    .await
                    .unwrap();
            }
            TickerPlantCommand::SetLogin => {
                self.logged_in = true;
            }
            TickerPlantCommand::Logout { response_sender } => {
                let (logout_buf, _id) = self.rithmic_sender_api.request_logout();

                self.request_handler.register_request(RithmicRequest {
                    request_id: _id,
                    responder: response_sender,
                });

                self.rithmic_sender
                    .send(Message::Binary(logout_buf))
                    .await
                    .unwrap();
            }
            TickerPlantCommand::SendHeartbeat {} => {
                let (heartbeat_buf, _id) = self.rithmic_sender_api.request_heartbeat();

                let _ = self
                    .rithmic_sender
                    .send(Message::Binary(heartbeat_buf))
                    .await;
            }
            TickerPlantCommand::Subscribe {
                symbol,
                exchange,
                fields,
                request_type,
                response_sender,
            } => {
                let (sub_buf, id) = self.rithmic_sender_api.request_market_data_update(
                    &symbol,
                    &exchange,
                    fields,
                    request_type,
                );

                self.request_handler.register_request(RithmicRequest {
                    request_id: id,
                    responder: response_sender,
                });

                self.rithmic_sender
                    .send(Message::Binary(sub_buf))
                    .await
                    .unwrap();
            }
        }
    }
}

pub struct RithmicTickerPlantHandle {
    sender: tokio::sync::mpsc::Sender<TickerPlantCommand>,
    // Used for cloning
    subscription_sender: tokio::sync::broadcast::Sender<RithmicResponse>,
    pub subscription_receiver: tokio::sync::broadcast::Receiver<RithmicResponse>,
}

impl RithmicTickerPlantHandle {
    pub async fn login(&self) -> Result<RithmicResponse, String> {
        event!(Level::INFO, "ticker_plant: logging in");

        let (tx, rx) = oneshot::channel::<Result<Vec<RithmicResponse>, String>>();

        let command = TickerPlantCommand::Login {
            response_sender: tx,
        };

        let _ = self.sender.send(command).await;
        let response = rx.await.unwrap().unwrap().remove(0);

        if response.error.is_none() {
            let _ = self.sender.send(TickerPlantCommand::SetLogin).await;

            event!(Level::INFO, "ticker_plant: logged in");

            Ok(response)
        } else {
            event!(
                Level::ERROR,
                "ticker_plant: login failed {:?}",
                response.error
            );

            Err(response.error.unwrap())
        }
    }

    pub async fn disconnect(&self) -> Result<RithmicResponse, String> {
        let (tx, rx) = oneshot::channel::<Result<Vec<RithmicResponse>, String>>();

        let command = TickerPlantCommand::Logout {
            response_sender: tx,
        };

        let _ = self.sender.send(command).await;
        let mut r = rx.await.unwrap().unwrap();
        let _ = self.sender.send(TickerPlantCommand::Close).await;
        let response = r.remove(0);

        self.subscription_sender.send(response.clone()).unwrap();

        Ok(response)
    }

    pub async fn subscribe(&self, symbol: &str, exchange: &str) -> Result<RithmicResponse, String> {
        let (tx, rx) = oneshot::channel::<Result<Vec<RithmicResponse>, String>>();

        let command = TickerPlantCommand::Subscribe {
            symbol: symbol.to_string(),
            exchange: exchange.to_string(),
            fields: vec![UpdateBits::LastTrade, UpdateBits::Bbo],
            request_type: Request::Subscribe,
            response_sender: tx,
        };

        let _ = self.sender.send(command).await;

        Ok(rx.await.unwrap().unwrap().remove(0))
    }
}

impl Clone for RithmicTickerPlantHandle {
    fn clone(&self) -> Self {
        RithmicTickerPlantHandle {
            sender: self.sender.clone(),
            subscription_sender: self.subscription_sender.clone(),
            subscription_receiver: self.subscription_sender.subscribe(),
        }
    }
}
