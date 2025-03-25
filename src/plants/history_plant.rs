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
        *,
        request_login::SysInfraType,
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
use crate::plants::ticker_plant::TickerPlantCommand;

pub enum HistoryPlantCommand {
    Close,
    GetHistoricalTickBar {
        symbol: String,
        exchange: String,
        bar_type: request_tick_bar_replay::BarType,
        bar_sub_type: request_tick_bar_replay::BarSubType,
        bar_type_specifier: String,
        start_index: i32,
        finish_index: i32,
        direction: request_tick_bar_replay::Direction,
        time_order: request_tick_bar_replay::TimeOrder,
        response_sender: oneshot::Sender<Result<Vec<RithmicResponse>, String>>,
    },
    GetHistoricalTimeBar {
        symbol: String,
        exchange: String,
        bar_type: request_time_bar_replay::BarType,
        bar_type_period: i32,
        start_index: i32,
        finish_index: i32,
        direction: request_time_bar_replay::Direction,
        time_order: request_time_bar_replay::TimeOrder,
        response_sender: oneshot::Sender<Result<Vec<RithmicResponse>, String>>,
    },
    Login {
        response_sender: oneshot::Sender<Result<Vec<RithmicResponse>, String>>,
    },
    Logout {
        response_sender: oneshot::Sender<Result<Vec<RithmicResponse>, String>>,
    },
    SendHeartbeat {},
    SetLogin,
    SubscribeTickBar {
        symbol: String,
        exchange: String,
        bar_type: request_tick_bar_update::BarType,
        bar_sub_type: request_tick_bar_update::BarSubType,
        bar_type_specifier: String,
        request_type: request_tick_bar_update::Request,
        response_sender: oneshot::Sender<Result<Vec<RithmicResponse>, String>>,
    },
    SubscribeTimeBar {
        symbol: String,
        exchange: String,
        bar_type: request_time_bar_update::BarType,
        bar_type_period: i32,
        request_type: request_time_bar_update::Request,
        response_sender: oneshot::Sender<Result<Vec<RithmicResponse>, String>>,
    },
}

pub struct RithmicHistoryPlant {
    pub connection_handle: tokio::task::JoinHandle<()>,
    sender: tokio::sync::mpsc::Sender<HistoryPlantCommand>,
    subscription_sender: Sender<RithmicResponse>,
}

impl RithmicHistoryPlant {
    pub async fn new(conn_info: &RithmicConnectionInfo) -> RithmicHistoryPlant {
        let (req_tx, req_rx) = tokio::sync::mpsc::channel::<HistoryPlantCommand>(32);
        let (sub_tx, _sub_rx) = tokio::sync::broadcast::channel(1024);

        let mut history_plant = HistoryPlant::new(req_rx, sub_tx.clone(), conn_info)
            .await
            .unwrap();

        let connection_handle = tokio::spawn(async move {
            history_plant.run().await;
        });

        RithmicHistoryPlant {
            connection_handle,
            sender: req_tx,
            subscription_sender: sub_tx,
        }
    }
}

impl RithmicStream for RithmicHistoryPlant {
    type Handle = RithmicHistoryPlantHandle;

    fn get_handle(&self) -> RithmicHistoryPlantHandle {
        RithmicHistoryPlantHandle {
            sender: self.sender.clone(),
            subscription_sender: self.subscription_sender.clone(),
            subscription_receiver: self.subscription_sender.subscribe(),
        }
    }
}

#[derive(Debug)]
pub struct HistoryPlant {
    config: RithmicConnectionInfo,
    interval: Interval,
    logged_in: bool,
    request_handler: RithmicRequestHandler,
    request_receiver: tokio::sync::mpsc::Receiver<HistoryPlantCommand>,
    rithmic_reader: SplitStream<WebSocketStream<MaybeTlsStream<TcpStream>>>,
    rithmic_receiver_api: RithmicReceiverApi,
    rithmic_sender: SplitSink<
        WebSocketStream<MaybeTlsStream<TcpStream>>,
        Message,
    >,

    rithmic_sender_api: RithmicSenderApi,
    subscription_sender: Sender<RithmicResponse>,
}

impl HistoryPlant {
    async fn new(
        request_receiver: tokio::sync::mpsc::Receiver<HistoryPlantCommand>,
        subscription_sender: Sender<RithmicResponse>,
        conn_info: &RithmicConnectionInfo,
    ) -> Result<HistoryPlant, ()> {
        let config = conn_info.clone();

        let ws_stream = connect(&config.url).await.unwrap();
        let (rithmic_sender, rithmic_reader) = ws_stream.split();
        let rithmic_sender_api = RithmicSenderApi::new(&config);
        let rithmic_receiver_api = RithmicReceiverApi {
            source: "history_plant".to_string(),
        };

        let interval = get_heartbeat_interval();

        Ok(HistoryPlant {
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
impl PlantActor for HistoryPlant {
    type Command = HistoryPlantCommand;

    /// Execute the history plant in its own thread
    /// We will listen for messages from request_receiver and forward them to Rithmic
    /// while also listening for messages from Rithmic and forwarding them to subscription_sender
    /// or request handler
    async fn run(&mut self) {
        loop {
            tokio::select! {
                _ = self.interval.tick() => {
                    if self.logged_in {
                        self.handle_command(HistoryPlantCommand::SendHeartbeat {}).await;
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
                    "history_plant received close frame: {:?}",
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
                event!(Level::INFO, "history_plant connection closed");

                stop = true;
            }
            _ => {
                event!(
                    Level::WARN,
                    "history_plant received unknown message {:?}",
                    message
                );
            }
        }

        Ok(stop)
    }

    async fn handle_command(&mut self, command: HistoryPlantCommand) {
        match command {
            HistoryPlantCommand::Close => {
                self.rithmic_sender
                    .send(Message::Close(None))
                    .await
                    .unwrap();
            }
            HistoryPlantCommand::GetHistoricalTickBar {
                symbol,
                exchange,
                bar_type,
                bar_sub_type,
                bar_type_specifier,
                start_index,
                finish_index,
                direction,
                time_order,
                response_sender,
            } => {
                let (sub_buf, id) = self.rithmic_sender_api.request_tick_bar_replay(
                    &symbol,
                    &exchange,
                    bar_type,
                    bar_sub_type,
                    &bar_type_specifier,
                    start_index,
                    finish_index,
                    direction,
                    time_order,
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
            HistoryPlantCommand::GetHistoricalTimeBar {
                symbol,
                exchange,
                bar_type,
                bar_type_period,
                start_index,
                finish_index,
                direction,
                time_order,
                response_sender,
            } => {
                let (sub_buf, id) = self.rithmic_sender_api.request_time_bar_replay(
                    &symbol,
                    &exchange,
                    bar_type,
                    bar_type_period,
                    start_index,
                    finish_index,
                    direction,
                    time_order,
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
            HistoryPlantCommand::Login { response_sender } => {
                let (login_buf, id) = self.rithmic_sender_api.request_login(
                    &self.config.system_name,
                    SysInfraType::HistoryPlant,
                    &self.config.user,
                    &self.config.password,
                );

                event!(Level::INFO, "history_plant: sending login request {}", id);

                self.request_handler.register_request(RithmicRequest {
                    request_id: id,
                    responder: response_sender,
                });

                self.rithmic_sender
                    .send(Message::Binary(login_buf))
                    .await
                    .unwrap();
            }
            HistoryPlantCommand::Logout { response_sender } => {
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
            HistoryPlantCommand::SendHeartbeat {} => {
                let (heartbeat_buf, _id) = self.rithmic_sender_api.request_heartbeat();

                let _ = self
                    .rithmic_sender
                    .send(Message::Binary(heartbeat_buf))
                    .await;
            }
            HistoryPlantCommand::SetLogin => {
                self.logged_in = true;
            }
            HistoryPlantCommand::SubscribeTickBar {
                symbol,
                exchange,
                bar_type,
                bar_sub_type,
                bar_type_specifier,
                request_type,
                response_sender,
            } => {
                let (sub_buf, id) = self.rithmic_sender_api.request_tick_bar_update(
                    &symbol,
                    &exchange,
                    bar_type,
                    bar_sub_type,
                    &bar_type_specifier,
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
            HistoryPlantCommand::SubscribeTimeBar {
                symbol,
                exchange,
                bar_type,
                bar_type_period,
                request_type,
                response_sender,
            } => {
                let (sub_buf, id) = self.rithmic_sender_api.request_time_bar_update(
                    &symbol,
                    &exchange,
                    bar_type,
                    bar_type_period,
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

pub struct RithmicHistoryPlantHandle {
    sender: tokio::sync::mpsc::Sender<HistoryPlantCommand>,
    // Used for cloning
    subscription_sender: tokio::sync::broadcast::Sender<RithmicResponse>,
    pub subscription_receiver: tokio::sync::broadcast::Receiver<RithmicResponse>,
}

impl RithmicHistoryPlantHandle {
    pub async fn login(&self) -> Result<RithmicResponse, String> {
        event!(Level::INFO, "history_plant: logging in");

        let (tx, rx) = oneshot::channel::<Result<Vec<RithmicResponse>, String>>();

        let command = HistoryPlantCommand::Login {
            response_sender: tx,
        };

        let _ = self.sender.send(command).await;
        let response = rx.await.unwrap()?.remove(0);

        if response.error.is_none() {
            let _ = self.sender.send(HistoryPlantCommand::SetLogin).await;

            event!(Level::INFO, "history_plant: logged in");

            Ok(response)
        } else {
            event!(
                Level::ERROR,
                "history_plant: login failed {:?}",
                response.error
            );

            Err(response.error.unwrap())
        }
    }

    pub async fn disconnect(&self) -> Result<RithmicResponse, String> {
        let (tx, rx) = oneshot::channel::<Result<Vec<RithmicResponse>, String>>();

        let command = HistoryPlantCommand::Logout {
            response_sender: tx,
        };

        let _ = self.sender.send(command).await;
        let mut r = rx.await.unwrap()?;
        let _ = self.sender.send(HistoryPlantCommand::Close).await;
        let response = r.remove(0);

        self.subscription_sender.send(response.clone()).unwrap();

        Ok(response)
    }

    pub async fn get_historical_tick_bar(
        &self,
        symbol: String,
        exchange: String,
        bar_type: request_tick_bar_replay::BarType,
        bar_sub_type: request_tick_bar_replay::BarSubType,
        bar_type_specifier: String,
        start_index: i32,
        finish_index: i32,
        direction: request_tick_bar_replay::Direction,
        time_order: request_tick_bar_replay::TimeOrder,
    ) -> Result<Vec<RithmicResponse>, String> {
        let (tx, rx) = oneshot::channel::<Result<Vec<RithmicResponse>, String>>();

        let command = HistoryPlantCommand::GetHistoricalTickBar {
            symbol,
            exchange,
            bar_type,
            bar_sub_type,
            bar_type_specifier,
            start_index,
            finish_index,
            direction,
            time_order,
            response_sender: tx,
        };

        let _ = self.sender.send(command).await;

        Ok(rx.await.unwrap()?)
    }

    pub async fn get_historical_time_bar(
        &self,
        symbol: String,
        exchange: String,
        bar_type: request_time_bar_replay::BarType,
        bar_type_period: i32,
        start_index: i32,
        finish_index: i32,
        direction: request_time_bar_replay::Direction,
        time_order: request_time_bar_replay::TimeOrder,
    ) -> Result<Vec<RithmicResponse>, String> {
        let (tx, rx) = oneshot::channel::<Result<Vec<RithmicResponse>, String>>();

        let command = HistoryPlantCommand::GetHistoricalTimeBar {
            symbol,
            exchange,
            bar_type,
            bar_type_period,
            start_index,
            finish_index,
            direction,
            time_order,
            response_sender: tx,
        };

        let _ = self.sender.send(command).await;

        Ok(rx.await.unwrap()?)
    }

    pub async fn subscribe_tick_bar(
        &self,
        symbol: &str,
        exchange: &str,
        bar_type: request_tick_bar_update::BarType,
        bar_sub_type: request_tick_bar_update::BarSubType,
        bar_type_specifier: &str,
    ) -> Result<RithmicResponse, String> {
        let (tx, rx) = oneshot::channel::<Result<Vec<RithmicResponse>, String>>();

        let command = HistoryPlantCommand::SubscribeTickBar {
            symbol: symbol.to_string(),
            exchange: exchange.to_string(),
            bar_type,
            bar_sub_type,
            bar_type_specifier: bar_type_specifier.to_string(),
            request_type: request_tick_bar_update::Request::Subscribe,
            response_sender: tx,
        };

        let _ = self.sender.send(command).await;

        Ok(rx.await.unwrap()?.remove(0))
    }

    pub async fn subscribe_time_bar(
        &self,
        symbol: &str,
        exchange: &str,
        bar_type: request_time_bar_update::BarType,
        bar_type_period: i32
    ) -> Result<RithmicResponse, String> {
        let (tx, rx) = oneshot::channel::<Result<Vec<RithmicResponse>, String>>();

        let command = HistoryPlantCommand::SubscribeTimeBar {
            symbol: symbol.to_string(),
            exchange: exchange.to_string(),
            bar_type,
            bar_type_period,
            request_type: request_time_bar_update::Request::Subscribe,
            response_sender: tx,
        };

        let _ = self.sender.send(command).await;

        Ok(rx.await.unwrap()?.remove(0))
    }
}

impl Clone for RithmicHistoryPlantHandle {
    fn clone(&self) -> Self {
        RithmicHistoryPlantHandle {
            sender: self.sender.clone(),
            subscription_sender: self.subscription_sender.clone(),
            subscription_receiver: self.subscription_sender.subscribe(),
        }
    }
}
