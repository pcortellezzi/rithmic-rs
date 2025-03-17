use async_trait::async_trait;
use tracing::{event, Level};

use crate::{
    api::{
        RithmicConnectionInfo,
        receiver_api::{RithmicReceiverApi, RithmicResponse},
        rithmic_command_types::{RithmicBracketOrder, RithmicCancelOrder, RithmicModifyOrder},
        sender_api::RithmicSenderApi,
    },
    request_handler::{RithmicRequest, RithmicRequestHandler},
    rti::request_login::SysInfraType,
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

pub enum OrderPlantCommand {
    Close,
    Login {
        response_sender: oneshot::Sender<Result<Vec<RithmicResponse>, String>>,
    },
    SetLogin,
    Logout {
        response_sender: oneshot::Sender<Result<Vec<RithmicResponse>, String>>,
    },
    SendHeartbeat {},
    SubscribeOrderUpdates {
        response_sender: oneshot::Sender<Result<Vec<RithmicResponse>, String>>,
    },
    SubscribeBracketUpdates {
        response_sender: oneshot::Sender<Result<Vec<RithmicResponse>, String>>,
    },
    SubscribePnlUpdates {
        response_sender: oneshot::Sender<Result<Vec<RithmicResponse>, String>>,
    },
    PlaceBracketOrder {
        bracket_order: RithmicBracketOrder,
        response_sender: oneshot::Sender<Result<Vec<RithmicResponse>, String>>,
    },
    ModifyOrder {
        order: RithmicModifyOrder,
        response_sender: oneshot::Sender<Result<Vec<RithmicResponse>, String>>,
    },
    ModifyStop {
        order_id: String,
        ticks: i32,
        response_sender: oneshot::Sender<Result<Vec<RithmicResponse>, String>>,
    },
    ModifyProfit {
        order_id: String,
        ticks: i32,
        response_sender: oneshot::Sender<Result<Vec<RithmicResponse>, String>>,
    },
    CancelOrder {
        order_id: String,
        response_sender: oneshot::Sender<Result<Vec<RithmicResponse>, String>>,
    },
    ShowOrders {
        response_sender: oneshot::Sender<Result<Vec<RithmicResponse>, String>>,
    },
}

pub struct RithmicOrderPlant {
    pub connection_handle: tokio::task::JoinHandle<()>,
    sender: tokio::sync::mpsc::Sender<OrderPlantCommand>,
    subscription_sender: Sender<RithmicResponse>,
}

impl RithmicOrderPlant {
    pub async fn new(conn_info: &RithmicConnectionInfo) -> RithmicOrderPlant {
        let (req_tx, req_rx) = tokio::sync::mpsc::channel::<OrderPlantCommand>(32);
        let (sub_tx, _sub_rx) = tokio::sync::broadcast::channel(1024);

        let mut order_plant = OrderPlant::new(req_rx, sub_tx.clone(), conn_info)
            .await
            .unwrap();

        let connection_handle = tokio::spawn(async move {
            order_plant.run().await;
        });

        RithmicOrderPlant {
            connection_handle,
            sender: req_tx,
            subscription_sender: sub_tx,
        }
    }
}

impl RithmicStream for RithmicOrderPlant {
    type Handle = RithmicOrderPlantHandle;

    fn get_handle(&self) -> RithmicOrderPlantHandle {
        RithmicOrderPlantHandle {
            sender: self.sender.clone(),
            subscription_receiver: self.subscription_sender.subscribe(),
        }
    }
}

pub struct OrderPlant {
    config: RithmicConnectionInfo,
    interval: Interval,
    logged_in: bool,
    request_handler: RithmicRequestHandler,
    request_receiver: tokio::sync::mpsc::Receiver<OrderPlantCommand>,
    rithmic_reader: SplitStream<WebSocketStream<MaybeTlsStream<TcpStream>>>,
    rithmic_receiver_api: RithmicReceiverApi,
    rithmic_sender: SplitSink<
        WebSocketStream<MaybeTlsStream<TcpStream>>,
        Message,
    >,
    rithmic_sender_api: RithmicSenderApi,
    subscription_sender: Sender<RithmicResponse>,
}

impl OrderPlant {
    pub async fn new(
        request_receiver: tokio::sync::mpsc::Receiver<OrderPlantCommand>,
        subscription_sender: Sender<RithmicResponse>,
        conn_info: &RithmicConnectionInfo,
    ) -> Result<OrderPlant, String> {
        let config = conn_info.clone();

        let ws_stream = connect(&config.url).await.unwrap();
        let (rithmic_sender, rithmic_reader) = ws_stream.split();
        let rithmic_sender_api = RithmicSenderApi::new(&config);
        let rithmic_receiver_api = RithmicReceiverApi {
            source: "order_plant".to_string(),
        };

        let interval = get_heartbeat_interval();

        Ok(OrderPlant {
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
impl PlantActor for OrderPlant {
    type Command = OrderPlantCommand;

    async fn run(&mut self) {
        loop {
            tokio::select! {
                _ = self.interval.tick() => {
                    if self.logged_in {
                        self.handle_command(OrderPlantCommand::SendHeartbeat {}).await;
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
                else => { break; }
            }
        }
    }

    async fn handle_rithmic_message(
        &mut self,
        message: Result<Message, Error>,
    ) -> Result<bool, ()> {
        let mut stop: bool = false;

        match message {
            Ok(Message::Close(frame)) => {
                event!(
                    Level::INFO,
                    "order_plant: Received close frame: {:?}",
                    frame
                );

                stop = true;
            }
            Ok(Message::Binary(data)) => match self.rithmic_receiver_api.buf_to_message(data) {
                Ok(response) => {
                    if response.is_update {
                        self.subscription_sender.send(response).unwrap();
                    } else {
                        self.request_handler.handle_response(response);
                    }
                }
                Err(e) => {
                    event!(Level::ERROR, "order_plant: response from server: {:?}", e);
                }
            },
            Err(Error::ConnectionClosed) => {
                event!(Level::INFO, "order_plant: Connection closed");

                stop = true;
            }
            _ => {
                event!(Level::WARN, "order_plant: Unhandled message: {:?}", message);
            }
        }

        Ok(stop)
    }

    async fn handle_command(&mut self, command: OrderPlantCommand) {
        match command {
            OrderPlantCommand::Close => {
                self.rithmic_sender
                    .send(Message::Close(None))
                    .await
                    .unwrap();
            }
            OrderPlantCommand::Login { response_sender } => {
                let (login_buf, id) = self.rithmic_sender_api.request_login(
                    &self.config.system_name,
                    SysInfraType::OrderPlant,
                    &self.config.user,
                    &self.config.password,
                );

                event!(Level::INFO, "order_plant: sending login request {}", id);

                self.request_handler.register_request(RithmicRequest {
                    request_id: id,
                    responder: response_sender,
                });

                self.rithmic_sender
                    .send(Message::Binary(login_buf))
                    .await
                    .unwrap();
            }
            OrderPlantCommand::SetLogin => {
                self.logged_in = true;
            }
            OrderPlantCommand::Logout { response_sender } => {
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
            OrderPlantCommand::SendHeartbeat {} => {
                let (heartbeat_buf, _id) = self.rithmic_sender_api.request_heartbeat();

                let _ = self
                    .rithmic_sender
                    .send(Message::Binary(heartbeat_buf))
                    .await;
            }
            OrderPlantCommand::SubscribeOrderUpdates { response_sender } => {
                let (req_buf, id) = self
                    .rithmic_sender_api
                    .request_subscribe_for_order_updates();

                self.request_handler.register_request(RithmicRequest {
                    request_id: id,
                    responder: response_sender,
                });

                self.rithmic_sender
                    .send(Message::Binary(req_buf))
                    .await
                    .unwrap();
            }
            OrderPlantCommand::SubscribeBracketUpdates { response_sender } => {
                let (req_buf, id) = self
                    .rithmic_sender_api
                    .request_subscribe_to_bracket_updates();

                self.request_handler.register_request(RithmicRequest {
                    request_id: id,
                    responder: response_sender,
                });

                self.rithmic_sender
                    .send(Message::Binary(req_buf))
                    .await
                    .unwrap();
            }
            OrderPlantCommand::PlaceBracketOrder {
                bracket_order,
                response_sender,
            } => {
                let (req_buf, id) = self.rithmic_sender_api.request_bracket_order(bracket_order);

                self.request_handler.register_request(RithmicRequest {
                    request_id: id,
                    responder: response_sender,
                });

                self.rithmic_sender
                    .send(Message::Binary(req_buf))
                    .await
                    .unwrap();
            }
            OrderPlantCommand::ModifyOrder {
                order,
                response_sender,
            } => {
                let (req_buf, id) = self.rithmic_sender_api.request_modify_order(
                    &order.id,
                    &order.exchange,
                    &order.symbol,
                    order.qty,
                    order.price,
                    order.ordertype,
                );

                self.request_handler.register_request(RithmicRequest {
                    request_id: id,
                    responder: response_sender,
                });

                self.rithmic_sender
                    .send(Message::Binary(req_buf))
                    .await
                    .unwrap();
            }
            OrderPlantCommand::CancelOrder {
                order_id,
                response_sender,
            } => {
                let (req_buf, id) = self.rithmic_sender_api.request_cancel_order(&order_id);

                self.request_handler.register_request(RithmicRequest {
                    request_id: id,
                    responder: response_sender,
                });

                self.rithmic_sender
                    .send(Message::Binary(req_buf))
                    .await
                    .unwrap();
            }
            OrderPlantCommand::ModifyStop {
                order_id,
                ticks,
                response_sender,
            } => {
                let (req_buf, id) = self
                    .rithmic_sender_api
                    .request_update_stop_bracket_level(&order_id, ticks);

                self.request_handler.register_request(RithmicRequest {
                    request_id: id,
                    responder: response_sender,
                });

                self.rithmic_sender
                    .send(Message::Binary(req_buf))
                    .await
                    .unwrap();
            }
            OrderPlantCommand::ModifyProfit {
                order_id,
                ticks,
                response_sender,
            } => {
                let (req_buf, id) = self
                    .rithmic_sender_api
                    .request_update_target_bracket_level(&order_id, ticks);

                self.request_handler.register_request(RithmicRequest {
                    request_id: id,
                    responder: response_sender,
                });

                self.rithmic_sender
                    .send(Message::Binary(req_buf))
                    .await
                    .unwrap();
            }
            OrderPlantCommand::ShowOrders { response_sender } => {
                let (req_buf, id) = self.rithmic_sender_api.request_show_orders();

                self.request_handler.register_request(RithmicRequest {
                    request_id: id,
                    responder: response_sender,
                });

                self.rithmic_sender
                    .send(Message::Binary(req_buf))
                    .await
                    .unwrap();
            }
            _ => {}
        };
    }
}

pub struct RithmicOrderPlantHandle {
    sender: tokio::sync::mpsc::Sender<OrderPlantCommand>,
    pub subscription_receiver: tokio::sync::broadcast::Receiver<RithmicResponse>,
}

impl RithmicOrderPlantHandle {
    pub async fn login(&self) -> Result<RithmicResponse, String> {
        event!(Level::INFO, "order_plant: logging in");

        let (tx, rx) = oneshot::channel::<Result<Vec<RithmicResponse>, String>>();

        let command = OrderPlantCommand::Login {
            response_sender: tx,
        };

        let _ = self.sender.send(command).await;
        let response = rx.await.unwrap().unwrap().remove(0);

        if response.error.is_none() {
            let _ = self.sender.send(OrderPlantCommand::SetLogin).await;

            event!(Level::INFO, "order_plant: logged in");

            Ok(response)
        } else {
            event!(
                Level::ERROR,
                "order_plant: login failed {:?}",
                response.error
            );

            Err(response.error.unwrap())
        }
    }

    pub async fn disconnect(&self) -> Result<RithmicResponse, String> {
        let (tx, rx) = oneshot::channel::<Result<Vec<RithmicResponse>, String>>();

        let command = OrderPlantCommand::Logout {
            response_sender: tx,
        };

        let _ = self.sender.send(command).await;
        let mut r = rx.await.unwrap().unwrap();
        let _ = self.sender.send(OrderPlantCommand::Close).await;

        Ok(r.remove(0))
    }

    pub async fn subscribe_order_updates(&self) -> Result<RithmicResponse, String> {
        let (tx, rx) = oneshot::channel::<Result<Vec<RithmicResponse>, String>>();

        let command = OrderPlantCommand::SubscribeOrderUpdates {
            response_sender: tx,
        };

        let _ = self.sender.send(command).await;

        Ok(rx.await.unwrap().unwrap().remove(0))
    }

    pub async fn subscribe_bracket_updates(&self) -> Result<RithmicResponse, String> {
        let (tx, rx) = oneshot::channel::<Result<Vec<RithmicResponse>, String>>();

        let command = OrderPlantCommand::SubscribeBracketUpdates {
            response_sender: tx,
        };

        let _ = self.sender.send(command).await;

        Ok(rx.await.unwrap().unwrap().remove(0))
    }

    pub async fn place_bracket_order(
        &self,
        bracket_order: RithmicBracketOrder,
    ) -> Result<Vec<RithmicResponse>, String> {
        let (tx, rx) = oneshot::channel::<Result<Vec<RithmicResponse>, String>>();

        let command = OrderPlantCommand::PlaceBracketOrder {
            bracket_order,
            response_sender: tx,
        };

        let _ = self.sender.send(command).await;

        rx.await.unwrap()
    }

    pub async fn modify_order(&self, order: RithmicModifyOrder) -> Result<RithmicResponse, String> {
        let (tx, rx) = oneshot::channel::<Result<Vec<RithmicResponse>, String>>();

        let command = OrderPlantCommand::ModifyOrder {
            order,
            response_sender: tx,
        };

        let _ = self.sender.send(command).await;

        Ok(rx.await.unwrap().unwrap().remove(0))
    }

    pub async fn cancel_order(&self, order: RithmicCancelOrder) -> Result<RithmicResponse, String> {
        let (tx, rx) = oneshot::channel::<Result<Vec<RithmicResponse>, String>>();

        let command = OrderPlantCommand::CancelOrder {
            order_id: order.id,
            response_sender: tx,
        };

        let _ = self.sender.send(command).await;

        Ok(rx.await.unwrap().unwrap().remove(0))
    }

    pub async fn adjust_profit(&self, id: &str, ticks: i32) -> Result<RithmicResponse, String> {
        let (tx, rx) = oneshot::channel::<Result<Vec<RithmicResponse>, String>>();

        let command = OrderPlantCommand::ModifyProfit {
            order_id: id.to_string(),
            ticks,
            response_sender: tx,
        };

        let _ = self.sender.send(command).await;

        Ok(rx.await.unwrap().unwrap().remove(0))
    }

    pub async fn adjust_stop(&self, id: &str, ticks: i32) -> Result<RithmicResponse, String> {
        let (tx, rx) = oneshot::channel::<Result<Vec<RithmicResponse>, String>>();

        let command = OrderPlantCommand::ModifyStop {
            order_id: id.to_string(),
            ticks,
            response_sender: tx,
        };

        let _ = self.sender.send(command).await;

        Ok(rx.await.unwrap().unwrap().remove(0))
    }

    pub async fn show_orders(&self) -> Result<RithmicResponse, String> {
        let (tx, rx) = oneshot::channel::<Result<Vec<RithmicResponse>, String>>();

        let command = OrderPlantCommand::ShowOrders {
            response_sender: tx,
        };

        let _ = self.sender.send(command).await;

        Ok(rx.await.unwrap().unwrap().remove(0))
    }
}
