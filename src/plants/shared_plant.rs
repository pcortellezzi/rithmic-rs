use std::env;
use anyhow::anyhow;
use async_trait::async_trait;
use base64::Engine;
use base64::prelude::BASE64_STANDARD;
use tracing::{event, Level};

use crate::{
    api::{
        RithmicConnectionInfo,
        receiver_api::{RithmicReceiverApi, RithmicResponse},
        sender_api::RithmicSenderApi,
        DEFAULT_RTI_WS_URL,
    },
    request_handler::{RithmicRequest, RithmicRequestHandler},
    ws::{get_heartbeat_interval, PlantActor, RithmicStream, connect},
};

use futures_util::{
    stream::{SplitSink, SplitStream},
    SinkExt, StreamExt,
};

use tokio_tungstenite::{
    client_async,
    connect_async,
    tungstenite::{Error, Message},
    Connector,
    WebSocketStream,
    MaybeTlsStream
};

use tokio::{
    net::TcpStream,
    sync::{broadcast::Sender, oneshot},
    time::Interval,
};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tungstenite::client::IntoClientRequest;
use tungstenite::handshake::client::generate_key;
use tungstenite::handshake::client::Request;
use tungstenite::http;
use tungstenite::http::header::{CONNECTION, UPGRADE};
use tungstenite::http::Uri;
use crate::rti::{
    ResponseRithmicSystemGatewayInfo, ResponseRithmicSystemInfo,
    messages::RithmicMessage
};

pub enum SharedPlantCommand {
    RithmicSystemInfo {},
    RithmicSystemGatewayInfo {
        system_name: String
    },
}

pub struct RithmicSharedPlant {
    rithmic_sender_api: RithmicSenderApi,
    rithmic_receiver_api: RithmicReceiverApi,
}

impl RithmicSharedPlant {
    pub fn new() -> RithmicSharedPlant {
        let config = RithmicConnectionInfo::default();
        let rithmic_sender_api = RithmicSenderApi::new(&config);
        let rithmic_receiver_api = RithmicReceiverApi {
            source: "shared_plant".to_string(),
        };

        RithmicSharedPlant {
            rithmic_sender_api,
            rithmic_receiver_api
        }
    }

    pub async fn rithmic_system_info(&mut self) -> Result<ResponseRithmicSystemInfo, anyhow::Error> {
        let ws_stream = connect(DEFAULT_RTI_WS_URL).await.unwrap();
        let (rithmic_sender, mut rithmic_reader) = ws_stream.split();

        let command = SharedPlantCommand::RithmicSystemInfo {};
        self.handle_command(rithmic_sender, command).await;
        if let Some(message) = rithmic_reader.next().await {
            if let Ok(Message::Binary(data)) = message {
                if let RithmicMessage::ResponseRithmicSystemInfo(msg) = self.rithmic_receiver_api.buf_to_message(data).unwrap().message {
                    Ok(msg)
                } else {
                    Err(anyhow!("message is not a rithmic system info"))
                }
            } else {
                Err(anyhow!("rithmic message error"))
            }
        } else {
            Err(anyhow!("rithmic message error"))
        }
    }

    pub async fn rithmic_system_gateway_info(&mut self, system_name: String
    ) -> Result<ResponseRithmicSystemGatewayInfo, anyhow::Error> {
        let ws_stream = connect(DEFAULT_RTI_WS_URL).await.unwrap();
        let (rithmic_sender, mut rithmic_reader) = ws_stream.split();

        let command = SharedPlantCommand::RithmicSystemGatewayInfo {
            system_name,
        };
        self.handle_command(rithmic_sender, command).await;
        if let Some(message) = rithmic_reader.next().await {
            if let Ok(Message::Binary(data)) = message {
                if let RithmicMessage::ResponseRithmicSystemGatewayInfo(msg) = self.rithmic_receiver_api.buf_to_message(data).unwrap().message {
                    Ok(msg)
                } else {
                    Err(anyhow!("message is not a rithmic system gateway info"))
                }
            } else {
                Err(anyhow!("rithmic message error"))
            }
        } else {
            Err(anyhow!("rithmic message error"))
        }
    }

    async fn handle_command(
        &mut self,
        mut rithmic_sender: SplitSink<WebSocketStream<MaybeTlsStream<TcpStream>>, Message,>,
        command: SharedPlantCommand) {
        match command {
            SharedPlantCommand::RithmicSystemInfo {} => {
                let (request_buf, id) = self.rithmic_sender_api.request_rithmic_system_info();

                event!(Level::INFO, "shared_plant: sending system info request {}", id);

                rithmic_sender
                    .send(Message::Binary(request_buf))
                    .await
                    .unwrap();
            }
            SharedPlantCommand::RithmicSystemGatewayInfo { system_name } => {
                let (request_buf, id) = self.rithmic_sender_api.request_rithmic_system_gateway_info(
                    system_name
                );

                event!(Level::INFO, "shared_plant: sending system gateway info request {}", id);

                rithmic_sender
                    .send(Message::Binary(request_buf))
                    .await
                    .unwrap();
            }
        }
    }
}