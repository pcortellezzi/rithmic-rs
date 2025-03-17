use std::env;
use std::time::Duration;
use anyhow::anyhow;
use async_trait::async_trait;
use base64::Engine;
use base64::prelude::BASE64_STANDARD;
use bytes::Bytes;
use http::{Request, Uri};
use http::header::PROXY_AUTHORIZATION;
use tokio::net::TcpStream;
use tokio::time::{interval_at, Instant, Interval};
use tokio_tungstenite::{MaybeTlsStream, WebSocketStream};
use tokio_tungstenite::tungstenite::{Error, Message};
use tungstenite::client::IntoClientRequest;

pub trait RithmicStream {
    type Handle;

    fn get_handle(&self) -> Self::Handle;
}

#[async_trait]
pub trait PlantActor {
    type Command;

    async fn run(&mut self);
    async fn handle_command(&mut self, command: Self::Command);
    async fn handle_rithmic_message(&mut self, message: Result<Message, Error>) -> Result<bool, ()>;
}

pub fn get_heartbeat_interval() -> Interval {
    let heartbeat_interval = Duration::from_secs(60);
    let start_offset = Instant::now() + heartbeat_interval;

    interval_at(start_offset, heartbeat_interval)
}

pub async fn connect(url: &str) -> Result<WebSocketStream<MaybeTlsStream<TcpStream>>, anyhow::Error> {
    let ws_uri: Uri = url.parse()?;

    if let Ok(proxy_url_str) = env::var("HTTPS_PROXY") {
        let proxy_uri: hyper::Uri = proxy_url_str.parse()?;

        // Établir une connexion TCP avec le proxy
        let stream = hyper_util::rt::TokioIo::new(TcpStream::connect(format!("{}:{}", proxy_uri.host().unwrap_or_default(), proxy_uri.port_u16().unwrap_or(80))).await?);

        let (mut request_sender, conn) = hyper::client::conn::http1::handshake(stream).await?;
        let conn = tokio::spawn(conn.without_shutdown());

        let mut request_builder = Request::connect(format!("{}:{}", ws_uri.host().unwrap_or_default(), ws_uri.port_u16().unwrap_or(443)));
        // Ajoute l'authentification si présente dans l'URL du proxy
        if let Some(auth) = proxy_uri.authority() {
            if let Some((username, password)) = auth.as_str().split_once('@') {
                let credentials = format!("{}:{}", username, password.splitn(2, ':').next().unwrap_or(""));
                let auth = format!("Basic {}", BASE64_STANDARD.encode(credentials));
                request_builder = request_builder.header(PROXY_AUTHORIZATION, auth);
            }
        }
        let request = request_builder.body(http_body_util::Empty::<Bytes>::new())?;

        let res = request_sender.send_request(request).await?;

        if !res.status().is_success() {
            return Err(anyhow!(
                "The proxy server returned an error response: status code: {}, body: {:#?}",
                res.status(),
                res.body()
            ));
        }

        let tcp = conn.await
            .map_err(|e| anyhow!(e))??
            .io
            .into_inner();

        // CryptoProvider::install_default();
        let ws_stream = tokio_tungstenite::client_async_tls(ws_uri.into_client_request()?, tcp).await?.0;
        Ok(ws_stream)
    } else {
        let ws_stream = tokio_tungstenite::connect_async(ws_uri.into_client_request()?).await?.0;
        Ok(ws_stream)
    }
}