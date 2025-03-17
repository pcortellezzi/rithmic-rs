use std::string::ToString;
use std::thread::sleep;
use std::time::Duration;
use rithmic_client::api::RithmicConnectionInfo;
use rithmic_client::plants::ticker_plant::RithmicTickerPlant;
use rithmic_client::plants::shared_plant::RithmicSharedPlant;
use rithmic_client::rti;
use rithmic_client::ws::RithmicStream;
use tracing::{event, Level};
use tracing::instrument::WithSubscriber;
use rti::messages::RithmicMessage;

static SYSTEM_NAME: &str = "Rithmic Paper Trading";
static GATEWAY_NAME: &str = "Chicago Area";
static USERNAME: &str = "xxxxxxxx";
static PASSWORD: &str = "yyyyyyyy";
static TICKER: &str = "NQH5";
static EXCHANGE: &str = "CME";

#[tokio::main]
async fn main() {
    let mut shared_plant = RithmicSharedPlant::new();

    let rithmic_system_info = shared_plant.rithmic_system_info().await.unwrap();
    if rithmic_system_info.system_name.contains(&SYSTEM_NAME.to_string()) {
        let rithmic_system_gateway_info = shared_plant.rithmic_system_gateway_info(SYSTEM_NAME.to_string()).await.unwrap();
        if let Some(i) = rithmic_system_gateway_info.gateway_name.iter().position(|x| x == &GATEWAY_NAME.to_string()) {
            let rcinf = RithmicConnectionInfo {
                url: rithmic_system_gateway_info.gateway_uri[i].clone(),
                user: USERNAME.to_string(),
                password: PASSWORD.to_string(),
                system_name: SYSTEM_NAME.to_string(),
            };

            let ticker_plant = RithmicTickerPlant::new(&rcinf).await;
            let mut ticker_plant_handle = ticker_plant.get_handle();
            if let Ok (rti_response) = ticker_plant_handle.login().await {
                match rti_response.message {
                    RithmicMessage::ResponseLogin(login) => {
                        println!("{:?}", login);
                        event!(Level::INFO, "login successful");

                        if let Ok(rti_response) = ticker_plant_handle.subscribe(TICKER, EXCHANGE).await {
                            match rti_response.message {
                                RithmicMessage::ResponseMarketDataUpdate(market_data) => {
                                    println!("{:?}", market_data);
                                    event!(Level::INFO, "market data update successful");
                                    loop {
                                        if let Ok(rti_response) = ticker_plant_handle.subscription_receiver.recv().await {
                                            match rti_response.message {
                                                RithmicMessage::LastTrade(last_trade) => {
                                                    println!("{:?}", last_trade);
                                                    event!(Level::INFO, "last trade received");
                                                }
                                                RithmicMessage::DepthByOrder(depth_by_order) => {
                                                    println!("{:?}", depth_by_order);
                                                    event!(Level::INFO, "depth by order received");
                                                }
                                                _ => {
                                                    event!(Level::INFO, "message not handled");
                                                }
                                            }
                                        }
                                    }
                                }
                                _ => {
                                    event!(Level::ERROR, "market data update failed");
                                }
                            }
                        }
                    }
                    _ => {
                        event!(Level::ERROR, "login failed");
                    }
                }
                let _ = ticker_plant_handle.disconnect().await;
            }
        }
    }
}
