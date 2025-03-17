pub mod receiver_api;
pub mod rithmic_command_types;
pub mod sender_api;


pub static DEFAULT_RTI_WS_URL: &str = "wss://rprotocol-mobile.rithmic.com";


#[derive(Clone, Debug)]
pub struct RithmicConnectionInfo {
    pub url: String,
    pub user: String,
    pub password: String,
    pub system_name: String,
}

impl Default for RithmicConnectionInfo {
    fn default() -> RithmicConnectionInfo {
        RithmicConnectionInfo {
            url: DEFAULT_RTI_WS_URL.to_string(),
            user: "".to_string(),
            password: "".to_string(),
            system_name: "".to_string(),
        }

    }
}