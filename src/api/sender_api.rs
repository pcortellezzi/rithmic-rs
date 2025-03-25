use bytes::Bytes;
use prost::Message;

use crate::{
    api::RithmicConnectionInfo,
    rti::{*,
        request_login::SysInfraType,
    },
};
use super::rithmic_command_types::RithmicBracketOrder;

pub const TRADE_ROUTE_LIVE: &str = "globex";
pub const TRADE_ROUTE_DEMO: &str = "simulator";
pub const USER_TYPE: i32 = 3;

#[derive(Debug, Clone)]
pub struct RithmicSenderApi {
    account_id: String,
    conn_info: RithmicConnectionInfo,
    fcm_id: String,
    ib_id: String,
    message_id_counter: u64,
}

impl RithmicSenderApi {
    pub fn new(conn_info: &RithmicConnectionInfo) -> Self {
        RithmicSenderApi {
            account_id: "".to_string(),
            conn_info: conn_info.clone(),
            fcm_id: "".to_string(),
            ib_id: "".to_string(),
            message_id_counter: 0,
        }
    }

    fn get_next_message_id(&mut self) -> String {
        self.message_id_counter += 1;
        self.message_id_counter.to_string()
    }

    fn request_to_buf(&self, req: impl Message, id: String) -> (Bytes, String) {
        let len = req.encoded_len() as u32;
        let header = len.to_be_bytes();

        let mut buf = Vec::with_capacity((len + 4) as usize);
        buf.extend_from_slice(&header); // Ajout du header
        req.encode(&mut buf).unwrap(); // Encodage du message

        (Bytes::from(buf), id)
    }

    pub fn request_get_instrument_by_underlying(&mut self) -> (Bytes, String) {
        let id = self.get_next_message_id();

        let req = RequestGetInstrumentByUnderlying {
            template_id: 103,
            ..RequestGetInstrumentByUnderlying::default()
        };

        self.request_to_buf(req, id)
    }

    pub fn request_heartbeat(&mut self) -> (Bytes, String) {
        let id = self.get_next_message_id();

        let req = RequestHeartbeat {
            template_id: 18,
            user_msg: vec![id.clone()],
            ..RequestHeartbeat::default()
        };

        self.request_to_buf(req, id)
    }

    pub fn request_login(
        &mut self,
        system_name: &str,
        infra_type: SysInfraType,
        user: &str,
        password: &str,
    ) -> (Bytes, String) {
        let id = self.get_next_message_id();

        let req = RequestLogin {
            template_id: 10,
            template_version: Some("5.27".into()),
            user: Some(user.to_string()),
            password: Some(password.to_string()),
            app_name: Some("pede:pts".to_string()),
            app_version: Some("1".into()),
            system_name: Some(system_name.to_string()),
            infra_type: Some(infra_type.into()),
            user_msg: vec![id.clone()],
            ..RequestLogin::default()
        };

        self.request_to_buf(req, id)
    }

    pub fn request_logout(&mut self) -> (Bytes, String) {
        let id = self.get_next_message_id();

        let req = RequestLogout {
            template_id: 12,
            user_msg: vec![id.clone()],
        };

        self.request_to_buf(req, id)
    }

    pub fn request_market_data_update(
        &mut self,
        symbol: &str,
        exchange: &str,
        fields: Vec<request_market_data_update::UpdateBits>,
        request_type: request_market_data_update::Request,
    ) -> (Bytes, String) {
        let id = self.get_next_message_id();

        let mut req = RequestMarketDataUpdate {
            template_id: 100,
            user_msg: vec![id.clone()],
            ..RequestMarketDataUpdate::default()
        };

        let mut bits = 0;

        for field in fields {
            bits |= field as u32;
        }

        req.symbol = Some(symbol.into());
        req.exchange = Some(exchange.into());
        req.request = Some(request_type.into());
        req.update_bits = Some(bits);

        self.request_to_buf(req, id)
    }

    pub fn request_product_codes(&mut self, exchange: Option<String>) -> (Bytes, String) {
        let id = self.get_next_message_id();

        let req = RequestProductCodes {
            template_id: 111,
            user_msg: vec![id.clone()],
            exchange,
            give_toi_products_only: Some(true),
        };

        self.request_to_buf(req, id)
    }
    pub fn request_reference_data(&mut self, symbol: Option<String>, exchange: Option<String>) -> (Bytes, String) {
        let id = self.get_next_message_id();

        let req = RequestReferenceData {
            template_id: 14,
            user_msg: vec![id.clone()],
            symbol,
            exchange,
        };

        self.request_to_buf(req, id)
    }

    pub fn request_rithmic_system_gateway_info(&mut self, system_name: String) -> (Bytes, String) {
        let id = self.get_next_message_id();

        let req = RequestRithmicSystemGatewayInfo {
            template_id: 20,
            user_msg: vec![id.clone()],
            system_name: Some(system_name),
        };

        self.request_to_buf(req, id)
    }

    pub fn request_rithmic_system_info(&mut self) -> (Bytes, String) {
        let id = self.get_next_message_id();

        let req = RequestRithmicSystemInfo {
            template_id: 16,
            user_msg: vec![id.clone()],
        };

        self.request_to_buf(req, id)
    }

    pub fn request_search_symbols(&mut self,
        search_text: Option<String>,
        instrument_type: Option<request_search_symbols::InstrumentType>,
        exact_search: Option<bool>
    ) -> (Bytes, String) {
        let id = self.get_next_message_id();

        let req = RequestSearchSymbols {
            template_id: 109,
            user_msg: vec![id.clone()],
            search_text,
            instrument_type: if instrument_type.is_some() {Some(instrument_type.unwrap() as i32)} else {None},
            pattern: Some(if exact_search.is_some_and(|b| { b }) {
                request_search_symbols::Pattern::Equals as i32
            } else { request_search_symbols::Pattern::Contains as i32 }),
            ..RequestSearchSymbols::default()
        };

        self.request_to_buf(req, id)
    }

    pub fn request_subscribe_for_order_updates(&mut self) -> (Bytes, String) {
        let id = self.get_next_message_id();

        let req = RequestSubscribeForOrderUpdates {
            template_id: 308,
            fcm_id: Some(self.fcm_id.clone()),
            ib_id: Some(self.ib_id.clone()),
            account_id: Some(self.account_id.clone()),
            user_msg: vec![id.clone()],
        };

        self.request_to_buf(req, id)
    }

    pub fn request_subscribe_to_bracket_updates(&mut self) -> (Bytes, String) {
        let id = self.get_next_message_id();

        let req = RequestSubscribeToBracketUpdates {
            template_id: 336,
            fcm_id: Some(self.fcm_id.clone()),
            ib_id: Some(self.ib_id.clone()),
            account_id: Some(self.account_id.clone()),
            user_msg: vec![id.clone()],
        };

        self.request_to_buf(req, id)
    }

    pub fn request_tick_bar_replay(
        &mut self,
        symbol: &str,
        exchange: &str,
        bar_type: request_tick_bar_replay::BarType,
        bar_sub_type: request_tick_bar_replay::BarSubType,
        bar_type_specifier: &str,
        start_index: i32,
        finish_index: i32,
        direction: request_tick_bar_replay::Direction,
        time_order: request_tick_bar_replay::TimeOrder,
    ) -> (Bytes, String) {
        let id = self.get_next_message_id();

        let req = RequestTickBarReplay {
            template_id: 206,
            user_msg: vec![id.clone()],
            symbol: Some(symbol.into()),
            exchange: Some(exchange.into()),
            bar_type: Some(bar_type.into()),
            bar_sub_type: Some(bar_sub_type.into()),
            bar_type_specifier: Some(bar_type_specifier.into()),
            start_index: Some(start_index),
            finish_index: Some(finish_index),
            direction: Some(direction.into()),
            time_order: Some(time_order.into()),
            ..RequestTickBarReplay::default()
        };

        self.request_to_buf(req, id)
    }

    pub fn request_tick_bar_update(
        &mut self,
        symbol: &str,
        exchange: &str,
        bar_type: request_tick_bar_update::BarType,
        bar_sub_type: request_tick_bar_update::BarSubType,
        bar_type_specifier: &str,
        request_type: request_tick_bar_update::Request,
    ) -> (Bytes, String) {
        let id = self.get_next_message_id();

        let req = RequestTickBarUpdate {
            template_id: 204,
            user_msg: vec![id.clone()],
            symbol: Some(symbol.into()),
            exchange: Some(exchange.into()),
            bar_type: Some(bar_type.into()),
            bar_sub_type: Some(bar_sub_type.into()),
            bar_type_specifier: Some(bar_type_specifier.into()),
            request: Some(request_type.into()),
            ..RequestTickBarUpdate::default()
        };

        self.request_to_buf(req, id)
    }

    pub fn request_time_bar_replay(
        &mut self,
        symbol: &str,
        exchange: &str,
        bar_type: request_time_bar_replay::BarType,
        bar_type_period: i32,
        start_index: i32,
        finish_index: i32,
        direction: request_time_bar_replay::Direction,
        time_order: request_time_bar_replay::TimeOrder,
    ) -> (Bytes, String) {
        let id = self.get_next_message_id();

        let req = RequestTimeBarReplay {
            template_id: 202,
            user_msg: vec![id.clone()],
            symbol: Some(symbol.into()),
            exchange: Some(exchange.into()),
            bar_type: Some(bar_type.into()),
            bar_type_period: Some(bar_type_period),
            start_index: Some(start_index),
            finish_index: Some(finish_index),
            direction: Some(direction.into()),
            time_order: Some(time_order.into()),
            ..RequestTimeBarReplay::default()
        };

        self.request_to_buf(req, id)
    }

    pub fn request_time_bar_update(
        &mut self,
        symbol: &str,
        exchange: &str,
        bar_type: request_time_bar_update::BarType,
        bar_type_period: i32,
        request_type: request_time_bar_update::Request,
    ) -> (Bytes, String) {
        let id = self.get_next_message_id();

        let req = RequestTimeBarUpdate {
            template_id: 200,
            user_msg: vec![id.clone()],
            symbol: Some(symbol.into()),
            exchange: Some(exchange.into()),
            bar_type: Some(bar_type.into()),
            bar_type_period: Some(bar_type_period),
            request: Some(request_type.into()),
            ..RequestTimeBarUpdate::default()
        };

        self.request_to_buf(req, id)
    }

    pub fn request_new_order(
        &mut self,
        exchange: &str,
        symbol: &str,
        qty: i32,
        price: f64,
        action: request_new_order::TransactionType,
        ordertype: request_new_order::PriceType,
        localid: &str,

        // optional args
        duration: Option<request_new_order::Duration>,
    ) -> (Bytes, String) {
        let id = self.get_next_message_id();

        // TODO
        let trade_route = "";

        let req = RequestNewOrder {
            template_id: 312,
            fcm_id: Some(self.fcm_id.clone()),
            ib_id: Some(self.ib_id.clone()),
            account_id: Some(self.account_id.clone()),
            trade_route: Some(trade_route.into()),
            exchange: Some(exchange.into()),
            symbol: Some(symbol.into()),
            quantity: Some(qty),
            price: Some(price),
            transaction_type: Some(action.into()),
            price_type: Some(ordertype.into()),
            manual_or_auto: Some(2),
            duration: if let Some(d) = duration {
                Some(d.into())
            } else {
                Some(1)
            },
            user_msg: vec![id.clone()],
            user_tag: Some(localid.into()),
            ..RequestNewOrder::default()
        };

        self.request_to_buf(req, id)
    }

    pub fn request_bracket_order(
        &mut self,
        bracket_order: RithmicBracketOrder,
    ) -> (Bytes, String) {
        let id = self.get_next_message_id();

        // TODO
        let trade_route = "";

        let req = RequestBracketOrder {
            template_id: 330,
            fcm_id: Some(self.fcm_id.clone()),
            ib_id: Some(self.ib_id.clone()),
            account_id: Some(self.account_id.clone()),
            trade_route: Some(trade_route.into()),
            exchange: Some(bracket_order.exchange),
            symbol: Some(bracket_order.symbol),
            user_type: Some(USER_TYPE),
            quantity: Some(bracket_order.qty),
            transaction_type: Some(bracket_order.action),
            price_type: Some(bracket_order.ordertype),
            manual_or_auto: Some(2),
            duration: Some(bracket_order.duration),
            bracket_type: Some(6),
            target_quantity: Some(bracket_order.qty),
            stop_quantity: Some(bracket_order.qty),
            target_ticks: Some(bracket_order.profit_ticks),
            stop_ticks: Some(bracket_order.stop_ticks),
            price: if bracket_order.ordertype != request_bracket_order::PriceType::Market.into() {
                bracket_order.price
            } else {
                None
            },
            user_msg: vec![id.clone()],
            user_tag: Some(bracket_order.localid),
            ..RequestBracketOrder::default()
        };

        self.request_to_buf(req, id)
    }

    pub fn request_modify_order(
        &mut self,
        basket_id: &str,
        exchange: &str,
        symbol: &str,
        qty: i32,
        price: f64,
        ordertype: i32,
    ) -> (Bytes, String) {
        let id = self.get_next_message_id();

        let req = RequestModifyOrder {
            template_id: 314,
            fcm_id: Some(self.fcm_id.clone()),
            ib_id: Some(self.ib_id.clone()),
            account_id: Some(self.account_id.clone()),
            basket_id: Some(basket_id.into()),
            manual_or_auto: Some(2),
            exchange: Some(exchange.into()),
            symbol: Some(symbol.into()),
            price_type: Some(ordertype),
            quantity: Some(qty),
            price: Some(price),
            user_msg: vec![id.clone()],
            trigger_price: match ordertype {
                3 | 4 => Some(price),
                _ => None,
            },
            ..RequestModifyOrder::default()
        };

        self.request_to_buf(req, id)
    }

    pub fn request_cancel_order(&mut self, basket_id: &str) -> (Bytes, String) {
        let id = self.get_next_message_id();

        let req = RequestCancelOrder {
            template_id: 316,
            fcm_id: Some(self.fcm_id.clone()),
            ib_id: Some(self.ib_id.clone()),
            account_id: Some(self.account_id.clone()),
            basket_id: Some(basket_id.into()),
            manual_or_auto: Some(2),
            user_msg: vec![id.clone()],
            ..RequestCancelOrder::default()
        };

        self.request_to_buf(req, id)
    }

    pub fn request_exit_position(&mut self, symbol: &str, exchange: &str) -> (Bytes, String) {
        let id = self.get_next_message_id();

        let req = RequestExitPosition {
            template_id: 3504,
            fcm_id: Some(self.fcm_id.clone()),
            ib_id: Some(self.ib_id.clone()),
            account_id: Some(self.account_id.clone()),
            symbol: Some(symbol.into()),
            exchange: Some(exchange.into()),
            manual_or_auto: Some(2),
            user_msg: vec![id.clone()],
            ..RequestExitPosition::default()
        };

        self.request_to_buf(req, id)
    }

    pub fn request_update_target_bracket_level(
        &mut self,
        basket_id: &str,
        profit_ticks: i32,
    ) -> (Bytes, String) {
        let id = self.get_next_message_id();

        let req = RequestUpdateTargetBracketLevel {
            template_id: 332,
            fcm_id: Some(self.fcm_id.clone()),
            ib_id: Some(self.ib_id.clone()),
            account_id: Some(self.account_id.clone()),
            basket_id: Some(basket_id.into()),
            target_ticks: Some(profit_ticks),
            user_msg: vec![id.clone()],
            ..RequestUpdateTargetBracketLevel::default()
        };

        self.request_to_buf(req, id)
    }

    pub fn request_update_stop_bracket_level(
        &mut self,
        basket_id: &str,
        stop_ticks: i32,
    ) -> (Bytes, String) {
        let id = self.get_next_message_id();

        let req = RequestUpdateStopBracketLevel {
            template_id: 334,
            fcm_id: Some(self.fcm_id.clone()),
            ib_id: Some(self.ib_id.clone()),
            account_id: Some(self.account_id.clone()),
            basket_id: Some(basket_id.into()),
            stop_ticks: Some(stop_ticks),
            user_msg: vec![id.clone()],
            ..RequestUpdateStopBracketLevel::default()
        };

        self.request_to_buf(req, id)
    }

    pub fn request_show_brackets(&mut self) -> (Bytes, String) {
        let id = self.get_next_message_id();

        let req = RequestShowBrackets {
            template_id: 338,
            fcm_id: Some(self.fcm_id.clone()),
            ib_id: Some(self.ib_id.clone()),
            account_id: Some(self.account_id.clone()),
            user_msg: vec![id.clone()],
        };

        self.request_to_buf(req, id)
    }

    pub fn request_show_bracket_stops(&mut self) -> (Bytes, String) {
        let id = self.get_next_message_id();

        let req = RequestShowBracketStops {
            template_id: 340,
            fcm_id: Some(self.fcm_id.clone()),
            ib_id: Some(self.ib_id.clone()),
            account_id: Some(self.account_id.clone()),
            user_msg: vec![id.clone()],
        };

        self.request_to_buf(req, id)
    }

    pub fn request_show_orders(&mut self) -> (Bytes, String) {
        let id = self.get_next_message_id();

        let req = RequestShowOrders {
            template_id: 320,
            fcm_id: Some(self.fcm_id.clone()),
            ib_id: Some(self.ib_id.clone()),
            account_id: Some(self.account_id.clone()),
            user_msg: vec![id.clone()],
        };

        self.request_to_buf(req, id)
    }

    pub fn request_pnl_position_updates(
        &mut self,
        action: request_pn_l_position_updates::Request,
    ) -> (Bytes, String) {
        let id = self.get_next_message_id();

        let req = RequestPnLPositionUpdates {
            template_id: 400,
            fcm_id: Some(self.fcm_id.clone()),
            ib_id: Some(self.ib_id.clone()),
            account_id: Some(self.account_id.clone()),
            request: Some(action.into()),
            user_msg: vec![id.clone()],
        };

        self.request_to_buf(req, id)
    }

    pub fn request_pnl_position_snapshot(&mut self) -> (Bytes, String) {
        let id = self.get_next_message_id();

        let req = RequestPnLPositionSnapshot {
            template_id: 402,
            fcm_id: Some(self.fcm_id.clone()),
            ib_id: Some(self.ib_id.clone()),
            account_id: Some(self.account_id.clone()),
            user_msg: vec![id.clone()],
        };

        self.request_to_buf(req, id)
    }
}
