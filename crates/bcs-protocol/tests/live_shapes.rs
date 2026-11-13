//! Shapes seen on the live BCS API (05.11.2026), where the broker differs from its own docs.

use brokapi_bcs_protocol::http::models::{CreateOrderRequest, OrderStateListResponse};
use brokapi_bcs_protocol::models::enums::OrderSide;
use rust_decimal::Decimal;
use std::str::FromStr;
use uuid::Uuid;

/// `GET /orders?orderIdType=1&orderId=...` answered with `data` as one object, not the list of the docs.
/// Ids are replaced, the rest is as the broker sent it.
const LIVE_GET_ORDER: &str = r#"{"clientOrderId":"68d4690a-2896-4c37-9675-614537e664cc","data":{"accruedCoupon":0,"averagePrice":0,"classCode":"TQBR","clientCode":"<redacted>","commission":0,"currency":"RUB","executedQuantity":0,"executionId":"<redacted>","executionType":"8","executionValue":0,"lastQuantity":0,"messageType":"8","orderId":"<redacted>","orderNumber":"","orderQuantity":1,"orderStatus":"8","orderType":"2","price":"265.08","rejectReason":"Биржа не позволяет установить цену заявки меньше 267.25.","remainedQuantity":0,"securityExchange":"TQBR","side":"1","ticker":"SBER","timeInForce":"1","transactionTime":"2026-11-05T14:41:37.914Z"},"originalClientOrderId":""}"#;

#[test]
fn an_order_state_with_one_report_object_is_read() {
    let state: OrderStateListResponse = serde_json::from_str(LIVE_GET_ORDER).unwrap();
    assert_eq!(state.data.len(), 1);
    assert_eq!(state.data[0].order_status.as_deref(), Some("8"));
    assert_eq!(state.data[0].ticker.as_deref(), Some("SBER"));
}

#[test]
fn an_order_state_with_a_list_of_reports_is_still_read() {
    let state: OrderStateListResponse = serde_json::from_str(
        r#"{"clientOrderId":"r","data":[{"orderStatus":"0"},{"orderStatus":"1"}]}"#,
    )
    .unwrap();
    assert_eq!(state.data.len(), 2);
    assert_eq!(state.data[1].order_status.as_deref(), Some("1"));
}

/// The live API refused `"price":279.7600000000` with `INVALID_FORMAT` and took `279.71`.
/// A price past the 8 decimals of the docs loses its trailing zeros, a short one stays as given.
#[test]
fn a_price_with_trailing_zeros_goes_out_in_its_shortest_form() {
    let price = Decimal::from_str("279.7600000000").unwrap();
    let req = CreateOrderRequest::limit(Uuid::nil(), "SBER", "TQBR", OrderSide::Buy, 1, price);
    let body = serde_json::to_string(&req).unwrap();
    assert!(
        body.contains(r#""price":279.76,"#) || body.ends_with(r#""price":279.76}"#),
        "{body}"
    );

    let req = CreateOrderRequest::limit(
        Uuid::nil(),
        "SBER",
        "TQBR",
        OrderSide::Buy,
        1,
        Decimal::from_str("244.30").unwrap(),
    );
    assert!(
        serde_json::to_string(&req)
            .unwrap()
            .contains(r#""price":244.30"#)
    );
}

#[test]
fn a_time_in_force_sent_as_text_is_read() {
    let state: OrderStateListResponse = serde_json::from_str(LIVE_GET_ORDER).unwrap();
    assert_eq!(state.data[0].time_in_force, Some(1));
    let state: OrderStateListResponse =
        serde_json::from_str(r#"{"clientOrderId":"r","data":[{"timeInForce":2}]}"#).unwrap();
    assert_eq!(state.data[0].time_in_force, Some(2));
}
