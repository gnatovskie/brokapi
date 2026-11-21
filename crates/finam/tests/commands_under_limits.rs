//! What a bot sees when the broker refuses a command for a rate limit or fails it half way, and when a stream
//! breaks while an order fills. Against the gRPC mock, no network, no money.

mod mock_server;

use std::time::Duration;

use brokapi_core::{
    ClientOrderId, Decimal, Error, Event, ExecutionSource, OrderExecutor, OrderState, OrderType,
    PlaceOrderRequest, PlaceStopOrderRequest, Price, Qty, Side, StopExpiry, StopOrderExecutor,
    StopOrderKind, TimeRange, UnixNanos, dec,
};
use mock_server::{MockServer, sber, ud};
use tonic::Code;

fn limit_buy() -> PlaceOrderRequest {
    PlaceOrderRequest {
        cid: ClientOrderId::new_v4(),
        instrument: sber(),
        side: Side::Buy,
        order_type: OrderType::Limit,
        qty: Qty::from(1),
        price: Some(Price::new(dec!(1.0))),
    }
}

fn stop(kind: StopOrderKind, side: Side) -> PlaceStopOrderRequest {
    PlaceStopOrderRequest {
        cid: ClientOrderId::new_v4(),
        instrument: sber(),
        side,
        kind,
        qty: Qty::from(5),
        stop_price: Price::new(dec!(100.0)),
        limit_price: None,
        expiry: StopExpiry::GoodTillCancel,
    }
}

fn fail_next_cancel(server: &MockServer, code: Code) {
    *server.state.options.cancel_error.lock().unwrap() = Some(code);
}

fn fail_next_place(server: &MockServer, code: Code) {
    *server.state.options.place_error.lock().unwrap() = Some(code);
}

/// A cancel that the broker refuses for a rate limit leaves the order alive and says `RateLimited`. One that fails on the
/// way is `Unknown`. Neither is repeated by the library: every call of the bot is one request, and the state of the order
/// is read, not guessed.
#[tokio::test]
async fn a_cancel_that_hits_a_limit_or_fails_is_not_repeated_and_the_order_is_still_there() {
    let server = MockServer::start().await;
    let (ud, _rx) = ud(&server, "ACC1").await;
    ud.reconcile(&[]).await.unwrap();
    let req = limit_buy();
    let cid = req.cid;
    ud.place(req).await.unwrap();

    fail_next_cancel(&server, Code::ResourceExhausted);
    assert!(matches!(
        ud.cancel(cid).await,
        Err(Error::RateLimited { .. })
    ));
    assert_eq!(server.state.count_calls("CancelOrder"), 1);
    assert!(matches!(
        ud.order_state(cid).await.unwrap().state,
        OrderState::Acked { .. }
    ));

    fail_next_cancel(&server, Code::Unavailable);
    assert!(matches!(ud.cancel(cid).await, Err(Error::Unknown)));
    assert_eq!(
        server.state.count_calls("CancelOrder"),
        2,
        "one request per call"
    );

    ud.cancel(cid).await.expect("the cancel after the failures");
    assert!(matches!(
        ud.order_state(cid).await.unwrap().state,
        OrderState::Cancelled { .. }
    ));
}

/// The same for the two ways Finam takes a stop order: `PlaceOrder` (stop-loss, stop-limit) and `PlaceSLTPOrder`
/// (take-profit). A limit refusal is `RateLimited` and the library sends nothing again by itself. An answer lost on the
/// way is `Unknown`, and then the same cid is refused (`duplicate_place`): the stop may exist, a second one must not.
#[tokio::test]
async fn a_stop_order_that_hits_a_limit_or_fails_is_not_repeated() {
    let server = MockServer::start().await;
    let (ud, _rx) = ud(&server, "ACC1").await;
    ud.reconcile(&[]).await.unwrap();

    fail_next_place(&server, Code::ResourceExhausted);
    assert!(matches!(
        ud.place_stop(stop(StopOrderKind::StopLoss, Side::Sell))
            .await,
        Err(Error::RateLimited { .. })
    ));
    assert_eq!(server.state.count_calls("PlaceOrder"), 1);
    assert!(ud.active_stop_orders().await.unwrap().is_empty());
    // The broker said no for a limit and took nothing: a new try with a new cid is a new request.
    ud.place_stop(stop(StopOrderKind::StopLoss, Side::Sell))
        .await
        .unwrap();
    assert_eq!(server.state.count_calls("PlaceOrder"), 2);

    let tp = stop(StopOrderKind::TakeProfit, Side::Sell);
    fail_next_place(&server, Code::Unavailable);
    assert!(matches!(
        ud.place_stop(tp.clone()).await,
        Err(Error::Unknown)
    ));
    assert_eq!(server.state.count_calls("PlaceSLTPOrder"), 1);
    assert!(matches!(
        ud.place_stop(tp).await,
        Err(Error::Rejected { ref code, .. }) if code == "duplicate_place"
    ));
    assert_eq!(
        server.state.count_calls("PlaceSLTPOrder"),
        1,
        "the repeat never went out"
    );
    // A new cid goes through on this road too.
    ud.place_stop(stop(StopOrderKind::TakeProfit, Side::Sell))
        .await
        .unwrap();
    assert_eq!(ud.active_stop_orders().await.unwrap().len(), 2);
}

/// A fill that happens while the streams of the account are down never arrives as an event. After `Reconnected` the bot
/// finds it with `reconcile`, `order_state` and `executions`, and the reopened streams carry what comes next.
#[tokio::test]
async fn a_fill_lost_in_the_gap_of_the_streams_is_found_after_the_reconnect() {
    let server = MockServer::start().await;
    let (ud, mut rx) = ud(&server, "ACC1").await;
    ud.reconcile(&[]).await.unwrap();
    ud.subscribe_order_state().await.unwrap();
    ud.subscribe_executions().await.unwrap();
    let req = limit_buy();
    let cid = req.cid;
    ud.place(req).await.unwrap();
    let order = server.state.orders_of("ACC1").remove(0);
    brokapi_mocks::time::wait_until("both streams are open", || {
        server.state.count_calls("SubscribeTrades") >= 1
            && server
                .state
                .order_stream_opens
                .load(std::sync::atomic::Ordering::SeqCst)
                >= 1
    })
    .await;
    let comment = order.order.clone().unwrap().comment;

    // The streams end, and when they are gone the broker fills the order.
    let opens = server
        .state
        .order_stream_opens
        .load(std::sync::atomic::Ordering::SeqCst);
    server.state.cut_user_streams();
    brokapi_mocks::time::wait_until("the streams are gone", || {
        server.state.streams_closed("ACC1")
    })
    .await;
    server
        .state
        .fill("ACC1", &comment, dec!(1), dec!(1))
        .unwrap();
    brokapi_mocks::time::jump_until("the streams are back", Duration::from_secs(1), || {
        server
            .state
            .order_stream_opens
            .load(std::sync::atomic::Ordering::SeqCst)
            > opens
            && server.state.count_calls("SubscribeTrades") >= 2
    })
    .await;
    brokapi_conformance::next_matching("a_fill_lost_in_the_gap", &mut rx, |e| {
        matches!(e.payload, Event::Reconnected { .. })
    })
    .await;

    let report = ud.reconcile(&[cid]).await.unwrap();
    assert_eq!(report.closed_offline, vec![cid], "{report:?}");
    assert!(matches!(
        ud.order_state(cid).await.unwrap().state,
        OrderState::Filled { .. }
    ));
    let fills = ud
        .executions(TimeRange::new(
            UnixNanos::ZERO,
            UnixNanos::from_nanos(i64::MAX / 2),
        ))
        .await
        .unwrap();
    assert_eq!(fills.len(), 1, "{fills:?}");
    assert_eq!(fills[0].cid, Some(cid));
    assert_eq!(fills[0].qty.decimal(), Decimal::ONE);

    // The reopened streams carry the next order.
    let next = limit_buy();
    let next_cid = next.cid;
    ud.place(next).await.unwrap();
    brokapi_conformance::next_matching(
        "a_fill_lost_in_the_gap",
        &mut rx,
        |e| matches!(&e.payload, Event::Order(o) if o.cid == next_cid),
    )
    .await;
}
