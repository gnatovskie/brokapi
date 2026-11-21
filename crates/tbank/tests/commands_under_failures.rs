//! What a bot sees when T-Bank refuses a command (a limit, a refusal) or fails it on the way, and when the order streams
//! break while an order fills. Against the gRPC mock, no network, no money.

mod mock_server;

use std::sync::Arc;
use std::sync::atomic::Ordering;
use std::time::Duration;

use brokapi_core::{
    ClientOrderId, Decimal, Envelope, Error, Event, InstrumentId, InstrumentKey, NativeId,
    OrderExecutor, OrderState, OrderType, PlaceOrderRequest, PlaceStopOrderRequest, Price, Qty,
    Side, StopExpiry, StopOrderExecutor, StopOrderKind, TimeRange, UnixNanos, dec,
};
use brokapi_core::{ExecutionSource, StopOrderRef};
use brokapi_runtime::BrokerLimiter;
use brokapi_tbank::limits::default_limits;
use brokapi_tbank::{UserDataConfig, UserDataConnector};
use brokapi_tbank_protocol::TbankConfig;
use mock_server::{ACCOUNT_A, MockOptions, MockServer, TOKEN};
use tokio::sync::mpsc;

fn sber() -> InstrumentId {
    InstrumentId::new(
        InstrumentKey::new("SBER", "TQBR"),
        NativeId::Uid("uid-sber".into()),
    )
}

async fn start() -> (
    Arc<MockOptions>,
    MockServer,
    UserDataConnector,
    mpsc::Receiver<Envelope>,
) {
    let opts = Arc::new(MockOptions::default());
    let server = MockServer::start(Arc::clone(&opts)).await;
    let (tx, rx) = mpsc::channel(256);
    let cfg = TbankConfig::for_mock(TOKEN, &server.endpoint).with_account(ACCOUNT_A);
    let ud = UserDataConnector::start(
        UserDataConfig::new(cfg, "tbank-ud"),
        BrokerLimiter::new(&default_limits()),
        tx,
    )
    .await
    .expect("ud start");
    ud.reconcile(&[]).await.expect("reconcile");
    (opts, server, ud, rx)
}

fn limit_buy() -> PlaceOrderRequest {
    PlaceOrderRequest {
        cid: ClientOrderId::new_v4(),
        instrument: sber(),
        side: Side::Buy,
        order_type: OrderType::Limit,
        qty: Qty::from(20),
        price: Some(Price::from(250)),
    }
}

fn stop(kind: StopOrderKind) -> PlaceStopOrderRequest {
    PlaceStopOrderRequest {
        cid: ClientOrderId::new_v4(),
        instrument: sber(),
        side: Side::Sell,
        kind,
        qty: Qty::from(20),
        stop_price: Price::new(dec!(200)),
        limit_price: (kind == StopOrderKind::StopLimit).then(|| Price::new(dec!(199))),
        expiry: StopExpiry::GoodTillCancel,
    }
}

fn fail_next_cancel(opts: &MockOptions, code: tonic::Code, text: &str) {
    *opts.cancel_fail.lock().unwrap() = Some((code, text.to_string()));
}

/// A cancel that hits the limit of the broker is `RateLimited`, one that fails on the way is `Unknown`: either way the order
/// is still alive at the broker, and the next cancel goes through.
#[tokio::test]
async fn a_cancel_that_hits_a_limit_or_fails_leaves_the_order_alive() {
    let (opts, _server, ud, _rx) = start().await;
    let req = limit_buy();
    let cid = req.cid;
    ud.place(req).await.unwrap();

    fail_next_cancel(&opts, tonic::Code::ResourceExhausted, "80002");
    assert!(matches!(
        ud.cancel(cid).await,
        Err(Error::RateLimited { .. })
    ));
    fail_next_cancel(&opts, tonic::Code::Unavailable, "broker down");
    assert!(matches!(ud.cancel(cid).await, Err(Error::Unknown)));
    assert_eq!(ud.active_orders().await.unwrap().len(), 1);

    ud.cancel(cid).await.expect("the cancel after the failures");
    assert!(ud.active_orders().await.unwrap().is_empty());
}

/// A stop order that hits a limit is `RateLimited` and the cid stays usable. The lost-stop-orders limit of an instrument
/// (80007) is a refusal. An answer lost on the way is `Unknown`, nothing goes out again, and the same cid is refused.
#[tokio::test]
async fn a_stop_order_that_hits_a_limit_or_fails_is_not_repeated() {
    let (opts, _server, ud, _rx) = start().await;
    let calls = || opts.stop_post_calls.load(Ordering::SeqCst);

    let req = stop(StopOrderKind::StopLoss);
    *opts.stop_post_fail.lock().unwrap() = Some((tonic::Code::ResourceExhausted, "80002".into()));
    assert!(matches!(
        ud.place_stop(req.clone()).await,
        Err(Error::RateLimited { .. })
    ));
    assert_eq!(calls(), 1);
    *opts.stop_post_fail.lock().unwrap() = None;
    ud.place_stop(req)
        .await
        .expect("the same cid after a limit");

    let capped = stop(StopOrderKind::StopLoss);
    *opts.stop_post_fail.lock().unwrap() = Some((tonic::Code::ResourceExhausted, "80007".into()));
    assert!(matches!(
        ud.place_stop(capped).await,
        Err(Error::Rejected { .. })
    ));
    *opts.stop_post_fail.lock().unwrap() = None;

    let lost = stop(StopOrderKind::StopLimit);
    *opts.stop_post_fail.lock().unwrap() = Some((tonic::Code::Unavailable, "down".into()));
    let before = calls();
    assert!(matches!(
        ud.place_stop(lost.clone()).await,
        Err(Error::Unknown)
    ));
    *opts.stop_post_fail.lock().unwrap() = None;
    assert!(matches!(
        ud.place_stop(lost).await,
        Err(Error::Rejected { ref code, .. }) if code == "duplicate_place"
    ));
    assert_eq!(calls(), before + 1, "the repeat never went out");
}

/// A cancel of a stop order that fails on the way is `Unknown` and the stop order stays in the list.
#[tokio::test]
async fn a_stop_cancel_that_fails_leaves_the_stop_in_the_list() {
    let (opts, _server, ud, _rx) = start().await;
    let req = stop(StopOrderKind::StopLoss);
    let cid = req.cid;
    ud.place_stop(req).await.unwrap();

    fail_next_cancel(&opts, tonic::Code::Unavailable, "down");
    assert!(matches!(
        ud.cancel_stop(StopOrderRef::Cid(cid)).await,
        Err(Error::Unknown)
    ));
    assert_eq!(ud.active_stop_orders().await.unwrap().len(), 1);
    ud.cancel_stop(StopOrderRef::Cid(cid))
        .await
        .expect("the cancel after the failure");
    assert!(ud.active_stop_orders().await.unwrap().is_empty());
}

/// A fill that happens while the order streams are down never arrives as an event. After `Reconnected` the bot finds it
/// with `reconcile`, `order_state` and `executions`, and the reopened streams carry the next order.
#[tokio::test]
async fn a_fill_lost_in_the_gap_of_the_streams_is_found_after_the_reconnect() {
    let (opts, server, ud, mut rx) = start().await;
    ud.subscribe_order_state().await.unwrap();
    ud.subscribe_executions().await.unwrap();
    let req = limit_buy();
    let cid = req.cid;
    ud.place(req).await.unwrap();

    let before = opts.user_stream_opens.load(Ordering::SeqCst);
    server.cut_user_streams();
    server.fill_silently(ACCOUNT_A, &cid.hyphenated().to_string());
    brokapi_mocks::time::jump_until("the streams are back", Duration::from_secs(1), || {
        opts.user_stream_opens.load(Ordering::SeqCst) >= before + 2
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
            UnixNanos::from_nanos(0),
            UnixNanos::from_nanos(i64::MAX / 2),
        ))
        .await
        .unwrap();
    assert!(
        fills
            .iter()
            .any(|x| x.qty.decimal() == Decimal::from(20) && x.price.decimal() == dec!(250)),
        "{fills:?}"
    );

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
