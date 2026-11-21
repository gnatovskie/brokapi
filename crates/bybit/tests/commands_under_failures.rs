//! What a bot sees when the exchange refuses an order command (a limit, a refusal) or fails it on the way. Against the
//! mock, no network, no money.

mod mock_server;

use std::sync::Arc;
use std::sync::atomic::Ordering;

use brokapi_bybit::limits::default_limits;
use brokapi_bybit::util::instrument_id;
use brokapi_bybit::{UserDataConfig, UserDataConnector};
use brokapi_bybit_protocol::config::BybitConfig;
use brokapi_core::{
    ClientOrderId, Decimal, Envelope, Error, Event, ExecutionSource, InstrumentId, OrderExecutor,
    OrderState, OrderType, PlaceOrderRequest, PlaceStopOrderRequest, Price, Qty, Side, StopExpiry,
    StopOrderExecutor, StopOrderKind, StopOrderRef, TimeRange, UnixNanos, dec,
};
use brokapi_runtime::BrokerLimiter;
use mock_server::{DEFAULT_KEY, MockOptions, MockServer, SECRET};
use tokio::sync::mpsc;

fn mock() -> InstrumentId {
    instrument_id("MOCKUSDT", "linear")
}

async fn start() -> (
    Arc<MockOptions>,
    UserDataConnector,
    mpsc::Receiver<Envelope>,
) {
    let opts = Arc::new(MockOptions::default());
    let server = MockServer::start(Arc::clone(&opts)).await;
    let (ud, rx) = start_on(&opts, &server).await;
    std::mem::forget(server);
    (opts, ud, rx)
}

async fn start_on(
    _opts: &Arc<MockOptions>,
    server: &MockServer,
) -> (UserDataConnector, mpsc::Receiver<Envelope>) {
    let mut limits = default_limits();
    for bucket in limits.buckets.values_mut() {
        bucket.burst = bucket.burst.max(1000);
    }
    let (tx, rx) = mpsc::channel(256);
    let cfg = BybitConfig::for_mock(DEFAULT_KEY, SECRET, &server.http_base);
    let ud = UserDataConnector::start(
        UserDataConfig::new(cfg, "bybit-ud"),
        BrokerLimiter::new(&limits),
        tx,
    )
    .await
    .expect("ud start");
    ud.reconcile(&[]).await.expect("reconcile");
    (ud, rx)
}

fn limit_buy() -> PlaceOrderRequest {
    PlaceOrderRequest {
        cid: ClientOrderId::new_v4(),
        instrument: mock(),
        side: Side::Buy,
        order_type: OrderType::Limit,
        qty: Qty::from(10),
        price: Some(Price::new(dec!(50))),
    }
}

fn stop_loss() -> PlaceStopOrderRequest {
    PlaceStopOrderRequest {
        cid: ClientOrderId::new_v4(),
        instrument: mock(),
        side: Side::Sell,
        kind: StopOrderKind::StopLoss,
        qty: Qty::from(10),
        stop_price: Price::new(dec!(40)),
        limit_price: None,
        expiry: StopExpiry::GoodTillCancel,
    }
}

fn refuse_next(opts: &MockOptions, status: u16, code: i64, message: &str) {
    let body =
        format!(r#"{{"retCode":{code},"retMsg":"{message}","result":{{}},"retExtInfo":{{}}}}"#);
    *opts.command_refusal.lock().unwrap() = Some((status, body));
}

/// The limit of the exchange (`10006`) on place is `RateLimited`, sent once, nothing placed. A refusal with a code of the
/// exchange (`110007`, not enough balance) is `Rejected` with the code, nothing placed. Every cid is used once: after a
/// sent command, refused or not, the same cid is `duplicate_place`, and a new cid goes through.
#[tokio::test]
async fn a_limit_or_a_refusal_on_place_places_nothing_and_the_cid_is_used_up() {
    let (opts, ud, _rx) = start().await;
    let commands = || opts.order_commands.load(Ordering::SeqCst);
    let req = limit_buy();
    let refused = limit_buy();

    refuse_next(&opts, 200, 10006, "Too many visits!");
    assert!(matches!(
        ud.place(req.clone()).await,
        Err(Error::RateLimited { .. })
    ));
    assert_eq!(commands(), 1, "sent once");
    assert!(ud.active_orders().await.unwrap().is_empty());

    refuse_next(&opts, 200, 110007, "ab not enough for new order");
    match ud.place(refused.clone()).await {
        Err(Error::Rejected { code, .. }) => assert_eq!(code, "bybit_110007"),
        other => panic!("expected Rejected, got {other:?}"),
    }
    assert!(ud.active_orders().await.unwrap().is_empty());

    for used in [req, refused] {
        assert!(matches!(
            ud.place(used).await,
            Err(Error::Rejected { ref code, .. }) if code == "duplicate_place"
        ));
    }
    assert_eq!(commands(), 2, "no repeat went out");
    ud.place(limit_buy()).await.expect("a new cid goes through");
    assert_eq!(ud.active_orders().await.unwrap().len(), 1);
}

/// A cancel that fails on the way (HTTP 503) is `Unknown` and not repeated, the order is still alive, and the next cancel
/// goes through. A cancel at the limit is `RateLimited`.
#[tokio::test]
async fn a_cancel_that_hits_a_limit_or_fails_leaves_the_order_alive() {
    let (opts, ud, _rx) = start().await;
    let req = limit_buy();
    let cid = req.cid;
    ud.place(req).await.unwrap();
    let before = opts.order_commands.load(Ordering::SeqCst);

    refuse_next(&opts, 200, 10006, "Too many visits!");
    assert!(matches!(
        ud.cancel(cid).await,
        Err(Error::RateLimited { .. })
    ));
    refuse_next(&opts, 503, 10016, "unavailable");
    assert!(matches!(ud.cancel(cid).await, Err(Error::Unknown)));
    assert_eq!(opts.order_commands.load(Ordering::SeqCst), before + 2);
    assert!(matches!(
        ud.order_state(cid).await.unwrap().state,
        OrderState::Acked { .. }
    ));

    ud.cancel(cid).await.expect("the cancel after the failures");
    assert!(matches!(
        ud.order_state(cid).await.unwrap().state,
        OrderState::Cancelled { .. }
    ));
}

/// A stop order: a limit is `RateLimited` and the cid is used up all the same, a failure on the way is `Unknown`, and
/// nothing is sent twice. A cancel of a stop order that fails leaves it in the list.
#[tokio::test]
async fn a_stop_order_that_hits_a_limit_or_fails_is_not_repeated() {
    let (opts, ud, _rx) = start().await;
    let commands = || opts.order_commands.load(Ordering::SeqCst);
    let req = stop_loss();

    refuse_next(&opts, 200, 10006, "Too many visits!");
    assert!(matches!(
        ud.place_stop(req.clone()).await,
        Err(Error::RateLimited { .. })
    ));
    assert!(matches!(
        ud.place_stop(req).await,
        Err(Error::Rejected { ref code, .. }) if code == "duplicate_place"
    ));
    assert_eq!(commands(), 1, "the repeat never went out");

    let lost = stop_loss();
    refuse_next(&opts, 503, 10016, "unavailable");
    let before = commands();
    assert!(matches!(
        ud.place_stop(lost.clone()).await,
        Err(Error::Unknown)
    ));
    assert!(matches!(
        ud.place_stop(lost).await,
        Err(Error::Rejected { ref code, .. }) if code == "duplicate_place"
    ));
    assert_eq!(commands(), before + 1, "the repeat never went out");

    let kept = stop_loss();
    let kept_cid = kept.cid;
    ud.place_stop(kept).await.unwrap();
    refuse_next(&opts, 503, 10016, "unavailable");
    assert!(matches!(
        ud.cancel_stop(StopOrderRef::Cid(kept_cid)).await,
        Err(Error::Unknown)
    ));
    assert_eq!(ud.active_stop_orders().await.unwrap().len(), 1);
}

/// A fill that happens while the private socket is down never arrives as an event. After `Reconnected` the bot finds it with
/// `reconcile`, `order_state` and `executions`, and the reopened socket carries the next order.
#[tokio::test]
async fn a_fill_lost_in_the_gap_of_the_socket_is_found_after_the_reconnect() {
    let opts = Arc::new(MockOptions::default());
    let server = MockServer::start(Arc::clone(&opts)).await;
    let (ud, mut rx) = start_on(&opts, &server).await;
    ud.subscribe_order_state().await.unwrap();
    ud.subscribe_executions().await.unwrap();
    let req = limit_buy();
    let cid = req.cid;
    ud.place(req).await.unwrap();

    let before = opts.ws_connections();
    server.cut_event_sockets();
    brokapi_mocks::time::wait_until("the socket is down", || opts.event_sockets() == 0).await;
    server.fill(
        &cid.hyphenated().to_string(),
        Qty::from(10),
        Price::new(dec!(50)),
    );
    brokapi_mocks::time::jump_until(
        "the socket is back",
        std::time::Duration::from_secs(1),
        || opts.ws_connections() > before,
    )
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
    assert!(
        fills
            .iter()
            .any(|x| x.cid == Some(cid) && x.qty == Qty::from(10)),
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

/// The list of working orders comes in pages of at most 50: a bot with 120 orders gets all 120, once each, and the lists the
/// bot asks for (`active_orders`, `reconcile`) agree on them.
#[tokio::test]
async fn more_orders_than_a_page_are_all_listed() {
    let (opts, ud, _rx) = start().await;
    let mut cids = Vec::new();
    for _ in 0..120 {
        let req = limit_buy();
        cids.push(req.cid);
        ud.place(req).await.unwrap();
    }
    let active = ud.active_orders().await.unwrap();
    assert_eq!(active.len(), 120);
    let mut seen: Vec<_> = active.iter().map(|o| o.cid).collect();
    seen.sort();
    seen.dedup();
    assert_eq!(seen.len(), 120, "every order once");
    for cid in &cids {
        assert!(active.iter().any(|o| o.cid == *cid));
    }
    let pages = opts
        .served
        .lock()
        .unwrap()
        .iter()
        .filter(|s| s.contains("order/realtime"))
        .count();
    assert!(pages >= 3, "120 orders take at least 3 pages, saw {pages}");

    let report = ud.reconcile(&cids).await.unwrap();
    assert_eq!(report.confirmed.len(), 120, "{report:?}");
    assert!(report.closed_offline.is_empty() && report.foreign.is_empty());
}

/// The connector checks the step and the smallest quantity of a symbol before it sends, but not the smallest value of an
/// order (`minNotionalValue`). The exchange refuses it: `Rejected` with its code, nothing placed, sent once.
#[tokio::test]
async fn an_order_below_the_smallest_value_is_refused_by_the_exchange() {
    let (opts, ud, _rx) = start().await;
    let mut small = limit_buy();
    small.price = Some(Price::new(dec!(0.1)));
    match ud.place(small).await {
        Err(Error::Rejected { code, .. }) => assert_eq!(code, "bybit_110094"),
        other => panic!("expected Rejected, got {other:?}"),
    }
    assert_eq!(opts.order_commands.load(Ordering::SeqCst), 1, "sent once");
    assert!(ud.active_orders().await.unwrap().is_empty());
    ud.place(limit_buy())
        .await
        .expect("an order of enough value");
}
