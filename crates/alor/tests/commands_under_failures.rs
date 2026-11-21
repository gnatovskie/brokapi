//! What a bot sees when the broker refuses a command (a limit, a block of trading, no money for a replace) or fails it
//! on the way, and when the data socket breaks while an order fills. Against the mock, no network, no money.

mod mock_server;

use std::sync::Arc;
use std::time::Duration;

use brokapi_alor::limits::default_limits;
use brokapi_alor::util::instrument_id;
use brokapi_alor::{UserDataConfig, UserDataConnector};
use brokapi_alor_protocol::config::AlorConfig;
use brokapi_core::{
    ClientOrderId, Decimal, Envelope, Error, Event, ExecutionSource, OrderExecutor, OrderState,
    OrderType, PlaceOrderRequest, PlaceStopOrderRequest, Price, Qty, ReplaceOrderRequest, Side,
    StopExpiry, StopOrderExecutor, StopOrderKind, TimeRange, UnixNanos, dec,
};
use brokapi_runtime::BrokerLimiter;
use mock_server::{MockOptions, MockServer};
use tokio::sync::mpsc;

async fn start() -> (
    Arc<MockOptions>,
    MockServer,
    UserDataConnector,
    mpsc::Receiver<Envelope>,
) {
    let opts = Arc::new(MockOptions::default());
    let server = MockServer::start(Arc::clone(&opts)).await;
    let (tx, rx) = mpsc::channel(256);
    let cfg = AlorConfig::for_mock("mock-refresh-token", &server.http_base);
    let ud = UserDataConnector::start(
        UserDataConfig::new(cfg, "alor-ud"),
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
        instrument: instrument_id("SBER", "TQBR"),
        side: Side::Buy,
        order_type: OrderType::Limit,
        qty: Qty::from(10),
        price: Some(Price::new(dec!(100))),
    }
}

fn stop(kind: StopOrderKind) -> PlaceStopOrderRequest {
    PlaceStopOrderRequest {
        cid: ClientOrderId::new_v4(),
        instrument: instrument_id("SBER", "TQBR"),
        side: Side::Sell,
        kind,
        qty: Qty::from(10),
        stop_price: Price::new(dec!(90)),
        limit_price: (kind == StopOrderKind::StopLimit).then(|| Price::new(dec!(89))),
        expiry: StopExpiry::GoodTillCancel,
    }
}

fn refuse_next(opts: &MockOptions, status: u16, message: &str) {
    *opts.command_refusal.lock().unwrap() = Some((status, message.to_string()));
}

/// Live (13.11.2026): trading of shares is closed for the client, the broker answers `ClientBlocked` ("only a cancel is
/// possible"). It is a refusal with the text of the broker, nothing is placed, and the next order, after the block is
/// gone, goes through.
#[tokio::test]
async fn a_block_of_trading_is_a_refusal_with_the_text_of_the_broker() {
    let (opts, _server, ud, _rx) = start().await;
    refuse_next(
        &opts,
        400,
        "ClientBlocked: Торговля на данной секции рынка приостановлена. Доступна только отмена заявок.",
    );
    let req = limit_buy();
    match ud.place(req).await {
        Err(Error::Rejected { message, .. }) => {
            assert!(message.contains("ClientBlocked"), "{message}")
        }
        other => panic!("expected Rejected, got {other:?}"),
    }
    assert!(ud.active_orders().await.unwrap().is_empty());
    ud.place(limit_buy())
        .await
        .expect("after the block is gone");
    assert_eq!(ud.active_orders().await.unwrap().len(), 1);
}

/// The broker's own limit (HTTP 429) on place, cancel and replace is `RateLimited`: sent once, the order of the command
/// untouched. A cancel that fails on the way is `Unknown`, not repeated, and the order is still read as alive.
#[tokio::test]
async fn a_limit_or_a_failure_on_an_order_command_is_not_repeated_and_the_order_stands() {
    let (opts, _server, ud, _rx) = start().await;
    let commands = || {
        opts.order_commands
            .load(std::sync::atomic::Ordering::SeqCst)
    };

    refuse_next(&opts, 429, "Too many requests");
    assert!(matches!(
        ud.place(limit_buy()).await,
        Err(Error::RateLimited { .. })
    ));
    assert_eq!(commands(), 1, "the place was sent once");
    assert!(ud.active_orders().await.unwrap().is_empty());

    let req = limit_buy();
    let cid = req.cid;
    ud.place(req).await.unwrap();
    let before = commands();

    refuse_next(&opts, 429, "Too many requests");
    assert!(matches!(
        ud.cancel(cid).await,
        Err(Error::RateLimited { .. })
    ));
    refuse_next(&opts, 503, "unavailable");
    assert!(matches!(ud.cancel(cid).await, Err(Error::Unknown)));
    assert_eq!(commands(), before + 2, "one request per call");
    assert!(matches!(
        ud.order_state(cid).await.unwrap().state,
        OrderState::Acked { .. }
    ));

    refuse_next(&opts, 429, "Too many requests");
    let replaced = ClientOrderId::new_v4();
    assert!(matches!(
        ud.replace(ReplaceOrderRequest {
            cid: replaced,
            original_cid: cid,
            qty: None,
            price: Some(Price::new(dec!(101))),
        })
        .await,
        Err(Error::RateLimited { .. })
    ));
    let snap = ud.order_state(cid).await.unwrap();
    assert_eq!(snap.price.map(|p| p.decimal()), Some(dec!(100)));

    ud.cancel(cid).await.expect("the cancel after the failures");
    assert!(matches!(
        ud.order_state(cid).await.unwrap().state,
        OrderState::Cancelled { .. }
    ));
}

/// Live (16.11.2026): a replace on the futures market needs money for both orders at once. The refusal
/// (`InsufficientClientFunds`) leaves the original order alive and unchanged.
#[tokio::test]
async fn a_replace_refused_for_money_leaves_the_original_alive() {
    let (opts, _server, ud, _rx) = start().await;
    let req = limit_buy();
    let cid = req.cid;
    ud.place(req).await.unwrap();
    refuse_next(
        &opts,
        400,
        "InsufficientClientFunds: Нехватка средств по лимитам клиента",
    );
    match ud
        .replace(ReplaceOrderRequest {
            cid: ClientOrderId::new_v4(),
            original_cid: cid,
            qty: None,
            price: Some(Price::new(dec!(101))),
        })
        .await
    {
        Err(Error::Rejected { message, .. }) => {
            assert!(message.contains("InsufficientClientFunds"), "{message}")
        }
        other => panic!("expected Rejected, got {other:?}"),
    }
    let snap = ud.order_state(cid).await.unwrap();
    assert!(matches!(snap.state, OrderState::Acked { .. }));
    assert_eq!(snap.price.map(|p| p.decimal()), Some(dec!(100)));
}

/// A stop order: a limit is `RateLimited` and nothing goes out again by itself, an answer lost on the way is `Unknown` and
/// then the same cid is refused (`duplicate_place`): the stop may exist, a second one must not.
#[tokio::test]
async fn a_stop_order_that_hits_a_limit_or_fails_is_not_repeated() {
    let (opts, _server, ud, _rx) = start().await;
    let commands = || {
        opts.order_commands
            .load(std::sync::atomic::Ordering::SeqCst)
    };

    refuse_next(&opts, 429, "Too many requests");
    assert!(matches!(
        ud.place_stop(stop(StopOrderKind::StopLoss)).await,
        Err(Error::RateLimited { .. })
    ));
    assert_eq!(commands(), 1);
    assert!(ud.active_stop_orders().await.unwrap().is_empty());

    let lost = stop(StopOrderKind::StopLimit);
    refuse_next(&opts, 503, "unavailable");
    assert!(matches!(
        ud.place_stop(lost.clone()).await,
        Err(Error::Unknown)
    ));
    assert_eq!(commands(), 2);
    assert!(matches!(
        ud.place_stop(lost).await,
        Err(Error::Rejected { ref code, .. }) if code == "duplicate_place"
    ));
    assert_eq!(commands(), 2, "the repeat never went out");

    ud.place_stop(stop(StopOrderKind::StopLoss)).await.unwrap();
    assert_eq!(ud.active_stop_orders().await.unwrap().len(), 1);
}

/// A fill that happens while the data socket is down never arrives as an event. After `Reconnected` the bot finds it with
/// `reconcile`, `order_state` and `executions`, and the reopened socket carries the next order.
#[tokio::test]
async fn a_fill_lost_in_the_gap_of_the_socket_is_found_after_the_reconnect() {
    let (opts, server, ud, mut rx) = start().await;
    ud.subscribe_order_state().await.unwrap();
    ud.subscribe_executions().await.unwrap();
    let req = limit_buy();
    let cid = req.cid;
    ud.place(req).await.unwrap();
    brokapi_mocks::time::wait_until("the socket is open", || opts.event_sockets() > 0).await;

    let before = opts.ws_connections();
    server.cut_event_sockets();
    brokapi_mocks::time::wait_until("the socket is down", || opts.event_sockets() == 0).await;
    server.fill(
        &cid.hyphenated().to_string(),
        Qty::from(10),
        Price::new(dec!(100)),
    );
    brokapi_mocks::time::jump_until("the socket is back", Duration::from_secs(1), || {
        opts.ws_connections() > before
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
    assert_eq!(fills[0].qty.decimal(), Decimal::from(10));

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

/// The lot of the mock is 10: the broker counts lots, the bot pieces. Place, replace, stop and fill all convert, and a
/// quantity that is not whole lots is refused before anything is sent (live runs had a lot of 1, futures only).
#[tokio::test]
async fn quantities_in_pieces_become_lots_and_come_back_as_pieces() {
    let (opts, server, ud, _rx) = start().await;
    let commands = || {
        opts.order_commands
            .load(std::sync::atomic::Ordering::SeqCst)
    };

    let mut req = limit_buy();
    req.qty = Qty::from(30);
    let cid = req.cid;
    ud.place(req).await.unwrap();

    let new_cid = ClientOrderId::new_v4();
    ud.replace(ReplaceOrderRequest {
        cid: new_cid,
        original_cid: cid,
        qty: Some(Qty::from(20)),
        price: None,
    })
    .await
    .unwrap();
    let active = ud.active_orders().await.unwrap();
    assert_eq!(active.len(), 1);
    assert_eq!(active[0].cid, new_cid);
    assert_eq!(active[0].qty, Qty::from(20), "2 lots are 20 pieces");

    server.fill(
        &new_cid.hyphenated().to_string(),
        Qty::from(10),
        Price::new(dec!(100)),
    );
    let snap = ud.order_state(new_cid).await.unwrap();
    assert!(
        matches!(snap.state, OrderState::PartiallyFilled { .. }),
        "{snap:?}"
    );
    assert_eq!(snap.filled, Qty::from(10), "one lot is 10 pieces");

    let mut stop_req = stop(StopOrderKind::StopLoss);
    stop_req.qty = Qty::from(30);
    ud.place_stop(stop_req).await.unwrap();
    let stops = ud.active_stop_orders().await.unwrap();
    assert_eq!(stops.len(), 1);
    assert_eq!(stops[0].qty, Qty::from(30), "3 lots are 30 pieces");

    let before = commands();
    for q in ["5", "25"] {
        let mut bad = stop(StopOrderKind::StopLoss);
        bad.qty = q.parse().unwrap();
        assert!(matches!(
            ud.place_stop(bad).await,
            Err(Error::Rejected { ref code, .. }) if code == "invalid_qty"
        ));
        let mut bad = limit_buy();
        bad.qty = q.parse().unwrap();
        assert!(matches!(
            ud.place(bad).await,
            Err(Error::Rejected { ref code, .. }) if code == "invalid_qty"
        ));
    }
    assert_eq!(commands(), before, "nothing was sent for a broken quantity");
}

/// By the docs of the broker a stop order that triggered has the status `filled` and leaves the list of the waiting
/// ones. What order the broker makes of it, and under which cid, is not documented and not seen live: the mock only
/// gives the documented part. The stop leaves `active_stop_orders` and the list still reads without an error.
#[tokio::test]
async fn a_stop_order_that_triggered_leaves_the_list_of_waiting_ones() {
    let (_opts, server, ud, _rx) = start().await;
    let waiting = stop(StopOrderKind::StopLoss);
    let kept = stop(StopOrderKind::StopLimit);
    let cid = waiting.cid;
    ud.place_stop(waiting).await.unwrap();
    ud.place_stop(kept.clone()).await.unwrap();
    assert_eq!(ud.active_stop_orders().await.unwrap().len(), 2);

    server.fill(
        &cid.hyphenated().to_string(),
        Qty::from(10),
        Price::new(dec!(90)),
    );
    let left = ud.active_stop_orders().await.unwrap();
    assert_eq!(left.len(), 1, "{left:?}");
    assert_eq!(left[0].cid, Some(kept.cid));
}
