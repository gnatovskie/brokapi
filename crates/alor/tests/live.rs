//! Level-3: the ALOR connectors against the live API. Built only with `--features integration-live`, never run in
//! CI and never by an agent: they need tokens. Run by hand, one test at a time:
//!
//! ```text
//! # .env (git-ignored)
//! ALOR_REFRESH_TOKEN=...           # from the developer portal
//! ALOR_PORTFOLIO=D12345            # your portfolio
//! ALOR_EXCHANGE=MOEX               # optional, MOEX or SPBX
//! ALOR_ENV=test                    # optional: the test contour (a test account is needed)
//! ALOR_LIVE_TICKER=SBER            # optional, default SBER
//! ALOR_LIVE_BOARD=TQBR             # optional, default TQBR
//! ALOR_LIVE_TRADE=1                # opt-in: places ONE real limit order far from the market
//! ALOR_LIVE_PRICE=<price>          # far BELOW the market, a multiple of the price step
//! ALOR_LIVE_REPLACE=1              # optional: also replace the order (needs about two margins)
//! ALOR_ALLOW_MARGIN=1              # derivatives market for a low-risk client: the owner's consent
//! ALOR_LIVE_STOP_PRICE=<price>     # SELL stops far BELOW the market; price and price * 0.99 on the price step
//!
//! cargo test -p brokapi-alor --features integration-live --test live -- --nocapture --test-threads=1 live_md_reference_and_streams
//! ```
//!
//! Order: read-only tests first (`live_md_*`, `live_ud_*`), then `live_order_roundtrip`. The test contour is the
//! safe place for the round trip; the production contour is a real account.

#![cfg(feature = "integration-live")]

use std::sync::Arc;
use std::time::Duration;

use brokapi_alor::limits::default_limits;
use brokapi_alor::util::instrument_id;
use brokapi_alor::{MarketDataConfig, MarketDataConnector, UserDataConfig, UserDataConnector};
use brokapi_alor_protocol::config::AlorConfig;
use brokapi_core::{
    ClientOrderId, Decimal, Envelope, Event, ExecutionSource, InstrumentId, InstrumentQuery,
    InstrumentSource, MarketDataSource, OrderBookSource, OrderExecutor, OrderType,
    PlaceOrderRequest, PortfolioSource, Price, Qty, ShutdownMode, Side, SubscribeOpts, TimeRange,
    Timeframe, UnixNanos,
};
use brokapi_runtime::BrokerLimiter;
use tokio::sync::mpsc;
use tokio::time::timeout;

fn env(name: &str) -> Option<String> {
    std::env::var(name).ok().filter(|v| !v.is_empty())
}

fn config() -> AlorConfig {
    AlorConfig::from_env().expect("ALOR_REFRESH_TOKEN in .env")
}

fn limiter() -> Arc<BrokerLimiter> {
    BrokerLimiter::new(&default_limits())
}

fn instrument() -> InstrumentId {
    instrument_id(
        &env("ALOR_LIVE_TICKER").unwrap_or_else(|| "SBER".into()),
        &env("ALOR_LIVE_BOARD").unwrap_or_else(|| "TQBR".into()),
    )
}

fn now_nanos() -> i64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_nanos() as i64)
        .unwrap_or(0)
}

/// Waits for an event the predicate accepts; panics with `what` after 30 s (a stream needs trading hours).
async fn next(
    rx: &mut mpsc::Receiver<Envelope>,
    what: &str,
    pred: impl Fn(&Event) -> bool,
) -> Envelope {
    timeout(Duration::from_secs(30), async {
        while let Some(env) = rx.recv().await {
            if pred(&env.payload) {
                return env;
            }
        }
        panic!("event channel closed while waiting for {what}");
    })
    .await
    .unwrap_or_else(|_| panic!("no {what} within 30 s (is the exchange open?)"))
}

#[tokio::test]
async fn live_md_reference_and_streams() {
    let (tx, mut rx) = mpsc::channel(256);
    let md = MarketDataConnector::start(MarketDataConfig::new(config(), "alor-md"), limiter(), tx)
        .await
        .expect("md start");
    let id = instrument();
    let found = md
        .find(InstrumentQuery::ByTicker {
            ticker: id.key.symbol.clone(),
            exchange: None,
        })
        .await
        .expect("find");
    println!("found: {found:?}");
    let params = md.trading_params(&id).await.expect("trading_params");
    println!(
        "lot {} step {} value of a step {}",
        params.lot_size, params.price_step, params.price_step_value
    );
    println!(
        "status: {:?}",
        md.trading_status(&id).await.expect("trading_status")
    );
    println!(
        "book: {:?}",
        md.order_book(&id, 5).await.expect("order_book")
    );
    println!(
        "tape: {:?}",
        md.last_trades(&id, 5).await.expect("last_trades")
    );
    let to = now_nanos();
    let from = to - 3 * 3_600 * 1_000_000_000;
    let bars = md
        .candles(
            &id,
            Timeframe::M10,
            TimeRange::new(UnixNanos::from_nanos(from), UnixNanos::from_nanos(to)),
        )
        .await
        .expect("candles");
    println!("{} candles of 10 minutes", bars.len());

    md.subscribe_order_book(&id, SubscribeOpts::conflate())
        .await
        .expect("subscribe_order_book");
    md.subscribe_quotes(&id, SubscribeOpts::conflate())
        .await
        .expect("subscribe_quotes");
    let book = next(&mut rx, "an order book", |e| {
        matches!(e, Event::OrderBook(_))
    })
    .await;
    println!("{book:?}");
    next(&mut rx, "a quote", |e| matches!(e, Event::Quote(_))).await;
    md.shutdown(ShutdownMode::Immediate).await;
}

#[tokio::test]
async fn live_ud_portfolio_and_orders() {
    let (tx, mut rx) = mpsc::channel(256);
    let ud = UserDataConnector::start(UserDataConfig::new(config(), "alor-ud"), limiter(), tx)
        .await
        .expect("ud start");
    let portfolio = ud.portfolio().await.expect("portfolio");
    println!(
        "{} balances, {} positions",
        portfolio.balances.len(),
        portfolio.positions.len()
    );
    let report = ud.reconcile(&[]).await.expect("reconcile");
    println!("foreign orders: {}", report.foreign.len());
    use brokapi_core::StopOrderExecutor;
    println!(
        "active stop orders: {:?}",
        ud.active_stop_orders().await.expect("active_stop_orders")
    );
    ud.subscribe_portfolio(SubscribeOpts::lossless(64))
        .await
        .expect("subscribe_portfolio");
    next(&mut rx, "a position or a balance", |e| {
        matches!(
            e,
            Event::Positions(_) | Event::Balances(_) | Event::Portfolio(_)
        )
    })
    .await;
    let range = TimeRange::new(
        UnixNanos::from_nanos(now_nanos() - 7 * 24 * 3_600 * 1_000_000_000),
        UnixNanos::from_nanos(now_nanos()),
    );
    println!(
        "{} fills in the last week",
        ud.executions(range).await.expect("executions").len()
    );
    ud.shutdown(ShutdownMode::Immediate).await;
}

/// One real limit order far below the market: place, the stream event, a restart and the reconcile, an optional
/// replace, the cancel. Needs `ALOR_LIVE_TRADE=1`. `ALOR_LIVE_REPLACE=1` adds the replace: on the derivatives
/// market ALOR checks the margin of the new order without releasing the old one, so it needs about two margins.
///
/// Whatever step fails, the test then cancels every active order with one of its cids (found by the cid in the
/// order comment, so after a restart too) and only then fails: a real order is never left at the exchange.
#[tokio::test]
async fn live_order_roundtrip() {
    if env("ALOR_LIVE_TRADE").is_none() {
        println!("skipped: ALOR_LIVE_TRADE is not set");
        return;
    }
    let price: Decimal = env("ALOR_LIVE_PRICE")
        .expect("ALOR_LIVE_PRICE")
        .parse()
        .expect("a number");
    let id = instrument();
    let md_cfg = MarketDataConfig::new(config(), "alor-md");
    let (mtx, _mrx) = mpsc::channel(8);
    let md = MarketDataConnector::start(md_cfg, limiter(), mtx)
        .await
        .expect("md start");
    let lot = md
        .trading_params(&id)
        .await
        .expect("trading_params")
        .lot_size;
    md.shutdown(ShutdownMode::Immediate).await;

    let mut ours = vec![ClientOrderId::new_v4()];
    let outcome = order_steps(&id, lot, Price::new(price), &mut ours).await;
    let leftovers = cancel_ours(&ours).await;
    if let Err(e) = outcome {
        panic!("{e}; cleanup: {leftovers}");
    }
    println!("cleanup: {leftovers}");
}

async fn start_ud() -> Result<(UserDataConnector, mpsc::Receiver<Envelope>), String> {
    let (tx, rx) = mpsc::channel(256);
    let ud = UserDataConnector::start(UserDataConfig::new(config(), "alor-ud"), limiter(), tx)
        .await
        .map_err(|e| format!("ud start: {e:?}"))?;
    Ok((ud, rx))
}

/// Like [`next`], but an error instead of a panic: the caller still has an order to cancel.
async fn try_next(
    rx: &mut mpsc::Receiver<Envelope>,
    what: &str,
    pred: impl Fn(&Event) -> bool,
) -> Result<Envelope, String> {
    timeout(Duration::from_secs(30), async {
        while let Some(env) = rx.recv().await {
            if pred(&env.payload) {
                return Ok(env);
            }
        }
        Err(format!("event channel closed while waiting for {what}"))
    })
    .await
    .unwrap_or_else(|_| Err(format!("no {what} within 30 s")))
}

/// The steps with a live order. `ours` collects every cid sent to the broker, for the cleanup.
async fn order_steps(
    id: &InstrumentId,
    lot: Qty,
    price: Price,
    ours: &mut Vec<ClientOrderId>,
) -> Result<(), String> {
    use brokapi_core::OrderState;
    let mut cid = ours[0];
    let (ud, mut rx) = start_ud().await?;
    let before = ud
        .reconcile(&[])
        .await
        .map_err(|e| format!("reconcile: {e:?}"))?;
    if !before.foreign.is_empty() {
        return Err(format!(
            "{} active orders before the test: not placing",
            before.foreign.len()
        ));
    }
    ud.subscribe_order_state()
        .await
        .map_err(|e| format!("subscribe_order_state: {e:?}"))?;
    let ack = ud
        .place(PlaceOrderRequest {
            cid,
            instrument: id.clone(),
            side: Side::Buy,
            order_type: OrderType::Limit,
            qty: lot,
            price: Some(price),
        })
        .await
        .map_err(|e| format!("place: {e:?}"))?;
    println!("placed {ack:?}");
    try_next(
        &mut rx,
        "the order event",
        |e| matches!(e, Event::Order(o) if o.cid == cid),
    )
    .await?;
    let state = ud
        .order_state(cid)
        .await
        .map_err(|e| format!("order_state: {e:?}"))?;
    println!("state: {:?}", state.state);
    if !matches!(state.state, OrderState::Acked { .. }) {
        return Err(format!("expected Acked, got {:?}", state.state));
    }

    // A restart: a new connector must find the order by the cid in its comment.
    ud.shutdown(ShutdownMode::Immediate).await;
    let (ud, mut rx) = start_ud().await?;
    let report = ud
        .reconcile(&[cid])
        .await
        .map_err(|e| format!("reconcile after restart: {e:?}"))?;
    println!(
        "after restart: confirmed {:?}, closed offline {:?}, foreign {}",
        report.confirmed.iter().map(|s| s.cid).collect::<Vec<_>>(),
        report.closed_offline,
        report.foreign.len()
    );
    if !report
        .confirmed
        .iter()
        .any(|s| s.cid == cid && s.broker_id == Some(ack.broker_id.clone()))
    {
        return Err("the order is not confirmed by the reconcile after the restart".into());
    }
    ud.subscribe_order_state()
        .await
        .map_err(|e| format!("subscribe_order_state after restart: {e:?}"))?;

    if env("ALOR_LIVE_REPLACE").is_some() {
        let second = ClientOrderId::new_v4();
        ours.push(second);
        let replaced = ud
            .replace(brokapi_core::ReplaceOrderRequest {
                cid: second,
                original_cid: cid,
                qty: Some(Qty::new(lot.decimal())),
                price: Some(price),
            })
            .await
            .map_err(|e| format!("replace: {e:?}"))?;
        println!("replaced {replaced:?}");
        cid = second;
    } else {
        println!("replace skipped: ALOR_LIVE_REPLACE is not set");
    }

    ud.cancel(cid).await.map_err(|e| format!("cancel: {e:?}"))?;
    try_next(&mut rx, "the cancel event", |e| {
        matches!(e, Event::Order(o) if o.cid == cid && matches!(o.state, OrderState::Cancelled { .. }))
    })
    .await?;
    let after = ud
        .order_state(cid)
        .await
        .map_err(|e| format!("order_state after cancel: {e:?}"))?;
    println!("after cancel: {:?}", after.state);
    if !matches!(after.state, OrderState::Cancelled { .. }) {
        return Err(format!("expected Cancelled, got {:?}", after.state));
    }
    ud.shutdown(ShutdownMode::Immediate).await;
    Ok(())
}

/// Cancels every active order with one of `ours` through a fresh connector. Returns what it did, for the report.
async fn cancel_ours(ours: &[ClientOrderId]) -> String {
    let (ud, _rx) = match start_ud().await {
        Ok(v) => v,
        Err(e) => return format!("FAILED, check the account by hand: {e}"),
    };
    let active = match ud.active_orders().await {
        Ok(v) => v,
        Err(e) => return format!("FAILED to list active orders, check the account by hand: {e:?}"),
    };
    let mut done = Vec::new();
    for s in active.iter().filter(|s| ours.contains(&s.cid)) {
        match ud.cancel(s.cid).await {
            Ok(()) => done.push(format!("cancelled {}", s.cid)),
            Err(e) => done.push(format!("FAILED to cancel {}, cancel by hand: {e:?}", s.cid)),
        }
    }
    ud.shutdown(ShutdownMode::Immediate).await;
    if done.is_empty() {
        "nothing of ours active".into()
    } else {
        done.join("; ")
    }
}

/// Stop orders far from the market: the common conformance path (a SELL stop-loss cancelled by cid, a SELL
/// stop-limit cancelled by the broker id, a stop-limit without a limit price refused locally). Needs
/// `ALOR_LIVE_TRADE=1` and `ALOR_LIVE_STOP_PRICE` far BELOW the market, so that nothing triggers.
///
/// Refuses to start when the account already has active stop orders. Whatever step fails, every stop order
/// that was not there before is cancelled, and only then the test fails.
#[tokio::test]
async fn live_stop_order_roundtrip() {
    use brokapi_core::{StopOrderExecutor, StopOrderRef};
    use futures_util::FutureExt;

    if env("ALOR_LIVE_TRADE").is_none() {
        println!("skipped: ALOR_LIVE_TRADE is not set");
        return;
    }
    let stop: Decimal = env("ALOR_LIVE_STOP_PRICE")
        .expect("ALOR_LIVE_STOP_PRICE")
        .parse()
        .expect("a number");
    let (ud, _rx) = start_ud().await.unwrap_or_else(|e| panic!("{e}"));
    ud.reconcile(&[]).await.expect("reconcile");
    assert!(ud.caps().supports_stop_orders);
    let before = ud.active_stop_orders().await.expect("active_stop_orders");
    assert!(
        before.is_empty(),
        "{} active stop orders before the test: not placing",
        before.len()
    );

    let outcome = std::panic::AssertUnwindSafe(brokapi_conformance::stop_order_far_from_market(
        &ud,
        &instrument(),
        Price::new(stop),
    ))
    .catch_unwind()
    .await;

    let mut cleanup = Vec::new();
    match ud.active_stop_orders().await {
        Ok(left) => {
            for s in left {
                let id = s.broker_id.clone();
                match ud.cancel_stop(StopOrderRef::Broker(id.clone())).await {
                    Ok(()) => cleanup.push(format!("cancelled stop {}", id.as_str())),
                    Err(e) => cleanup.push(format!(
                        "FAILED to cancel stop {}, cancel by hand: {e:?}",
                        id.as_str()
                    )),
                }
            }
        }
        Err(e) => cleanup.push(format!(
            "FAILED to list stop orders, check the account by hand: {e:?}"
        )),
    }
    ud.shutdown(ShutdownMode::Immediate).await;
    let cleanup = if cleanup.is_empty() {
        "nothing left".to_string()
    } else {
        cleanup.join("; ")
    };
    println!("cleanup: {cleanup}");
    if let Err(panic) = outcome {
        std::panic::resume_unwind(panic);
    }
}
