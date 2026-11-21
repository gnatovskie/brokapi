//! The context of a log line: which connector wrote it and which command it belongs to.
//!
//! A connector makes [`connector`] once, keeps it and runs all its tasks in it, so every line shows the broker, the
//! role and the account (as a mask, `****3456`). A trading command and `reconcile` run in [`op`] through [`command`]. The
//! span of the connector is the parent of `op`: a command is called by the code of the bot, out of the tasks of the
//! connector, and still shows the broker and the account. Every line inside, the requests of the protocol layer too,
//! shows the cid and the instrument, so
//! every line inside, the requests of the protocol layer too, shows the cid and the instrument, and the command
//! ends with one line of its outcome:
//!
//! | Outcome | Level |
//! |---|---|
//! | `Ok` | `info`, with the broker's order id or the counts of `reconcile` |
//! | `Err(Error::Unknown)` | `warn`: the order may exist, the command must not be repeated |
//! | any other `Err` | `debug`: the caller gets it and decides |
//!
//! Both spans are of the level `error`, so they are on whenever any line is: with `BROKAPI_LOG=warn,brokapi_bcs=debug`
//! a span of the level `info` would be off, and a warning of an unknown order would come without its cid.
//!
//! # Examples
//!
//! ```
//! use brokapi_core::{Error, InstrumentKey};
//! use brokapi_runtime::spans::{self, Op, Role};
//! use tracing::Instrument;
//!
//! # #[tokio::main(flavor = "current_thread")]
//! # async fn main() {
//! let connector = spans::connector("tbank", Role::Ud, Some("2000123456"));
//! let sber = InstrumentKey::new("SBER", "TQBR");
//! let cid = brokapi_core::ClientOrderId::new_v4();
//!
//! let cancelled: Result<(), Error> = spans::command(
//!     spans::op(&connector, Op::Cancel, Some(cid), Some(&sber)),
//!     async { Ok(()) },
//! )
//! .await;
//!
//! // A task of the connector runs in its span.
//! tokio::spawn(async { tracing::debug!("stream opened") }.instrument(connector.clone()));
//! assert!(cancelled.is_ok());
//! # }
//! ```

use std::fmt;
use std::future::Future;

use brokapi_core::{
    BrokerOrderId, ClientOrderId, Error, InstrumentKey, OrderAck, ReconcileReport, ShutdownReport,
    SubscriptionId,
};
use tracing::field::{Empty, display};
use tracing::{Instrument, Span, debug, info, warn};

/// The role of a connector: market data or user data (orders, portfolio).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Role {
    /// Market data: order books, trades, quotes, candles, instruments.
    Md,
    /// User data: orders, executions, portfolio.
    Ud,
}

impl Role {
    /// `md` or `ud`, as the field `role` shows it.
    pub fn as_str(self) -> &'static str {
        match self {
            Role::Md => "md",
            Role::Ud => "ud",
        }
    }
}

impl fmt::Display for Role {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.as_str())
    }
}

/// A command of a connector, as the field `op` shows it.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[non_exhaustive]
pub enum Op {
    /// `place`.
    Place,
    /// `cancel`.
    Cancel,
    /// `replace`.
    Replace,
    /// `place_stop`.
    PlaceStop,
    /// `cancel_stop`.
    CancelStop,
    /// `reconcile`.
    Reconcile,
    /// `subscribe`.
    Subscribe,
    /// `unsubscribe`.
    Unsubscribe,
    /// `shutdown`.
    Shutdown,
}

impl Op {
    /// The name of the command: `place`, `cancel`, ...
    pub fn as_str(self) -> &'static str {
        match self {
            Op::Place => "place",
            Op::Cancel => "cancel",
            Op::Replace => "replace",
            Op::PlaceStop => "place_stop",
            Op::CancelStop => "cancel_stop",
            Op::Reconcile => "reconcile",
            Op::Subscribe => "subscribe",
            Op::Unsubscribe => "unsubscribe",
            Op::Shutdown => "shutdown",
        }
    }
}

impl fmt::Display for Op {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.as_str())
    }
}

/// The span of a connector, `connector{broker, role, account}`. The account is shown as a mask.
///
/// A connector keeps it for its whole life: the tasks it spawns take it with [`Instrument::instrument`], its
/// commands give it to [`op`] as the parent.
pub fn connector(broker: &'static str, role: Role, account: Option<&str>) -> Span {
    let span = tracing::error_span!("connector", broker, role = role.as_str(), account = Empty);
    if let Some(account) = account {
        record_account(&span, account);
    }
    span
}

/// Writes the account into a [`connector`] span, as a mask: for a connector that learns its account after the
/// span is made (the only open account of a token).
pub fn record_account(span: &Span, account: &str) {
    span.record("account", brokapi_redact::mask_account(account).as_str());
}

/// Spawns a task of a connector in its span: the lines of the task show the broker, the role and the account,
/// whoever started it.
pub fn spawn_in<F>(span: &Span, work: F) -> tokio::task::JoinHandle<F::Output>
where
    F: Future + Send + 'static,
    F::Output: Send + 'static,
{
    tokio::spawn(work.instrument(span.clone()))
}

/// The span of a command, `op{op, cid, instrument, broker_order_id}`, inside the span of its connector.
///
/// `broker_order_id` is empty at first: the connector fills it with [`record_broker_order_id`] as soon as the
/// broker gives it, so the lines after that show it too.
pub fn op(
    connector: &Span,
    op: Op,
    cid: Option<ClientOrderId>,
    instrument: Option<&InstrumentKey>,
) -> Span {
    tracing::error_span!(
        parent: connector,
        "op",
        op = op.as_str(),
        cid = cid.map(display),
        instrument = instrument.map(|i| display(i.display())),
        broker_order_id = Empty,
    )
}

/// The line of a connector that has started, on `info`. Called inside its [`connector`] span.
pub fn started() {
    info!("connector started");
}

/// The line of a connector that has stopped, on `info`, with what its shutdown did. Orders left without an answer are
/// written by [`OrderRuntime`](crate::OrderRuntime) on `warn`, one line each.
pub fn stopped(report: &ShutdownReport) {
    info!(
        unsubscribed = report.unsubscribed,
        unsubscribe_failed = report.unsubscribe_failed,
        inflight_abandoned = report.inflight_abandoned.len(),
        "connector stopped"
    );
}

/// Writes the broker's id of the order into the current [`op`] span.
pub fn record_broker_order_id(id: &BrokerOrderId) {
    Span::current().record("broker_order_id", display(&id.0));
}

/// Runs a command in its span and writes its outcome: `info` for `Ok`, `warn` for
/// [`Error::Unknown`], `debug` for any other error.
///
/// The work stays in the span when it is moved to a task of its own by [`run_detached`](crate::run_detached).
pub async fn command<T, F>(span: Span, work: F) -> Result<T, Error>
where
    T: Outcome,
    F: Future<Output = Result<T, Error>>,
{
    let started = crate::now();
    let result = work.instrument(span.clone()).await;
    let latency_ms = crate::now().saturating_duration_since(started).as_millis() as u64;
    span.in_scope(|| match &result {
        Ok(value) => value.done(latency_ms),
        Err(Error::Unknown) => warn!(
            latency_ms,
            "command outcome unknown, the order may exist: do not repeat, reconcile"
        ),
        Err(e) => debug!(latency_ms, error = %e, "command failed"),
    });
    result
}

/// What a command returns, and how its success is written to the log by [`command`].
pub trait Outcome {
    /// Writes the line of a successful command, on `info`.
    fn done(&self, latency_ms: u64);
}

impl Outcome for () {
    fn done(&self, latency_ms: u64) {
        info!(latency_ms, "command done");
    }
}

impl Outcome for OrderAck {
    fn done(&self, latency_ms: u64) {
        record_broker_order_id(&self.broker_id);
        info!(latency_ms, "command done");
    }
}

impl Outcome for SubscriptionId {
    fn done(&self, latency_ms: u64) {
        info!(latency_ms, sub = self.0, "subscription opened");
    }
}

impl Outcome for ReconcileReport {
    fn done(&self, latency_ms: u64) {
        info!(
            latency_ms,
            confirmed = self.confirmed.len(),
            closed_offline = self.closed_offline.len(),
            foreign = self.foreign.len(),
            "reconciled"
        );
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::Write;
    use std::sync::{Arc, Mutex};

    /// The lines written while `f` runs, at `level` and up.
    fn lines(level: &str, f: impl AsyncFnOnce()) -> String {
        #[derive(Clone, Default)]
        struct Buf(Arc<Mutex<Vec<u8>>>);
        impl Write for Buf {
            fn write(&mut self, b: &[u8]) -> std::io::Result<usize> {
                self.0.lock().unwrap().extend_from_slice(b);
                Ok(b.len())
            }
            fn flush(&mut self) -> std::io::Result<()> {
                Ok(())
            }
        }
        let buf = Buf::default();
        let out = buf.clone();
        let subscriber = tracing_subscriber::fmt()
            .with_writer(move || out.clone())
            .with_env_filter(tracing_subscriber::EnvFilter::new(level))
            .with_ansi(false)
            .finish();
        let _guard = tracing::subscriber::set_default(subscriber);
        tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .unwrap()
            .block_on(f());
        String::from_utf8(buf.0.lock().unwrap().clone()).unwrap()
    }

    fn ack(cid: ClientOrderId) -> OrderAck {
        OrderAck {
            cid,
            broker_id: BrokerOrderId("B-77".into()),
            ts: brokapi_core::Timestamps::new(brokapi_core::UnixNanos(0), crate::now()),
        }
    }

    #[test]
    fn the_connector_shows_the_account_as_a_mask() {
        let text = lines("info", async || {
            connector("tbank", Role::Ud, Some("2000123456")).in_scope(|| info!("started"));
        });
        assert!(
            text.contains("connector{broker=\"tbank\" role=\"ud\" account=\"****3456\"}"),
            "{text}"
        );
        assert!(!text.contains("2000123456"), "{text}");
    }

    #[test]
    fn a_placed_order_ends_with_its_ids_on_info() {
        let cid = ClientOrderId::new_v4();
        let sber = InstrumentKey::new("SBER", "TQBR");
        let text = lines("info", async || {
            let r = command(
                op(&Span::none(), Op::Place, Some(cid), Some(&sber)),
                async { Ok(ack(cid)) },
            )
            .await;
            assert!(r.is_ok());
        });
        assert!(text.contains(" INFO "), "{text}");
        assert!(text.contains("op=\"place\""), "{text}");
        assert!(text.contains(&format!("cid={cid}")), "{text}");
        assert!(text.contains("instrument=SBER@TQBR"), "{text}");
        assert!(text.contains("broker_order_id=B-77"), "{text}");
        assert!(text.contains("command done"), "{text}");
    }

    #[test]
    fn unknown_is_a_warning_and_other_errors_are_debug() {
        let unknown = lines("info", async || {
            let _ = command::<(), _>(op(&Span::none(), Op::Cancel, None, None), async {
                Err(Error::Unknown)
            })
            .await;
        });
        assert!(
            unknown.contains(" WARN ") && unknown.contains("do not repeat"),
            "{unknown}"
        );

        let refused = || async {
            let _ = command::<(), _>(op(&Span::none(), Op::Cancel, None, None), async {
                Err(Error::Rejected {
                    code: "not_found".into(),
                    message: "no such order".into(),
                })
            })
            .await;
        };
        assert_eq!(lines("info", refused), "");
        let text = lines("debug", refused);
        assert!(
            text.contains("DEBUG") && text.contains("no such order"),
            "{text}"
        );
    }

    #[test]
    fn the_counts_of_reconcile_are_written() {
        let text = lines("info", async || {
            // The struct is non_exhaustive: built from JSON, as a connector of another crate would not.
            let report: ReconcileReport = serde_json::from_value(serde_json::json!({
                "confirmed": [],
                "closed_offline": [ClientOrderId::new_v4(), ClientOrderId::new_v4()],
                "foreign": [],
            }))
            .unwrap();
            let _ = command(op(&Span::none(), Op::Reconcile, None, None), async {
                Ok(report)
            })
            .await;
        });
        assert!(
            text.contains("reconciled") && text.contains("confirmed=0 closed_offline=2 foreign=0"),
            "{text}"
        );
    }

    #[test]
    fn a_detached_command_keeps_its_span() {
        let cid = ClientOrderId::new_v4();
        let text = lines("debug", async || {
            let _ = command(op(&Span::none(), Op::Place, Some(cid), None), async {
                crate::run_detached(async {
                    debug!("request sent");
                })
                .await;
                Ok(())
            })
            .await;
        });
        let sent = text
            .lines()
            .find(|l| l.contains("request sent"))
            .unwrap_or_default();
        assert!(sent.contains(&format!("cid={cid}")), "{text}");
    }

    #[test]
    fn the_order_runtime_writes_the_states_of_an_order() {
        use brokapi_sansio::OrderSignal;
        use std::time::Duration;
        let (acked, late) = (ClientOrderId::new_v4(), ClientOrderId::new_v4());
        let text = lines("info", async || {
            let mut orders = crate::OrderRuntime::with_timeout(Duration::from_secs(5));
            let t0 = crate::now();
            orders.register_place(acked, t0);
            orders.register_place(late, t0);
            orders.apply_signal(
                acked,
                OrderSignal::Ack {
                    broker_id: BrokerOrderId("B-1".into()),
                },
                t0,
            );
            orders.apply_signal(acked, OrderSignal::Filled, t0);
            orders.sweep_timeouts(t0 + Duration::from_secs(6));
        });
        let line = |cid: ClientOrderId, what: &str| {
            text.lines()
                .find(|l| l.contains(&cid.to_string()) && l.contains(what))
                .unwrap_or_default()
                .to_owned()
        };
        assert!(line(acked, "order acked").contains(" INFO "), "{text}");
        assert!(
            line(acked, "order acked").contains("broker_order_id=B-1"),
            "{text}"
        );
        assert!(line(acked, "order filled").contains(" INFO "), "{text}");
        assert!(line(late, "order unknown").contains(" WARN "), "{text}");
    }

    #[test]
    fn a_warning_keeps_the_context_when_only_one_crate_is_on_debug() {
        let cid = ClientOrderId::new_v4();
        let text = lines("warn,other_crate=debug", async || {
            let bcs = connector("bcs", Role::Ud, None);
            let _ = command::<(), _>(op(&bcs, Op::Place, Some(cid), None), async {
                Err(Error::Unknown)
            })
            .await;
        });
        assert!(text.contains("broker=\"bcs\""), "{text}");
        assert!(text.contains(&format!("cid={cid}")), "{text}");
    }
}
