//! Portfolio mapping.

use brokapi_core::Decimal;
use std::time::Instant;

use brokapi_bcs_protocol::http::models::PortfolioPosition as RestPortfolioPosition;
use brokapi_bcs_protocol::ws::models::WsPortfolioPosition;
use brokapi_core::{Balance, Event, Money, Portfolio, Position, Price, Qty};

use super::{instrument_id_ticker, timestamps};

/// Maps the REST portfolio to a [`Portfolio`].
///
/// A position of type `moneyLimit` is a balance (`available` is the quantity, `blocked` is the locked
/// part, both `0` when missing). A position with a ticker and a board is a holding. A record with
/// neither is dropped. Average price and current price are `None` when the broker leaves them out.
///
/// The broker lists every asset once per settlement term (`T0` settled, `T1`, `T2`, `T365` with every trade
/// of the day). One asset is one balance or one holding: the row of the longest term, the one that already
/// counts today's fills (a share bought today is not in `T0` until settlement). A row without a term
/// counts as settled.
pub fn map_rest_portfolio(positions: &[RestPortfolioPosition], now: Instant) -> Portfolio {
    map_portfolio_positions(positions.iter().map(rest_pos), now)
}

/// Maps a portfolio WebSocket message to an `Event::Portfolio`. The rules are those of
/// [`map_rest_portfolio`]. Every message is a full portfolio, not a change.
pub fn map_ws_portfolio(positions: &[WsPortfolioPosition], now: Instant) -> Event {
    Event::Portfolio(map_portfolio_positions(positions.iter().map(ws_pos), now))
}

struct PosView<'a> {
    position_type: &'a str,
    ticker: Option<&'a str>,
    board: Option<&'a str>,
    currency: Option<&'a str>,
    quantity: Option<Decimal>,
    locked: Option<Decimal>,
    balance_price: Option<Decimal>,
    current_price: Option<Decimal>,
    term: Option<&'a str>,
}

/// The order of the settlement terms: a longer term includes the trades a shorter one does not yet.
fn term_rank(term: Option<&str>) -> u8 {
    match term {
        Some("T365") => 4,
        Some("T2") => 3,
        Some("T1") => 2,
        _ => 1,
    }
}

fn rest_pos(p: &RestPortfolioPosition) -> PosView<'_> {
    PosView {
        position_type: &p.position_type,
        ticker: p.ticker.as_deref(),
        board: p.board.as_deref(),
        currency: p.currency.as_deref(),
        quantity: p.quantity,
        locked: p.locked,
        balance_price: p.balance_price,
        current_price: p.current_price,
        term: p.term.as_deref(),
    }
}

fn ws_pos(p: &WsPortfolioPosition) -> PosView<'_> {
    PosView {
        position_type: &p.position_type,
        ticker: p.ticker.as_deref(),
        board: p.board.as_deref(),
        currency: p.currency.as_deref(),
        quantity: p.quantity,
        locked: p.locked,
        balance_price: p.balance_price,
        current_price: p.current_price,
        term: p.term.as_deref(),
    }
}

fn map_portfolio_positions<'a>(
    positions: impl Iterator<Item = PosView<'a>>,
    now: Instant,
) -> Portfolio {
    let ts = timestamps(now, None);
    let mut balances: Vec<(Balance, u8)> = Vec::new();
    let mut holdings: Vec<(Position, u8)> = Vec::new();

    for pos in positions {
        let rank = term_rank(pos.term);
        if pos.position_type.eq_ignore_ascii_case("moneyLimit") {
            if let Some(currency) = pos.currency {
                let balance = Balance {
                    currency: currency.to_string(),
                    available: Money::new(pos.quantity.unwrap_or(Decimal::ZERO)),
                    blocked: Money::new(pos.locked.unwrap_or(Decimal::ZERO)),
                    ts,
                };
                keep_longest(&mut balances, balance, rank, |a, b| {
                    a.currency == b.currency
                });
            }
        } else if let (Some(ticker), Some(board)) = (pos.ticker, pos.board) {
            let holding = Position {
                instrument: instrument_id_ticker(ticker, board),
                qty: Qty::new(pos.quantity.unwrap_or(Decimal::ZERO)),
                avg_price: pos.balance_price.map(Price::new),
                current_price: pos.current_price.map(Price::new),
                ts,
            };
            keep_longest(&mut holdings, holding, rank, |a, b| {
                a.instrument == b.instrument
            });
        }
    }
    let balances = balances.into_iter().map(|(b, _)| b).collect();
    let holdings = holdings.into_iter().map(|(p, _)| p).collect();

    Portfolio {
        balances,
        positions: holdings,
        ts,
    }
}

/// Puts `item` in `items`, unless the same asset is already there from a row of a term at least as long.
fn keep_longest<T>(items: &mut Vec<(T, u8)>, item: T, rank: u8, same: impl Fn(&T, &T) -> bool) {
    match items.iter_mut().find(|(have, _)| same(have, &item)) {
        Some(slot) if slot.1 < rank => *slot = (item, rank),
        Some(_) => {}
        None => items.push((item, rank)),
    }
}

/// Backward-compatible helper used by UD core tests.
pub fn map_portfolio(positions: &[WsPortfolioPosition], now: Instant) -> Portfolio {
    map_portfolio_positions(positions.iter().map(ws_pos), now)
}
