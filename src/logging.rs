//! Turning the log of the library on: one call, the level `info` by default, the variable `BROKAPI_LOG`.
//!
//! The crates of brokapi write their events with [`tracing`](https://docs.rs/tracing) and never install a
//! subscriber themselves: without one there is no log and almost no cost. [`init`] installs a subscriber that
//! writes to stderr. A program with a subscriber of its own takes [`filter`] and keeps its own output.
//!
//! The level is chosen in this order:
//!
//! 1. the argument of [`init`] or [`filter`], when it is given;
//! 2. the variable `BROKAPI_LOG`;
//! 3. `info`.
//!
//! Both take a level (`debug`) or directives per module (`info,brokapi_bcs=debug`). A wrong argument is an
//! error. A wrong variable is ignored with a warning in the log: a typo in the environment must not stop a
//! trading bot.
//!
//! Every line shows its source, the path of the module that wrote it (`brokapi_bcs::ud`), so a line at `debug`
//! can be switched on for one broker only.
//!
//! # Examples
//!
//! ```
//! // In `main`, before the connectors start. `None` reads BROKAPI_LOG and falls back to `info`.
//! brokapi::logging::init(None)?;
//!
//! // The subscriber of a process is installed once.
//! assert!(matches!(
//!     brokapi::logging::init(Some("debug")),
//!     Err(brokapi::logging::Error::AlreadySet)
//! ));
//! # Ok::<(), brokapi::logging::Error>(())
//! ```

use std::fmt;

use tracing_subscriber::EnvFilter;
use tracing_subscriber::filter::LevelFilter;

/// The variable with the level or the directives of the log.
pub const ENV: &str = "BROKAPI_LOG";

/// The level when neither an argument nor [`ENV`] gives one.
pub const DEFAULT: &str = "info";

/// Why the log could not be turned on.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Error {
    /// The argument is not a level and not a list of directives. Holds the part that could not be read.
    BadDirective(String),
    /// The process already has a subscriber: installed earlier by [`init`] or by the program itself.
    AlreadySet,
}

impl fmt::Display for Error {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Error::BadDirective(part) => write!(
                f,
                "bad log directive {part:?}: expected a level (error, warn, info, debug, trace, off) \
                 or module=level, for example info,brokapi_bcs=debug"
            ),
            Error::AlreadySet => f.write_str("the log subscriber of this process is already set"),
        }
    }
}

impl std::error::Error for Error {}

/// The filter of the log: the argument, else [`ENV`], else `info`. See the [module docs](self).
///
/// For a program that installs its own subscriber and wants the same rules for the level.
///
/// # Errors
///
/// [`Error::BadDirective`] if `explicit` cannot be read. A wrong [`ENV`] is not an error: the filter is `info`.
///
/// # Examples
///
/// ```
/// use tracing_subscriber::prelude::*;
///
/// let filter = brokapi::logging::filter(Some("info,brokapi_bcs=debug"))?;
/// let subscriber = tracing_subscriber::registry().with(tracing_subscriber::fmt::layer().with_filter(filter));
/// # drop(subscriber);
///
/// assert!(brokapi::logging::filter(Some("loud")).is_err());
/// # Ok::<(), brokapi::logging::Error>(())
/// ```
pub fn filter(explicit: Option<&str>) -> Result<EnvFilter, Error> {
    let (directives, _) = choose(explicit, std::env::var(ENV).ok().as_deref())?;
    build(&directives)
}

/// Installs a subscriber that writes the log to stderr, at the level chosen as in [`filter`].
///
/// Call it once, early in `main`. A wrong [`ENV`] is reported in the log itself, as a warning.
///
/// # Errors
///
/// [`Error::BadDirective`] if `explicit` cannot be read, [`Error::AlreadySet`] if the process has a subscriber.
///
/// # Examples
///
/// ```no_run
/// #[tokio::main]
/// async fn main() -> Result<(), Box<dyn std::error::Error>> {
///     brokapi::logging::init(Some("info,brokapi_tbank=debug"))?;
///     // ... start the connectors
///     Ok(())
/// }
/// ```
pub fn init(explicit: Option<&str>) -> Result<(), Error> {
    let env = std::env::var(ENV).ok();
    let (directives, ignored) = choose(explicit, env.as_deref())?;
    tracing_subscriber::fmt()
        .with_env_filter(build(&directives)?)
        .with_writer(std::io::stderr)
        .with_target(true)
        .try_init()
        .map_err(|_| Error::AlreadySet)?;
    if let Some(value) = ignored {
        tracing::warn!(variable = ENV, value = %value, level = DEFAULT, "log variable ignored");
    }
    Ok(())
}

/// The directives to use and, when [`ENV`] was set but could not be read, its value.
fn choose(explicit: Option<&str>, env: Option<&str>) -> Result<(String, Option<String>), Error> {
    if let Some(given) = explicit {
        check(given)?;
        return Ok((tidy(given), None));
    }
    match env.map(str::trim).filter(|v| !v.is_empty()) {
        Some(value) if check(value).is_ok() => Ok((tidy(value), None)),
        Some(value) => Ok((DEFAULT.to_owned(), Some(value.to_owned()))),
        None => Ok((DEFAULT.to_owned(), None)),
    }
}

/// `warn, brokapi_bcs=debug` as `warn,brokapi_bcs=debug`: `EnvFilter` does not take spaces between directives.
fn tidy(directives: &str) -> String {
    directives
        .split(',')
        .map(str::trim)
        .collect::<Vec<_>>()
        .join(",")
}

/// Every directive must name a level. `EnvFilter` itself reads a bare word as a module with every level on,
/// so `BROKAPI_LOG=debgu` would quietly switch the whole log off except a module of that name.
fn check(directives: &str) -> Result<(), Error> {
    let directives = directives.trim();
    if directives.is_empty() {
        return Err(Error::BadDirective(String::new()));
    }
    for part in directives.split(',').map(str::trim) {
        // Span filters (`target[span{field}]=level`) are left to EnvFilter.
        if part.contains('[') {
            continue;
        }
        let level = match part.rsplit_once('=') {
            Some((target, level)) if !target.trim().is_empty() => level,
            Some(_) => return Err(Error::BadDirective(part.to_owned())),
            None => part,
        };
        if level.trim().parse::<LevelFilter>().is_err() {
            return Err(Error::BadDirective(part.to_owned()));
        }
    }
    Ok(())
}

fn build(directives: &str) -> Result<EnvFilter, Error> {
    EnvFilter::builder()
        .parse(directives)
        .map_err(|_| Error::BadDirective(directives.to_owned()))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn info_when_nothing_is_given() {
        assert_eq!(choose(None, None), Ok(("info".to_owned(), None)));
        assert_eq!(choose(None, Some("  ")), Ok(("info".to_owned(), None)));
    }

    #[test]
    fn the_variable_gives_a_level_or_directives() {
        assert_eq!(choose(None, Some("debug")).unwrap().0, "debug");
        assert_eq!(
            choose(None, Some("info,brokapi_bcs=debug")).unwrap().0,
            "info,brokapi_bcs=debug"
        );
        assert_eq!(choose(None, Some("WARN")).unwrap().0, "WARN");
    }

    #[test]
    fn the_argument_wins_over_the_variable() {
        assert_eq!(choose(Some("trace"), Some("error")).unwrap().0, "trace");
    }

    #[test]
    fn a_wrong_variable_falls_back_to_info_and_is_reported() {
        assert_eq!(
            choose(None, Some("debgu")),
            Ok(("info".to_owned(), Some("debgu".to_owned())))
        );
        assert_eq!(
            choose(None, Some("info,brokapi_bcs=loud")),
            Ok(("info".to_owned(), Some("info,brokapi_bcs=loud".to_owned())))
        );
    }

    #[test]
    fn a_wrong_argument_is_an_error() {
        for bad in [
            "debgu",
            "",
            "=debug",
            "brokapi_bcs",
            "info,brokapi_bcs=loud",
        ] {
            assert!(
                matches!(choose(Some(bad), None), Err(Error::BadDirective(_))),
                "{bad:?}"
            );
        }
    }

    #[test]
    fn every_accepted_value_builds_a_filter() {
        for good in [
            "info",
            "off",
            "debug",
            "info,brokapi_bcs=debug",
            "warn, brokapi_tbank::ud=trace",
            "brokapi_bcs[op]=debug",
        ] {
            let (directives, ignored) = choose(Some(good), None).unwrap();
            assert_eq!(ignored, None);
            build(&directives).unwrap();
            // the variable is read by the same rules
            assert_eq!(choose(None, Some(good)).unwrap().0, directives);
        }
        assert_eq!(
            choose(Some(" warn , brokapi_bcs=debug "), None).map(|c| c.0),
            Ok("warn,brokapi_bcs=debug".to_owned())
        );
    }
}
