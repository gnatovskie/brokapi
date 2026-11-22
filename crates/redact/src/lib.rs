//! What must not reach a log or a file, in one place for every brokapi crate.
//!
//! | Need | Function |
//! |---|---|
//! | Show an account number in a log or a `Debug` output | [`mask_account`] |
//! | Clean a parsed answer of a broker: tokens, account and order numbers, personal data | [`scrub`] |
//! | Clean the text of a request or an answer before it is logged at `trace` | [`scrub_body`] |
//! | Cut the known secrets of a run out of a text | [`remove_secrets`] |
//!
//! The connectors use it for their logs, the recorder of live runs in `brokapi-mocks` for the reference
//! files. One best-effort set of rules for both: because scrubbing matches known field names and secret
//! patterns, always review `trace` logs or recorded fixtures before sharing them.
//!
//! The book is at <https://brokapi.ru/docs/>.
//!
//! # Examples
//!
//! ```
//! use brokapi_redact::{mask_account, scrub_body};
//!
//! assert_eq!(mask_account("2000123456"), "****3456");
//!
//! let body = r#"{"accountId":"2000123456","ticker":"SBER","price":"244.30"}"#;
//! let clean = scrub_body(body);
//! assert!(!clean.contains("2000123456"));
//! assert!(clean.contains("SBER"));
//! ```

#![warn(missing_docs)]

mod scrub;

pub use scrub::{REDACTED, remove_secrets, scrub};

/// How many characters of an account number [`mask_account`] keeps.
const ACCOUNT_TAIL: usize = 4;

/// An account number as it may appear in a log: `****` and the last 4 characters.
///
/// A number of 4 characters or shorter is hidden as a whole, otherwise the mask would show all of it. An
/// empty string stays empty, so a missing account still reads as missing.
///
/// # Examples
///
/// ```
/// use brokapi_redact::mask_account;
///
/// assert_eq!(mask_account("2000123456"), "****3456");
/// assert_eq!(mask_account("D12345"), "****2345");
/// assert_eq!(mask_account("1234"), "****");
/// assert_eq!(mask_account(""), "");
/// ```
pub fn mask_account(account: &str) -> String {
    let count = account.chars().count();
    if count == 0 {
        return String::new();
    }
    if count <= ACCOUNT_TAIL {
        return "****".to_owned();
    }
    let tail: String = account.chars().skip(count - ACCOUNT_TAIL).collect();
    format!("****{tail}")
}

/// The text of a request or an answer, cleaned for a log.
///
/// JSON is cleaned by [`scrub`] and written back in one line. A text that is not JSON (a form, a page of an
/// error, a broken frame) cannot be cleaned field by field, so only its length is left:
/// `<redacted: 120 bytes>`.
///
/// # Examples
///
/// ```
/// use brokapi_redact::scrub_body;
///
/// let clean = scrub_body(r#"{"access_token":"eyJhbGciOi.eyJzdWIi.c2ln","expires_in":86400}"#);
/// assert_eq!(clean, r#"{"access_token":"<redacted>","expires_in":86400}"#);
///
/// assert_eq!(scrub_body("grant_type=refresh_token&refresh_token=abc"), "<redacted: 42 bytes>");
/// ```
pub fn scrub_body(body: &str) -> String {
    match serde_json::from_str::<serde_json::Value>(body) {
        Ok(mut value) => {
            scrub(&mut value);
            value.to_string()
        }
        Err(_) => format!("<redacted: {} bytes>", body.len()),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn the_mask_counts_characters_not_bytes() {
        assert_eq!(mask_account("счет-77881234"), "****1234");
        assert_eq!(mask_account("ИИС12345"), "****2345");
        assert_eq!(mask_account("ИИС1"), "****");
    }

    #[test]
    fn a_body_with_a_secret_of_every_kind_keeps_none() {
        let body = r#"{"token":"t.AbCdEf0123456789SecretTokenOfTBank","owner":{"email":"ivan@example.com"},
            "orders":[{"orderId":"20261019-TQBR-79000001","qty":2}]}"#;
        let clean = scrub_body(body);
        for secret in [
            "t.AbCdEf0123456789SecretTokenOfTBank",
            "ivan@example.com",
            "20261019-TQBR-79000001",
        ] {
            assert!(!clean.contains(secret), "{secret} left in {clean}");
        }
        assert!(clean.contains(r#""qty":2"#));
    }

    #[test]
    fn a_text_that_is_not_json_is_not_logged() {
        let clean = scrub_body("<html>token t.AbCdEf0123456789SecretTokenOfTBank</html>");
        assert!(!clean.contains("t.AbCd"));
        assert!(clean.starts_with("<redacted: "));
    }
}
