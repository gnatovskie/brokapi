//! The authorized HTTP client: token handling, the 401 repeat of reads and the mapping of error replies.
//!
//! The generic methods are here ([`AlorHttpClient::read`], [`AlorHttpClient::command`]). The endpoint methods
//! (`get_order_book`, `create_limit_order`, ...) are in [`super::endpoints`].

use std::future::Future;
use std::sync::Arc;
use std::time::{Duration, Instant};

use base64::Engine as _;
pub use reqwest::Method;
use reqwest::{Client, StatusCode};
use serde_json::Value;
use tokio::sync::{Mutex, RwLock};
use tracing::{debug, warn};

use crate::config::AlorConfig;
use crate::error::{AlorApiError, ApiErrorBody};

/// How long an access token is used before a new one is asked for. The broker recommends a new token every
/// 20-25 minutes of the 30 that a token lives.
pub const ACCESS_TOKEN_REUSE: Duration = Duration::from_secs(20 * 60);

/// The access token and the moment after which the client does not trust it any more.
#[derive(Clone)]
struct CachedToken {
    token: String,
    valid_until: tokio::time::Instant,
}

struct Shared {
    access: RwLock<Option<CachedToken>>,
    refresh_token: String,
    refresh_mutex: Mutex<()>,
}

/// The authorized HTTP client of the ALOR OpenAPI.
///
/// It holds the refresh token and the current access token and adds `Authorization: Bearer ...` to every call:
///
/// * The first call trades the refresh token for an access token (`POST /refresh` of the authorization
///   server). The access token is cached for 20 minutes (or less, if its own expiry is nearer).
/// * When the broker answers 401 to a **read**, the client gets a new access token and repeats the **same
///   request once**.
/// * A **command** (placing, changing or cancelling an order) is never repeated, not even after a 401: the
///   client renews the token for the next call and returns the 401 as an error.
/// * A transport error or a timeout is never repeated.
///
/// The client is cheap to clone. Clones share the tokens and the connection pool. The endpoint methods (one per
/// REST call) are documented on this type too, they are in [`super::endpoints`]. The broker limits HTTP calls to
/// 100 a second per user; the client does not throttle, the connector does it through the shared limiter.
///
/// # Examples
///
/// ```
/// # #[tokio::main(flavor = "current_thread")]
/// # async fn main() -> Result<(), Box<dyn std::error::Error>> {
/// # let server = brokapi_mocks::alor::MockServer::start(Default::default()).await;
/// # let cfg = brokapi_alor_protocol::config::AlorConfig::for_mock("mock-refresh-token", &server.http_base);
/// use brokapi_alor_protocol::http::client::AlorHttpClient;
///
/// let client = AlorHttpClient::new(cfg)?;
/// let book = client.get_order_book("MOEX", "SBER", Some("TQBR"), Some(5)).await?;
/// println!("{} levels of bids", book.bids.len());
/// # Ok(())
/// # }
/// ```
#[derive(Clone)]
pub struct AlorHttpClient {
    client: Client,
    shared: Arc<Shared>,
    http_base_url: Arc<str>,
    auth_base_url: Arc<str>,
    ws_base_url: Arc<str>,
}

impl AlorHttpClient {
    /// Creates a client from a configuration. No network call is made.
    ///
    /// # Errors
    ///
    /// [`AlorApiError::Http`] (no status) if the HTTP stack cannot be built.
    ///
    /// # Examples
    ///
    /// ```
    /// use brokapi_alor_protocol::config::AlorConfig;
    /// use brokapi_alor_protocol::http::client::AlorHttpClient;
    ///
    /// let client = AlorHttpClient::new(AlorConfig::new("refresh-token".to_string()))?;
    /// assert_eq!(client.base_url(), "https://api.alor.ru");
    /// # Ok::<(), brokapi_alor_protocol::error::AlorApiError>(())
    /// ```
    pub fn new(config: AlorConfig) -> Result<Self, AlorApiError> {
        let client = Client::builder()
            .timeout(config.request_timeout)
            // One dead address of the name must not eat the whole request time: with several addresses the
            // connect time is shared out between them, so the next address is tried after a few seconds.
            .connect_timeout(Duration::from_secs(6))
            .gzip(true)
            .build()?;
        Ok(Self {
            client,
            shared: Arc::new(Shared {
                access: RwLock::new(None),
                refresh_token: config.refresh_token,
                refresh_mutex: Mutex::new(()),
            }),
            http_base_url: Arc::from(config.http_base_url.as_str()),
            auth_base_url: Arc::from(config.auth_base_url.as_str()),
            ws_base_url: Arc::from(config.ws_base_url.as_str()),
        })
    }

    /// The access token now: the cached one, or a new one if there is none or it is past its time.
    ///
    /// The WebSocket session calls this before every message that needs a token.
    ///
    /// # Errors
    ///
    /// As [`AlorHttpClient::refresh_access_token`], and only when a new token is needed.
    ///
    /// # Examples
    ///
    /// ```
    /// # #[tokio::main(flavor = "current_thread")]
    /// # async fn main() -> Result<(), Box<dyn std::error::Error>> {
    /// # let server = brokapi_mocks::alor::MockServer::start(Default::default()).await;
    /// # let cfg = brokapi_alor_protocol::config::AlorConfig::for_mock("mock-refresh-token", &server.http_base);
    /// use brokapi_alor_protocol::http::client::AlorHttpClient;
    ///
    /// let client = AlorHttpClient::new(cfg)?;
    /// let first = client.get_access_token().await?;
    /// let second = client.get_access_token().await?; // from the cache, no second token request
    /// assert_eq!(first, second);
    /// # Ok(())
    /// # }
    /// ```
    pub async fn get_access_token(&self) -> Result<String, AlorApiError> {
        {
            let cached = self.shared.access.read().await;
            if let Some(c) = cached.as_ref()
                && tokio::time::Instant::now() < c.valid_until
            {
                return Ok(c.token.clone());
            }
        }
        self.refresh_access_token().await
    }

    /// Trades the refresh token for a new access token and stores it. Always calls the authorization server.
    ///
    /// Sends `POST /refresh` with the refresh token in a JSON body (not in the URL, so it stays out of logs).
    /// The new access token is cached. Refreshes are serialised by a lock.
    ///
    /// # Errors
    ///
    /// * [`AlorApiError::Auth`]: the server refused the refresh token (HTTP 400, 401 or 403).
    /// * [`AlorApiError::Http`]: a transport failure, or another status.
    /// * [`AlorApiError::Parse`]: the answer has no `AccessToken`.
    pub async fn refresh_access_token(&self) -> Result<String, AlorApiError> {
        let _guard = self.shared.refresh_mutex.lock().await;
        let url = format!("{}/refresh", self.auth_base_url);
        debug!("access token renewal started");
        let resp = self
            .client
            .post(&url)
            .json(&serde_json::json!({ "token": self.shared.refresh_token }))
            .send_logged()
            .await?;
        let status = resp.status();
        let body = resp.text().await?;
        if !status.is_success() {
            let message = error_text(&body);
            return Err(if matches!(status.as_u16(), 400 | 401 | 403) {
                AlorApiError::Auth { message }
            } else {
                AlorApiError::Http {
                    status: Some(status.as_u16()),
                    message,
                }
            });
        }
        let token = serde_json::from_str::<Value>(&body)?
            .get("AccessToken")
            .and_then(Value::as_str)
            .map(str::to_string)
            .ok_or_else(|| {
                AlorApiError::Parse("the answer of /refresh has no AccessToken".into())
            })?;
        let lifetime = token_lifetime(&token, unix_now());
        *self.shared.access.write().await = Some(CachedToken {
            token: token.clone(),
            valid_until: tokio::time::Instant::now() + lifetime,
        });
        Ok(token)
    }

    /// The portfolios named in the claims of the current access token (the `portfolios` claim of the JWT).
    ///
    /// The connector uses it to find the portfolio when the config names none. Empty when the token is not a
    /// JWT or carries no portfolios.
    ///
    /// # Errors
    ///
    /// As [`AlorHttpClient::get_access_token`].
    pub async fn token_portfolios(&self) -> Result<Vec<String>, AlorApiError> {
        let token = self.get_access_token().await?;
        Ok(jwt_claims(&token)
            .and_then(|c| {
                c.get("portfolios")
                    .and_then(Value::as_str)
                    .map(str::to_string)
            })
            .map(|s| s.split_whitespace().map(str::to_string).collect())
            .unwrap_or_default())
    }

    /// `GET`s a path with query parameters and parses the JSON reply into `T`.
    ///
    /// `path` starts with `/` and is added to the base URL. A 401 is answered with one token refresh and one
    /// repeat. Other failures are not retried. The typed methods of [`super::endpoints`] call it.
    ///
    /// # Errors
    ///
    /// * [`AlorApiError::NotFound`]: HTTP 404. [`AlorApiError::RateLimitExceeded`]: HTTP 429.
    /// * [`AlorApiError::Http`]: any other non-success status, or a transport failure.
    /// * [`AlorApiError::Parse`]: the reply is a success but is not the JSON that `T` expects.
    /// * [`AlorApiError::Auth`]: the token refresh after a 401 failed.
    ///
    /// # Examples
    ///
    /// ```
    /// # #[tokio::main(flavor = "current_thread")]
    /// # async fn main() -> Result<(), Box<dyn std::error::Error>> {
    /// # let server = brokapi_mocks::alor::MockServer::start(Default::default()).await;
    /// # let cfg = brokapi_alor_protocol::config::AlorConfig::for_mock("mock-refresh-token", &server.http_base);
    /// use brokapi_alor_protocol::error::AlorApiError;
    /// use brokapi_alor_protocol::http::client::AlorHttpClient;
    ///
    /// let client = AlorHttpClient::new(cfg)?;
    /// let time: serde_json::Value = client.read("/md/v2/time", &[]).await?;
    /// assert!(time.is_number());
    ///
    /// // A path the broker does not know ends in an error that carries the status.
    /// let err = client.read::<serde_json::Value>("/no/such/path", &[]).await.unwrap_err();
    /// assert!(matches!(err, AlorApiError::NotFound { .. }));
    /// # Ok(())
    /// # }
    /// ```
    pub async fn read<T: serde::de::DeserializeOwned>(
        &self,
        path: &str,
        query: &[(&str, String)],
    ) -> Result<T, AlorApiError> {
        let url = format!("{}{}", self.http_base_url, path);
        let token = self.get_access_token().await?;
        let resp = self
            .send(Method::GET, &url, query, None::<&()>, &[], &token)
            .await?;
        if resp.status() == StatusCode::UNAUTHORIZED {
            warn!(
                status = 401,
                "token refused, renewing it and repeating the read"
            );
            let new_token = self.refresh_access_token().await?;
            let resp = self
                .send(Method::GET, &url, query, None::<&()>, &[], &new_token)
                .await?;
            return self.handle(resp).await;
        }
        self.handle(resp).await
    }

    /// Sends a command (place, change or cancel an order) and parses the JSON reply into `T`. **Never repeats
    /// the request.**
    ///
    /// A repeat of a command is the one way to place an order twice, so the client sends it once. If the
    /// broker answers 401, the client renews the access token (so the next call works) and returns the 401 as
    /// [`AlorApiError::Http`] with `status: Some(401)`; a 401 means the broker did not act. A transport error
    /// or a timeout is an unknown outcome, as for every command. `req_id` becomes the header `X-REQID`: the
    /// broker answers a repeated id with a copy of its first answer instead of doing the command again.
    ///
    /// # Errors
    ///
    /// The same as [`AlorHttpClient::read`].
    pub async fn command<T, B>(
        &self,
        method: Method,
        path: &str,
        query: &[(&str, String)],
        body: Option<&B>,
        req_id: Option<&str>,
    ) -> Result<T, AlorApiError>
    where
        T: serde::de::DeserializeOwned,
        B: serde::Serialize,
    {
        let url = format!("{}{}", self.http_base_url, path);
        let token = self.get_access_token().await?;
        let headers: Vec<(&str, &str)> = req_id.map(|id| vec![("X-REQID", id)]).unwrap_or_default();
        let resp = self
            .send(method, &url, query, body, &headers, &token)
            .await?;
        if resp.status() == StatusCode::UNAUTHORIZED {
            warn!(
                status = 401,
                "token refused on a command, renewing it: the command is not repeated"
            );
            // The next call needs a fresh token. A failure of the renewal does not hide the 401.
            let _ = self.refresh_access_token().await;
        }
        self.handle(resp).await
    }

    /// `GET`s a path and returns the status and the body text, without parsing and without mapping errors.
    ///
    /// An error status is **not** an `Err` here: you get it as the first element of the pair. Only a 401 is
    /// handled (one refresh, one repeat).
    ///
    /// # Errors
    ///
    /// [`AlorApiError::Http`] (no status) on a transport failure, and the errors of
    /// [`AlorHttpClient::refresh_access_token`] when a 401 needs a new token.
    pub async fn read_raw(
        &self,
        path: &str,
        query: &[(&str, String)],
    ) -> Result<(StatusCode, String), AlorApiError> {
        let url = format!("{}{}", self.http_base_url, path);
        let token = self.get_access_token().await?;
        let mut resp = self
            .send(Method::GET, &url, query, None::<&()>, &[], &token)
            .await?;
        if resp.status() == StatusCode::UNAUTHORIZED {
            let new_token = self.refresh_access_token().await?;
            resp = self
                .send(Method::GET, &url, query, None::<&()>, &[], &new_token)
                .await?;
        }
        let status = resp.status();
        Ok((status, resp.text().await?))
    }

    async fn send<B: serde::Serialize>(
        &self,
        method: Method,
        url: &str,
        query: &[(&str, String)],
        body: Option<&B>,
        headers: &[(&str, &str)],
        token: &str,
    ) -> Result<reqwest::Response, AlorApiError> {
        let mut req = self.client.request(method, url).bearer_auth(token);
        if !query.is_empty() {
            req = req.query(query);
        }
        for (k, v) in headers {
            req = req.header(*k, *v);
        }
        if let Some(b) = body {
            req = req.json(b);
        }
        Ok(req.send_logged().await?)
    }

    /// Turns a reply into `T` or into the matching [`AlorApiError`].
    async fn handle<T: serde::de::DeserializeOwned>(
        &self,
        resp: reqwest::Response,
    ) -> Result<T, AlorApiError> {
        let status = resp.status();
        let body = resp.text().await?;
        if status.is_success() {
            // An empty success body (some deletes) reads as JSON null.
            let text = if body.trim().is_empty() {
                "null"
            } else {
                body.as_str()
            };
            return serde_json::from_str(text).map_err(|e| {
                AlorApiError::Parse(format!(
                    "Failed to parse response: {e} | body: {}",
                    truncate_utf8(&body, 200)
                ))
            });
        }
        let code = status.as_u16();
        Err(match code {
            404 => AlorApiError::NotFound {
                message: error_text(&body),
            },
            429 => AlorApiError::RateLimitExceeded,
            _ => AlorApiError::Http {
                status: Some(code),
                message: error_text(&body),
            },
        })
    }

    /// The underlying `reqwest` client, for calls this crate does not wrap. It carries **no** authorization:
    /// add the token yourself with [`AlorHttpClient::get_access_token`].
    pub fn inner_client(&self) -> &Client {
        &self.client
    }

    /// The REST base URL this client calls, without a trailing slash.
    pub fn base_url(&self) -> &str {
        &self.http_base_url
    }

    /// The WebSocket base URL that sessions started from this client connect to.
    pub fn ws_base_url(&self) -> &str {
        &self.ws_base_url
    }
}

/// The text of an error body: its `message`, else the whole body.
fn error_text(body: &str) -> String {
    match serde_json::from_str::<ApiErrorBody>(body) {
        Ok(ApiErrorBody {
            message: Some(m),
            code,
        }) => match code {
            Some(c) if !c.is_empty() => format!("{c}: {m}"),
            _ => m,
        },
        _ => body.to_string(),
    }
}

fn unix_now() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_secs())
        .unwrap_or(0)
}

/// The claims of a JWT (the middle part), if `token` is one.
pub fn jwt_claims(token: &str) -> Option<Value> {
    let payload = token.split('.').nth(1)?;
    let bytes = base64::engine::general_purpose::URL_SAFE_NO_PAD
        .decode(payload.trim_end_matches('='))
        .ok()?;
    serde_json::from_slice(&bytes).ok()
}

/// How long to use an access token: [`ACCESS_TOKEN_REUSE`], or 80% of what is left of its own life if that
/// is shorter. A token that is not a JWT gets the 20 minutes.
pub fn token_lifetime(token: &str, now_unix: u64) -> Duration {
    let exp = jwt_claims(token).and_then(|c| c.get("exp").and_then(Value::as_u64));
    match exp {
        Some(exp) => {
            let left = Duration::from_secs(exp.saturating_sub(now_unix));
            left.mul_f64(0.8).min(ACCESS_TOKEN_REUSE)
        }
        None => ACCESS_TOKEN_REUSE,
    }
}

/// The first `max` bytes of `s`, cut back to a character boundary.
fn truncate_utf8(s: &str, max: usize) -> &str {
    if s.len() <= max {
        return s;
    }
    let mut end = max;
    while !s.is_char_boundary(end) {
        end -= 1;
    }
    &s[..end]
}

/// The path of a request as the log shows it: the portfolio or the login after `Clients` as a mask,
/// `/md/v2/Clients/MOEX/D00001/orders` as `/md/v2/Clients/MOEX/****0001/orders`.
fn masked_path(path: &str) -> String {
    let mut parts: Vec<String> = path.split('/').map(str::to_owned).collect();
    if let Some(at) = parts.iter().position(|p| p.eq_ignore_ascii_case("clients")) {
        let exchange = parts
            .get(at + 1)
            .is_some_and(|p| !p.is_empty() && p.chars().all(|c| c.is_ascii_uppercase()))
            && parts.len() > at + 3;
        let account = if exchange { at + 2 } else { at + 1 };
        if let Some(part) = parts.get_mut(account) {
            *part = brokapi_redact::mask_account(part);
        }
    }
    parts.join("/")
}

/// Sends a request and writes it to the log on `debug`: the method and the path (never the query or the body: a read of
/// orders holds account data), the HTTP status and the time.
trait SendLogged {
    fn send_logged(self) -> impl Future<Output = reqwest::Result<reqwest::Response>> + Send;
}

impl SendLogged for reqwest::RequestBuilder {
    fn send_logged(self) -> impl Future<Output = reqwest::Result<reqwest::Response>> + Send {
        let (client, request) = self.build_split();
        async move {
            let request = request?;
            let method = format!("{} {}", request.method(), masked_path(request.url().path()));
            let started = Instant::now();
            let result = client.execute(request).await;
            let latency_ms = started.elapsed().as_millis() as u64;
            match &result {
                Ok(resp) => {
                    debug!(%method, status = resp.status().as_u16(), latency_ms, "request done")
                }
                Err(e) => debug!(%method, latency_ms, error = %e, "request failed"),
            }
            result
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn jwt(claims: &str) -> String {
        let enc = |s: &str| base64::engine::general_purpose::URL_SAFE_NO_PAD.encode(s);
        format!("{}.{}.sig", enc("{}"), enc(claims))
    }

    #[test]
    fn a_token_is_used_for_twenty_minutes_or_less() {
        assert_eq!(token_lifetime("not-a-jwt", 0), ACCESS_TOKEN_REUSE);
        let long = jwt(r#"{"exp": 100000}"#);
        assert_eq!(token_lifetime(&long, 0), ACCESS_TOKEN_REUSE);
        // 10 minutes left: 80% of it.
        let short = jwt(r#"{"exp": 600}"#);
        assert_eq!(token_lifetime(&short, 0), Duration::from_secs(480));
        // Already past its time.
        assert_eq!(token_lifetime(&short, 1000), Duration::ZERO);
    }

    #[test]
    fn claims_are_read_from_a_jwt() {
        let t = jwt(r#"{"portfolios":"D1 7500A","exp":5}"#);
        let c = jwt_claims(&t).unwrap();
        assert_eq!(c["portfolios"], "D1 7500A");
        assert!(jwt_claims("nope").is_none());
    }

    #[test]
    fn the_error_text_prefers_the_message_of_the_body() {
        assert_eq!(error_text(r#"{"message":"bad","code":"X"}"#), "X: bad");
        assert_eq!(error_text(r#"{"message":"bad"}"#), "bad");
        assert_eq!(error_text("plain"), "plain");
    }

    #[test]
    fn truncation_never_splits_a_character() {
        let s = "ж".repeat(150);
        let cut = truncate_utf8(&s, 199);
        assert_eq!(cut.len(), 198);
    }
}

#[cfg(test)]
mod log_tests {
    use super::masked_path;

    #[test]
    fn the_portfolio_and_the_login_in_a_path_are_masked() {
        assert_eq!(
            masked_path("/md/v2/Clients/MOEX/D00001/orders"),
            "/md/v2/Clients/MOEX/****0001/orders"
        );
        assert_eq!(
            masked_path("/md/v2/Clients/P012345/positions"),
            "/md/v2/Clients/****2345/positions"
        );
        assert_eq!(
            masked_path("/md/v2/Securities/MOEX/SBER"),
            "/md/v2/Securities/MOEX/SBER"
        );
    }
}
