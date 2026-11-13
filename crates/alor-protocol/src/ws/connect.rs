//! Opening a WebSocket without hanging on one dead address.
//!
//! `api.alor.ru` has several addresses, and one of them can swallow connections without an answer (seen on
//! 2026-11-13: `93.183.65.244`). `connect_async` takes the addresses one by one and waits for the system timeout
//! (minutes) on each, so a connect that should take 50 ms hangs. Here every address gets a short time, then the
//! next one is tried.

use std::net::SocketAddr;
use std::time::Duration;

use tokio::net::TcpStream;
use tokio::time::timeout;
use tokio_tungstenite::tungstenite::Error as WsError;
use tokio_tungstenite::tungstenite::handshake::client::Response;
use tokio_tungstenite::{MaybeTlsStream, WebSocketStream, client_async_tls};

/// How long one address gets to accept the connection before the next one is tried.
pub(crate) const PER_ADDRESS_TIMEOUT: Duration = Duration::from_secs(3);

/// The first address that accepts a TCP connection within `per_address`, or the last error.
pub(crate) async fn first_reachable(
    addrs: &[SocketAddr],
    per_address: Duration,
) -> std::io::Result<TcpStream> {
    let mut last = std::io::Error::new(std::io::ErrorKind::NotFound, "the name has no address");
    for addr in addrs {
        match timeout(per_address, TcpStream::connect(addr)).await {
            Ok(Ok(stream)) => return Ok(stream),
            Ok(Err(e)) => last = e,
            Err(_) => {
                last = std::io::Error::new(
                    std::io::ErrorKind::TimedOut,
                    format!("no answer from {addr} within {per_address:?}"),
                );
            }
        }
    }
    Err(last)
}

/// Like `connect_async`, but a dead address costs `PER_ADDRESS_TIMEOUT`, not the system timeout.
pub(crate) async fn connect(
    url: &str,
) -> Result<(WebSocketStream<MaybeTlsStream<TcpStream>>, Response), Box<WsError>> {
    let parsed = url::Url::parse(url).map_err(|e| {
        Box::new(WsError::Url(
            tokio_tungstenite::tungstenite::error::UrlError::UnableToConnect(e.to_string()),
        ))
    })?;
    let host = parsed.host_str().unwrap_or_default().to_string();
    let port = parsed.port_or_known_default().unwrap_or(443);
    let addrs: Vec<SocketAddr> = tokio::net::lookup_host((host.as_str(), port))
        .await
        .map_err(|e| Box::new(WsError::Io(e)))?
        .collect();
    let tcp = first_reachable(&addrs, PER_ADDRESS_TIMEOUT)
        .await
        .map_err(|e| Box::new(WsError::Io(e)))?;
    client_async_tls(url, tcp).await.map_err(Box::new)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn an_address_that_does_not_answer_does_not_stop_the_next_one() {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let good = listener.local_addr().unwrap();
        // 192.0.2.1 (TEST-NET-1) never answers: the packets are dropped or the network is unreachable.
        let dead: SocketAddr = "192.0.2.1:9".parse().unwrap();
        let stream = first_reachable(&[dead, good], Duration::from_millis(300)).await;
        assert!(stream.is_ok(), "{stream:?}");
    }

    #[tokio::test]
    async fn without_a_reachable_address_the_error_comes_back() {
        let dead: SocketAddr = "192.0.2.1:9".parse().unwrap();
        let err = first_reachable(&[dead], Duration::from_millis(200))
            .await
            .unwrap_err();
        assert!(!err.to_string().is_empty());
        assert!(
            first_reachable(&[], Duration::from_millis(200))
                .await
                .is_err()
        );
    }
}
