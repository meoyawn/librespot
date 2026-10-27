use std::{
    fmt,
    future::Future,
    pin::Pin,
    sync::Arc,
    task::{Context, Poll},
    time::{Duration, Instant},
};

use bytes::Bytes;
use futures_util::{FutureExt, future::IntoStream};
use governor::{
    Quota, RateLimiter, clock::MonotonicClock, middleware::NoOpMiddleware,
    state::keyed::DefaultKeyedStateStore,
};
use http::header::HeaderValue;
use http_body_util::{BodyExt, combinators::UnsyncBoxBody};
use hyper::{HeaderMap, Request, Response, StatusCode, header::USER_AGENT};
use nonzero_ext::nonzero;
use thiserror::Error;

use crate::{Error, config::SessionConfig, date::Date};

// The 30 seconds interval is documented by Spotify, but the calls per interval
// is a guesstimate and probably subject to licensing (purchasing extra calls)
// and may change at any time.
pub const RATE_LIMIT_INTERVAL: Duration = Duration::from_secs(30);
pub const RATE_LIMIT_MAX_WAIT: Duration = Duration::from_secs(10);
pub const RATE_LIMIT_CALLS_PER_INTERVAL: u32 = 300;

#[derive(Debug, Error)]
pub enum HttpClientError {
    #[error("Response status code: {0}")]
    StatusCode(hyper::StatusCode),
}

impl From<HttpClientError> for Error {
    fn from(err: HttpClientError) -> Self {
        match err {
            HttpClientError::StatusCode(code) => {
                // not exhaustive, but what reasonably could be expected
                match code {
                    StatusCode::GATEWAY_TIMEOUT | StatusCode::REQUEST_TIMEOUT => {
                        Error::deadline_exceeded(err)
                    }
                    StatusCode::GONE
                    | StatusCode::NOT_FOUND
                    | StatusCode::MOVED_PERMANENTLY
                    | StatusCode::PERMANENT_REDIRECT
                    | StatusCode::TEMPORARY_REDIRECT => Error::not_found(err),
                    StatusCode::FORBIDDEN | StatusCode::PAYMENT_REQUIRED => {
                        Error::permission_denied(err)
                    }
                    StatusCode::NETWORK_AUTHENTICATION_REQUIRED
                    | StatusCode::PROXY_AUTHENTICATION_REQUIRED
                    | StatusCode::UNAUTHORIZED => Error::unauthenticated(err),
                    StatusCode::EXPECTATION_FAILED
                    | StatusCode::PRECONDITION_FAILED
                    | StatusCode::PRECONDITION_REQUIRED => Error::failed_precondition(err),
                    StatusCode::RANGE_NOT_SATISFIABLE => Error::out_of_range(err),
                    StatusCode::INTERNAL_SERVER_ERROR
                    | StatusCode::MISDIRECTED_REQUEST
                    | StatusCode::SERVICE_UNAVAILABLE
                    | StatusCode::UNAVAILABLE_FOR_LEGAL_REASONS => Error::unavailable(err),
                    StatusCode::BAD_REQUEST
                    | StatusCode::HTTP_VERSION_NOT_SUPPORTED
                    | StatusCode::LENGTH_REQUIRED
                    | StatusCode::METHOD_NOT_ALLOWED
                    | StatusCode::NOT_ACCEPTABLE
                    | StatusCode::PAYLOAD_TOO_LARGE
                    | StatusCode::REQUEST_HEADER_FIELDS_TOO_LARGE
                    | StatusCode::UNSUPPORTED_MEDIA_TYPE
                    | StatusCode::URI_TOO_LONG => Error::invalid_argument(err),
                    StatusCode::TOO_MANY_REQUESTS => Error::resource_exhausted(err),
                    StatusCode::NOT_IMPLEMENTED => Error::unimplemented(err),
                    _ => Error::unknown(err),
                }
            }
        }
    }
}

/// Streaming HTTP response supplied by the embedding application's transport.
/// Body frames and failures propagate without collecting the entire audio file.
pub type HttpResponseBody = UnsyncBoxBody<Bytes, Error>;

pub struct HttpRequestFuture(
    Pin<Box<dyn Future<Output = Result<Response<HttpResponseBody>, Error>> + Send>>,
);

impl HttpRequestFuture {
    pub fn new(
        future: impl Future<Output = Result<Response<HttpResponseBody>, Error>> + Send + 'static,
    ) -> Self {
        Self(Box::pin(future))
    }
}

impl fmt::Debug for HttpRequestFuture {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("HttpRequestFuture")
    }
}

impl Future for HttpRequestFuture {
    type Output = Result<Response<HttpResponseBody>, Error>;

    fn poll(mut self: Pin<&mut Self>, context: &mut Context<'_>) -> Poll<Self::Output> {
        self.0.as_mut().poll(context)
    }
}

/// A production callback boundary for Dart's existing HTTP implementation.
/// The adapter owns the wire transport, proxy/TLS policy and cancellation of its stream.
/// This structured request does not assert byte-identical native wire formatting.
#[derive(Clone)]
pub struct HttpTransport(Arc<dyn Fn(Request<Bytes>) -> HttpRequestFuture + Send + Sync>);

impl HttpTransport {
    pub fn new<F, Fut>(request: F) -> Self
    where
        F: Fn(Request<Bytes>) -> Fut + Send + Sync + 'static,
        Fut: Future<Output = Result<Response<HttpResponseBody>, Error>> + Send + 'static,
    {
        Self(Arc::new(move |input| {
            HttpRequestFuture::new(request(input))
        }))
    }
}

impl fmt::Debug for HttpTransport {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("HttpTransport")
    }
}

pub struct HttpClient {
    config: SessionConfig,

    rate_limiter:
        RateLimiter<String, DefaultKeyedStateStore<String>, MonotonicClock, NoOpMiddleware>,
}

impl HttpClient {
    pub fn new(config: &SessionConfig) -> Self {
        let replenish_interval_ns =
            RATE_LIMIT_INTERVAL.as_nanos() / RATE_LIMIT_CALLS_PER_INTERVAL as u128;
        let quota = Quota::with_period(Duration::from_nanos(replenish_interval_ns as u64))
            .expect("replenish interval should be valid")
            .allow_burst(nonzero![RATE_LIMIT_CALLS_PER_INTERVAL]);
        let rate_limiter = RateLimiter::keyed(quota);

        Self {
            config: config.clone(),
            rate_limiter,
        }
    }

    pub async fn request(&self, req: Request<Bytes>) -> Result<Response<HttpResponseBody>, Error> {
        debug!("Requesting {}", req.uri());

        // `Request` does not implement `Clone` because its `Body` may be a single-shot stream.
        // As correct as that may be technically, we now need all this boilerplate to clone it
        // ourselves, as any `Request` is moved in the loop.
        let (parts, body_as_bytes) = req.into_parts();

        loop {
            let mut req = Request::builder()
                .method(parts.method.clone())
                .uri(parts.uri.clone())
                .version(parts.version)
                .body(body_as_bytes.clone())?;
            *req.headers_mut() = parts.headers.clone();

            let request = self.request_fut(req)?;
            let response = request.await;

            if let Ok(response) = &response {
                let code = response.status();

                if code == StatusCode::TOO_MANY_REQUESTS {
                    if let Some(duration) = Self::get_retry_after(response.headers()) {
                        warn!(
                            "Rate limited by service, retrying in {} seconds...",
                            duration.as_secs()
                        );
                        tokio::time::sleep(duration).await;
                        continue;
                    }
                }

                if !code.is_success() {
                    return Err(HttpClientError::StatusCode(code).into());
                }
            }

            let response = response?;
            return Ok(response);
        }
    }

    pub async fn request_body(&self, req: Request<Bytes>) -> Result<Bytes, Error> {
        let response = self.request(req).await?;
        Ok(response.into_body().collect().await?.to_bytes())
    }

    pub fn request_stream(
        &self,
        req: Request<Bytes>,
    ) -> Result<IntoStream<HttpRequestFuture>, Error> {
        Ok(self.request_fut(req)?.into_stream())
    }

    pub fn request_fut(&self, mut req: Request<Bytes>) -> Result<HttpRequestFuture, Error> {
        let user_agent = self.config.network_user_agent()?;
        // Do not silently overwrite a caller's identity or generate a librespot fallback.
        if req
            .headers()
            .get_all(USER_AGENT)
            .iter()
            .any(|value| value != user_agent)
        {
            return Err(Error::failed_precondition(
                "HTTP User-Agent does not match configured native identity",
            ));
        }
        if !req.headers().contains_key(USER_AGENT) {
            req.headers_mut().insert(USER_AGENT, user_agent);
        }

        // For rate limiting we cannot *just* depend on Spotify sending us HTTP/429
        // Retry-After headers. For example, when there is a service interruption
        // and HTTP/500 is returned, we don't want to DoS the Spotify infrastructure.
        let domain = match req.uri().host() {
            Some(host) => {
                // strip the prefix from *.domain.tld (assume rate limit is per domain, not subdomain)
                let mut parts = host.split('.').map(Into::into).collect::<Vec<String>>();
                let n = parts.len().saturating_sub(2);
                parts.drain(n..).collect()
            }
            None => String::from(""),
        };
        self.rate_limiter.check_key(&domain).map_err(|e| {
            Error::resource_exhausted(format!(
                "rate limited for at least another {} seconds",
                e.wait_time_from(Instant::now()).as_secs()
            ))
        })?;

        let transport =
            self.config.http_transport.as_ref().ok_or_else(|| {
                Error::failed_precondition("embedding HTTP transport is required")
            })?;
        Ok((transport.0)(req))
    }

    pub fn get_retry_after(headers: &HeaderMap<HeaderValue>) -> Option<Duration> {
        let now = Date::now_utc().as_timestamp_ms();

        let mut retry_after_ms = None;
        if let Some(header_val) = headers.get("X-RateLimit-Next") {
            // *.akamaized.net (Akamai)
            if let Ok(date_str) = header_val.to_str() {
                if let Ok(target) = Date::from_iso8601(date_str) {
                    retry_after_ms = Some(target.as_timestamp_ms().saturating_sub(now))
                }
            }
        } else if let Some(header_val) = headers.get("Fastly-RateLimit-Reset") {
            // *.scdn.co (Fastly)
            if let Ok(timestamp) = header_val.to_str() {
                if let Ok(target) = timestamp.parse::<i64>() {
                    retry_after_ms = Some(target.saturating_sub(now))
                }
            }
        } else if let Some(header_val) = headers.get("Retry-After") {
            // Generic RFC compliant (including *.spotify.com)
            if let Ok(retry_after) = header_val.to_str() {
                if let Ok(duration) = retry_after.parse::<i64>() {
                    retry_after_ms = Some(duration * 1000)
                }
            }
        }

        if let Some(retry_after) = retry_after_ms {
            let duration = Duration::from_millis(retry_after as u64);
            if duration <= RATE_LIMIT_MAX_WAIT {
                return Some(duration);
            } else {
                debug!(
                    "Waiting {} seconds would exceed {} second limit",
                    duration.as_secs(),
                    RATE_LIMIT_MAX_WAIT.as_secs()
                );
            }
        }

        None
    }
}
