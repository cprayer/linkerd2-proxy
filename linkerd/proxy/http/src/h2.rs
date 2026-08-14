use crate::{Body, TokioExecutor};
use futures::prelude::*;
use linkerd_error::{cause_ref, Error, Result};
use linkerd_stack::{MakeConnection, Service};
use std::{
    marker::PhantomData,
    pin::Pin,
    sync::{
        atomic::{AtomicBool, Ordering},
        Arc,
    },
    task::{Context, Poll},
};
use tracing::instrument::Instrument;
use tracing::{debug, debug_span, trace_span};

pub use h2::{Error as H2Error, Reason};
pub use linkerd_http_h2::{ClientKeepAlive, ClientParams, FlowControl, KeepAlive, ServerParams};

#[derive(Debug)]
pub struct Connect<C, B> {
    connect: C,
    params: ClientParams,
    _marker: PhantomData<fn() -> B>,
}

#[derive(Debug)]
pub struct Connection<B> {
    tx: hyper::client::conn::http2::SendRequest<B>,
    /// Set by the connection task when the peer's GOAWAY shuts the
    /// connection down.
    peer_goaway: Arc<AtomicBool>,
}

/// The error produced for a request that hyper canceled--i.e., abandoned
/// before it could be written to a connection--because the peer's GOAWAY shut
/// the connection down.
#[derive(Debug, thiserror::Error)]
#[error("request canceled by the peer's GOAWAY: {source}")]
pub struct GoAwayCanceled {
    #[source]
    source: hyper::Error,
}

// === impl GoAwayCanceled ===

impl GoAwayCanceled {
    /// Public so that tests in other crates can synthesize this error.
    pub fn new(source: hyper::Error) -> Self {
        debug_assert!(source.is_canceled());
        Self { source }
    }
}

// === impl Connect ===

impl<C, B> Connect<C, B> {
    pub fn new(connect: C, params: ClientParams) -> Self {
        Connect {
            connect,
            params,
            _marker: PhantomData,
        }
    }
}

impl<C: Clone, B> Clone for Connect<C, B> {
    fn clone(&self) -> Self {
        Connect {
            connect: self.connect.clone(),
            params: self.params.clone(),
            _marker: PhantomData,
        }
    }
}

type ConnectFuture<B> = Pin<Box<dyn Future<Output = Result<Connection<B>>> + Send + 'static>>;

impl<C, B, T> Service<T> for Connect<C, B>
where
    C: MakeConnection<(crate::Variant, T)>,
    C::Connection: Send + Unpin + 'static,
    C::Metadata: Send,
    C::Future: Send + 'static,
    B: Body + Send + Unpin + 'static,
    B::Data: Send,
    B::Error: Into<Error> + Send + Sync,
{
    type Response = Connection<B>;
    type Error = Error;
    type Future = ConnectFuture<B>;

    #[inline]
    fn poll_ready(&mut self, cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        self.connect.poll_ready(cx).map_err(Into::into)
    }

    fn call(&mut self, target: T) -> Self::Future {
        let ClientParams {
            flow_control,
            keep_alive,
            max_concurrent_reset_streams,
            max_frame_size,
            max_send_buf_size,
        } = self.params;

        let connect = self
            .connect
            .connect((crate::Variant::H2, target))
            .instrument(trace_span!("connect").or_current());

        Box::pin(
            async move {
                let (io, _meta) = connect.err_into::<Error>().await?;
                let mut builder = hyper::client::conn::http2::Builder::new(TokioExecutor::new());
                builder.timer(hyper_util::rt::TokioTimer::new());
                match flow_control {
                    None => {}
                    Some(FlowControl::Adaptive) => {
                        builder.adaptive_window(true);
                    }
                    Some(FlowControl::Fixed {
                        initial_stream_window_size,
                        initial_connection_window_size,
                    }) => {
                        builder
                            .initial_stream_window_size(initial_stream_window_size)
                            .initial_connection_window_size(initial_connection_window_size);
                    }
                }

                // Configure HTTP/2 PING frames
                if let Some(ClientKeepAlive {
                    timeout,
                    interval,
                    while_idle,
                }) = keep_alive
                {
                    builder
                        .keep_alive_timeout(timeout)
                        .keep_alive_interval(interval)
                        .keep_alive_while_idle(while_idle);
                }

                builder.max_frame_size(max_frame_size);
                if let Some(max) = max_concurrent_reset_streams {
                    builder.max_concurrent_reset_streams(max);
                }
                if let Some(sz) = max_send_buf_size {
                    builder.max_send_buf_size(sz);
                }

                let (tx, mut conn) = builder
                    .handshake(hyper_util::rt::TokioIo::new(io))
                    .instrument(trace_span!("handshake").or_current())
                    .await?;

                let peer_goaway = Arc::new(AtomicBool::new(false));
                tokio::spawn(
                    {
                        let peer_goaway = peer_goaway.clone();
                        async move {
                            // Requests queued in the dispatcher are canceled
                            // when `conn` is dropped, so record how the
                            // connection ended while holding it alive: their
                            // errors must be able to observe the flag.
                            match (&mut conn).await {
                                // Resolving without an error means the
                                // dispatcher shut down cleanly--most commonly
                                // on the peer's graceful GOAWAY, though a
                                // dropped `SendRequest` or an
                                // already-terminated connection also land
                                // here. Those two only occur once the
                                // dispatch queue is empty, so a request
                                // canceled afterwards was never written and
                                // marking it stays conservative.
                                Ok(()) => peer_goaway.store(true, Ordering::Release),
                                Err(error) => {
                                    let goaway = cause_ref::<H2Error>(&error)
                                        .is_some_and(|e| e.is_go_away() && e.is_remote());
                                    peer_goaway.store(goaway, Ordering::Release);
                                    debug!(%error, "failed");
                                }
                            }
                        }
                    }
                    .instrument(trace_span!("conn").or_current()),
                );

                Ok(Connection { tx, peer_goaway })
            }
            .instrument(debug_span!("h2").or_current()),
        )
    }
}

// === impl Connection ===

impl<B> Connection<B> {
    /// Returns a function that wraps this connection's request errors into
    /// `linkerd_error::Error`, marking cancelations caused by the peer's
    /// GOAWAY with [`GoAwayCanceled`]. hyper only cancels requests it never
    /// wrote to the connection, and it does so without recording why the
    /// connection went away, so the connection task's attribution is the only
    /// signal available.
    pub(crate) fn mark_goaway_cancelations(
        &self,
    ) -> impl Fn(hyper::Error) -> Error + Send + 'static {
        let peer_goaway = self.peer_goaway.clone();
        move |error| {
            if error.is_canceled() && peer_goaway.load(Ordering::Acquire) {
                return GoAwayCanceled::new(error).into();
            }
            error.into()
        }
    }
}

impl<B> tower::Service<http::Request<B>> for Connection<B>
where
    B: Body + Send + 'static,
    B::Data: Send,
    B::Error: Into<Error> + Send + Sync,
{
    type Response = http::Response<hyper::body::Incoming>;
    type Error = hyper::Error;
    type Future = Pin<Box<dyn Send + Future<Output = Result<Self::Response, Self::Error>>>>;

    #[inline]
    fn poll_ready(&mut self, cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        self.tx.poll_ready(cx).map_err(From::from)
    }

    fn call(&mut self, mut req: http::Request<B>) -> Self::Future {
        debug_assert_eq!(
            req.version(),
            http::Version::HTTP_2,
            "request version should be HTTP/2",
        );

        // A request translated from HTTP/1 to 2 might not include an
        // authority. In order to support that case, our h2 library requires
        // the version to be dropped down from HTTP/2, as a form of us
        // explicitly acknowledging that its not a normal HTTP/2 form.
        if req.uri().authority().is_none() {
            *req.version_mut() = http::Version::HTTP_11;
        }

        self.tx.send_request(req).boxed()
    }
}

#[cfg(test)]
mod tests;
