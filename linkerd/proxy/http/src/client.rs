//! The proxy's HTTP client.
//!
//! It can operate as a pure HTTP/1 client, a pure HTTP/2 client, or a
//! mixed-mode "orig-proto" client. The orig-proto mode attempts to dispatch
//! HTTP/1 messages over an H2 transport; however, some requests cannot be
//! proxied via this method, so it also maintains a fallback HTTP/1 client.

use crate::{h1, h2, orig_proto};
use futures::prelude::*;
use linkerd_error::{Error, Result};
use linkerd_http_box::BoxBody;
use linkerd_stack::{layer, ExtractParam, MakeConnection, Service, ServiceExt};
use std::{
    marker::PhantomData,
    pin::Pin,
    task::{Context, Poll},
};
use tracing::instrument::{Instrument, Instrumented};
use tracing::{debug, debug_span};

#[derive(Clone, Debug, Eq, PartialEq, Hash)]
pub enum Params {
    Http1(h1::PoolSettings),
    H2(h2::ClientParams),
    OrigProtoUpgrade(h2::ClientParams, h1::PoolSettings),
}

pub struct MakeClient<X, C, B> {
    connect: C,
    params: X,
    _marker: PhantomData<fn(B)>,
}

pub enum Client<C, T, B> {
    H2(h2::Connection<B>),
    Http1(h1::Client<C, T, B>),
    OrigProtoUpgrade(orig_proto::Upgrade<C, T, B>),
}

pub fn layer_via<X: Clone, C, B>(
    params: X,
) -> impl layer::Layer<C, Service = MakeClient<X, C, B>> + Clone {
    layer::mk(move |connect: C| MakeClient {
        connect,
        params: params.clone(),
        _marker: PhantomData,
    })
}

pub fn layer<C, B>() -> impl layer::Layer<C, Service = MakeClient<(), C, B>> + Clone {
    layer_via(())
}

// === impl MakeClient ===

type MakeFuture<C, T, B> = Pin<Box<dyn Future<Output = Result<Client<C, T, B>>> + Send + 'static>>;

impl<X, C, T, B> tower::Service<T> for MakeClient<X, C, B>
where
    T: Clone + Send + Sync + 'static,
    X: ExtractParam<Params, T>,
    C: MakeConnection<(crate::Variant, T)> + Clone + Unpin + Send + Sync + 'static,
    C::Connection: Unpin + Send,
    C::Metadata: Send,
    C::Future: Unpin + Send + 'static,
    B: crate::Body + Send + Unpin + 'static,
    B::Data: Send,
    B::Error: Into<Error> + Send + Sync,
{
    type Response = Client<C, T, B>;
    type Error = Error;
    type Future = MakeFuture<C, T, B>;

    #[inline]
    fn poll_ready(&mut self, _: &mut Context<'_>) -> Poll<Result<()>> {
        Poll::Ready(Ok(()))
    }

    fn call(&mut self, target: T) -> Self::Future {
        let connect = self.connect.clone();
        let settings = self.params.extract_param(&target);

        Box::pin(async move {
            debug!(?settings, "Building HTTP client");
            let client = match settings {
                Params::H2(params) => {
                    let h2 = h2::Connect::new(connect, params).oneshot(target).await?;
                    Client::H2(h2)
                }
                Params::Http1(params) => Client::Http1(h1::Client::new(connect, target, params)),
                Params::OrigProtoUpgrade(h2params, h1params) => {
                    let h2 = h2::Connect::new(connect.clone(), h2params)
                        .oneshot(target.clone())
                        .await?;
                    let http1 = h1::Client::new(connect, target, h1params);
                    Client::OrigProtoUpgrade(orig_proto::Upgrade::new(http1, h2))
                }
            };

            Ok(client)
        })
    }
}

impl<X: Clone, C: Clone, B> Clone for MakeClient<X, C, B> {
    fn clone(&self) -> Self {
        Self {
            connect: self.connect.clone(),
            params: self.params.clone(),
            _marker: self._marker,
        }
    }
}

// === impl Client ===

type RspFuture = Pin<Box<dyn Future<Output = Result<http::Response<BoxBody>>> + Send + 'static>>;

impl<C, T, B> Service<http::Request<B>> for Client<C, T, B>
where
    T: Clone + Send + Sync + 'static,
    C: MakeConnection<(crate::Variant, T)> + Clone + Send + Sync + 'static,
    C::Connection: Unpin + Send,
    C::Future: Unpin + Send + 'static,
    C::Error: Into<Error>,
    B: crate::Body + Send + Unpin + 'static,
    B::Data: Send,
    B::Error: Into<Error> + Send + Sync,
{
    type Response = http::Response<BoxBody>;
    type Error = Error;
    type Future = Instrumented<RspFuture>;

    #[inline]
    fn poll_ready(&mut self, cx: &mut Context<'_>) -> Poll<Result<()>> {
        match self {
            Self::H2(ref mut svc) => svc.poll_ready(cx).map_err(Into::into),
            Self::OrigProtoUpgrade(ref mut svc) => svc.poll_ready(cx).map_err(Into::into),
            Self::Http1(ref mut svc) => svc.poll_ready(cx),
        }
    }

    fn call(&mut self, req: http::Request<B>) -> Self::Future {
        let span = match self {
            Self::H2(_) => debug_span!("h2"),
            Self::Http1(_) => debug_span!("http1"),
            Self::OrigProtoUpgrade { .. } => debug_span!("orig-proto-upgrade"),
        };
        span.in_scope(|| {
            debug!(
                method = %req.method(),
                uri = %req.uri(),
                version = ?req.version(),
            );
            debug!(headers = ?req.headers());

            match self {
                Self::Http1(ref mut svc) => svc.call(req),
                Self::OrigProtoUpgrade(ref mut svc) => svc.call(req).map_err(Into::into).boxed(),
                Self::H2(ref mut svc) => Box::pin(
                    svc.try_send_request(req)
                        .map_err(|error| -> Error {
                            // Hyper returns the request only if it was not serialized.
                            if error.message().is_some() {
                                return h2::H2Error::from(h2::Reason::REFUSED_STREAM).into();
                            }
                            error.into_error().into()
                        })
                        .map_ok(|rsp| rsp.map(BoxBody::new)),
                ) as RspFuture,
            }
        })
        .instrument(span.or_current())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use linkerd_error::cause_ref;
    use std::sync::Arc;

    fn connect() -> (
        impl Future<
            Output = impl Service<
                http::Request<BoxBody>,
                Response = http::Response<BoxBody>,
                Error = Error,
                Future = Instrumented<RspFuture>,
            >,
        >,
        tokio::io::DuplexStream,
    ) {
        let (client_io, server_io) = tokio::io::duplex(64 * 1024);
        let client_io = Arc::new(tokio::sync::Mutex::new(Some(client_io)));
        let connect = linkerd_stack::service_fn(move |_: (crate::Variant, ())| {
            let io = client_io
                .try_lock()
                .expect("uncontended")
                .take()
                .expect("only one connection");
            futures::future::ok::<_, Error>((io, ()))
        });
        let client = async move {
            let client: Client<_, _, BoxBody> = MakeClient {
                connect,
                params: |_: &()| Params::H2(h2::ClientParams::default()),
                _marker: PhantomData,
            }
            .oneshot(())
            .await
            .expect("client must connect");
            client
        };
        (client, server_io)
    }

    #[tokio::test(flavor = "current_thread", start_paused = true)]
    async fn h2_refuses_requests_abandoned_by_a_peer_goaway() {
        refuses_unwritten_request(true).await;
    }

    #[tokio::test(flavor = "current_thread", start_paused = true)]
    async fn h2_refuses_requests_abandoned_without_goaway() {
        refuses_unwritten_request(false).await;
    }

    async fn refuses_unwritten_request(goaway: bool) {
        let (client, server_io) = connect();
        let (shutdown, wait) = tokio::sync::oneshot::channel::<()>();
        let server = tokio::spawn(async move {
            // Zero stream capacity keeps the request in hyper's dispatch queue.
            let mut srv = ::h2::server::Builder::new()
                .max_concurrent_streams(0)
                .handshake::<_, bytes::Bytes>(server_io)
                .await
                .expect("server handshake must succeed");
            tokio::select! {
                accepted = srv.accept() => panic!("server must not accept a stream: {accepted:?}"),
                _ = wait => {}
            }
            if goaway {
                srv.graceful_shutdown();
                assert!(
                    !matches!(srv.accept().await, Some(Ok(_))),
                    "server must not accept a stream during shutdown"
                );
            }
        });
        let mut client = client.await;
        let rsp = client
            .ready()
            .await
            .expect("client must be ready")
            .call(http_post());
        shutdown.send(()).expect("server must await the shutdown");
        server.await.expect("server task must not panic");
        let error = rsp.await.expect_err("request must fail");
        let h2 = cause_ref::<h2::H2Error>(&*error)
            .unwrap_or_else(|| panic!("must carry an HTTP/2 error: {error:?}"));
        assert_eq!(h2.reason(), Some(h2::Reason::REFUSED_STREAM));
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn h2_preserves_an_accepted_post_error() {
        tokio::time::timeout(std::time::Duration::from_secs(5), async {
            let (client, server_io) = connect();
            let (observed, wait) = tokio::sync::oneshot::channel();
            let server = tokio::spawn(async move {
                let mut srv = ::h2::server::handshake(server_io).await.unwrap();
                let (request, mut respond) = srv.accept().await.unwrap().unwrap();
                assert_eq!(request.method(), http::Method::POST);
                respond.send_reset(h2::Reason::INTERNAL_ERROR);
                tokio::select! {
                    _ = wait => {}
                    result = srv.accept() => panic!("accepted POST must not be sent again: {result:?}"),
                }
            });
            let mut client = client.await;
            let error = client
                .ready()
                .await
                .unwrap()
                .call(http_post())
                .await
                .unwrap_err();
            let hyper = cause_ref::<hyper::Error>(&*error).expect("must preserve hyper's error");
            assert!(!hyper.is_canceled());
            let h2 = cause_ref::<h2::H2Error>(hyper)
                .unwrap_or_else(|| panic!("original HTTP/2 error: {error:?}"));
            assert_eq!(h2.reason(), Some(h2::Reason::INTERNAL_ERROR));
            observed.send(()).unwrap();
            server.await.unwrap();
        })
        .await
        .expect("request must finish");
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn h2_dropping_an_accepted_post_cancels_the_stream() {
        tokio::time::timeout(std::time::Duration::from_secs(5), async {
            let (client, server_io) = connect();
            let (accepted, wait) = tokio::sync::oneshot::channel();
            let server = tokio::spawn(async move {
                let mut srv = ::h2::server::handshake(server_io).await.unwrap();
                let (request, mut respond) = srv.accept().await.unwrap().unwrap();
                assert_eq!(request.method(), http::Method::POST);
                accepted.send(()).unwrap();
                tokio::select! {
                    reset = futures::future::poll_fn(|cx| respond.poll_reset(cx)) => {
                        assert_eq!(reset.unwrap(), h2::Reason::CANCEL);
                    }
                    result = srv.accept() => panic!("expected stream cancellation: {result:?}"),
                }
            });
            let mut client = client.await;
            let rsp = client.ready().await.unwrap().call(http_post());
            wait.await.expect("server must accept the POST");
            drop(rsp);
            server.await.unwrap();
        })
        .await
        .expect("stream must be canceled");
    }

    fn http_post() -> http::Request<BoxBody> {
        http::Request::post("http://server.test/")
            .version(::http::Version::HTTP_2)
            .body(BoxBody::default())
            .expect("request must be valid")
    }
}
