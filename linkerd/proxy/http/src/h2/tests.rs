use super::*;
use linkerd_http_box::BoxBody;
use linkerd_stack::ServiceExt;
use std::{
    sync::OnceLock,
    task::{Wake, Waker},
};
use tokio::time::{self, Duration};

const TIMEOUT: Duration = Duration::from_secs(5);

/// A request canceled because the peer's graceful GOAWAY shut the
/// connection down is refused. This drives the cancelation hyper produces
/// for a request issued after the shutdown; the cancelation of a request
/// already queued in the dispatcher is covered by
/// `client::tests::h2_refuses_queued_requests_on_goaway`.
#[tokio::test(flavor = "current_thread", start_paused = true)]
async fn refuses_canceled_requests_on_peer_goaway() {
    let (mut conn, server_io) = connect().await;
    shut_down(server_io, None).await;

    let error = canceled_request(&mut conn).await;
    let h2 = linkerd_error::cause_ref::<H2Error>(&*error).expect("must carry an HTTP/2 error");
    assert_eq!(h2.reason(), Some(Reason::REFUSED_STREAM));
}

/// A peer's GOAWAY carrying an error code ends the connection with an
/// error rather than cleanly, so it exercises the other branch of the
/// connection task's attribution.
#[tokio::test(flavor = "current_thread", start_paused = true)]
async fn refuses_canceled_requests_on_peer_goaway_error() {
    let (mut conn, server_io) = connect().await;
    shut_down(server_io, Some(::h2::Reason::ENHANCE_YOUR_CALM)).await;

    let error = canceled_request(&mut conn).await;
    let h2 = linkerd_error::cause_ref::<H2Error>(&*error).expect("must carry an HTTP/2 error");
    assert_eq!(h2.reason(), Some(Reason::REFUSED_STREAM));
}

/// A request canceled because the connection failed without a peer GOAWAY
/// preserves hyper's original error.
#[tokio::test(flavor = "current_thread", start_paused = true)]
async fn does_not_refuse_canceled_requests_without_goaway() {
    let (mut conn, server_io) = connect().await;

    // The server closes the connection without sending a GOAWAY.
    drop(server_io);

    let error = canceled_request(&mut conn).await;
    assert!(
        linkerd_error::cause_ref::<H2Error>(&*error).is_none(),
        "cancelation must not be attributed to a GOAWAY: {error:?}"
    );
    let hyper = linkerd_error::cause_ref::<hyper::Error>(&*error)
        .expect("must be caused by hyper's client");
    assert!(hyper.is_canceled(), "must be canceled: {hyper:?}");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn does_not_refuse_an_accepted_request_on_goaway() {
    time::timeout(TIMEOUT, async {
        let (mut conn, server_io) = connect().await;
        let server = tokio::spawn(async move {
            let mut srv = ::h2::server::handshake(server_io).await.unwrap();
            let (_request, _respond) = srv.accept().await.unwrap().unwrap();
            srv.abrupt_shutdown(Reason::INTERNAL_ERROR);
            while let Some(Ok(_)) = srv.accept().await {}
        });
        let rescue_goaway = conn.rescue_goaway();
        let error = conn
            .ready()
            .await
            .unwrap()
            .call(http_get())
            .await
            .unwrap_err();
        assert!(
            !error.is_canceled(),
            "accepted request must not be canceled: {error:?}"
        );
        server.await.unwrap();
        while !conn.peer_goaway.load(Ordering::Acquire) {
            tokio::task::yield_now().await;
        }
        let original = format!("{error:?}");
        let error = rescue_goaway(error);
        let hyper = linkerd_error::cause_ref::<hyper::Error>(&*error)
            .expect("accepted request must preserve hyper's original error");
        assert_eq!(format!("{hyper:?}"), original);
    })
    .await
    .expect("request must finish");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn refuses_queued_requests_on_peer_goaway() {
    time::timeout(TIMEOUT, async {
        for reason in [None, Some(Reason::ENHANCE_YOUR_CALM)] {
            let (mut conn, server_io) = connect().await;
            let (settings_processed, wait_settings) = tokio::sync::oneshot::channel();
            let (shutdown, wait) = tokio::sync::oneshot::channel();
            let server = tokio::spawn(async move {
                let mut srv = ::h2::server::Builder::new()
                    .max_concurrent_streams(0)
                    .handshake::<_, bytes::Bytes>(server_io)
                    .await
                    .expect("server handshake must succeed");
                let mut ping = srv.ping_pong().expect("ping handle must be available");
                tokio::select! {
                    accepted = srv.accept() => panic!("server must not accept a stream: {accepted:?}"),
                    _ = async {
                        ping.ping(::h2::Ping::opaque()).await.expect("peer must acknowledge ping");
                        settings_processed.send(()).expect("client must await settings");
                        wait.await.expect("client must signal shutdown");
                    } => {}
                }
                match reason {
                    None => srv.graceful_shutdown(),
                    Some(reason) => srv.abrupt_shutdown(reason),
                }
                match srv.accept().await {
                    Some(Ok(_)) => panic!("server must not accept a stream during shutdown"),
                    Some(Err(error)) => assert!(error
                        .get_io()
                        .is_some_and(|e| e.kind() == std::io::ErrorKind::BrokenPipe)),
                    None => {}
                }
            });

            wait_settings.await.expect("server must acknowledge settings");
            let _pending_open = conn
                .ready()
                .await
                .expect("client must be ready")
                .call(http_get());
            let mut rsp = conn
                .ready()
                .await
                .expect("client must be ready")
                .call(http_get());
            let observed = Arc::new(OnceLock::new());
            let response = futures::future::poll_fn(|cx| {
                let waker = Waker::from(Arc::new(GoAwayWaker {
                    peer_goaway: conn.peer_goaway.clone(),
                    observed: observed.clone(),
                    waker: cx.waker().clone(),
                }));
                rsp.as_mut().poll(&mut Context::from_waker(&waker))
            });
            tokio::pin!(response);
            futures::future::poll_fn(|cx| {
                assert!(response.as_mut().poll(cx).is_pending());
                Poll::Ready(())
            })
            .await;
            shutdown.send(()).expect("server must await shutdown");

            let error = response.await.expect_err("request must be canceled");
            assert!(error.is_canceled(), "request must remain queued: {error:?}");
            assert_eq!(
                observed.get(),
                Some(&true),
                "GOAWAY must be recorded before waking the request"
            );
            let error = conn.rescue_goaway()(error);
            let h2 = linkerd_error::cause_ref::<H2Error>(&*error).expect("must carry an HTTP/2 error");
            assert_eq!(h2.reason(), Some(Reason::REFUSED_STREAM));
            server.await.expect("server task must not panic");
        }
    })
    .await
    .expect("queued request test timed out");
}

/// Connects a client through `Connect` to one half of a duplex, handing
/// the other half to the caller as the server-side I/O.
async fn connect() -> (Connection<BoxBody>, tokio::io::DuplexStream) {
    let (client_io, server_io) = tokio::io::duplex(64 * 1024);
    let mut client_io = Some(client_io);
    let connect = linkerd_stack::service_fn(move |(_, ()): (crate::Variant, ())| {
        futures::future::ready(Ok::<_, Error>((
            client_io.take().expect("only one connection"),
            (),
        )))
    });
    let conn = time::timeout(
        TIMEOUT,
        Connect::<_, BoxBody>::new(connect, ClientParams::default()).oneshot(()),
    )
    .await
    .expect("client handshake timed out")
    .expect("client handshake must succeed");
    (conn, server_io)
}

fn http_get() -> http::Request<BoxBody> {
    http::Request::builder()
        .version(::http::Version::HTTP_2)
        .uri("http://server.test/")
        .body(BoxBody::default())
        .expect("request must be valid")
}

/// Runs a real server against `server_io` until it has shut down, so that
/// the whole frame exchange has provably completed on the wire.
async fn shut_down(server_io: tokio::io::DuplexStream, reason: Option<::h2::Reason>) {
    time::timeout(
        TIMEOUT,
        tokio::spawn(async move {
            let mut srv = ::h2::server::handshake(server_io)
                .await
                .expect("server handshake must succeed");
            match reason {
                None => srv.graceful_shutdown(),
                Some(reason) => srv.abrupt_shutdown(reason),
            }
            while let Some(Ok(_)) = srv.accept().await {}
        }),
    )
    .await
    .expect("server shutdown timed out")
    .expect("server task must not panic");
}

/// Issues a request that hyper cancels because the connection is gone, and
/// returns the error with this connection's error mapping applied.
async fn canceled_request(conn: &mut Connection<BoxBody>) -> Error {
    // With the clock paused, time only advances once every task is idle,
    // so the connection task has necessarily observed the shutdown.
    tokio::time::sleep(std::time::Duration::from_millis(1)).await;

    let error = time::timeout(TIMEOUT, conn.call(http_get()))
        .await
        .expect("request timed out")
        .expect_err("request must be canceled");
    assert!(error.is_canceled(), "must be canceled: {error:?}");
    conn.rescue_goaway()(error)
}

struct GoAwayWaker {
    peer_goaway: Arc<AtomicBool>,
    observed: Arc<OnceLock<bool>>,
    waker: Waker,
}

impl Wake for GoAwayWaker {
    fn wake(self: Arc<Self>) {
        let _ = self.observed.set(self.peer_goaway.load(Ordering::Acquire));
        self.waker.wake_by_ref();
    }
}
