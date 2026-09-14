use super::*;
use linkerd_http_box::BoxBody;
use linkerd_stack::ServiceExt;
use tokio::time::{self, Duration};

const TIMEOUT: Duration = Duration::from_secs(5);

#[tokio::test(flavor = "current_thread")]
async fn refuses_request_recovered_after_graceful_goaway() {
    time::timeout(TIMEOUT, async {
        let (mut conn, server_io) = connect().await;
        let (ready, wait_ready) = tokio::sync::oneshot::channel();
        let (shutdown, wait_shutdown) = tokio::sync::oneshot::channel();
        let server = tokio::spawn(async move {
            let mut server = ::h2::server::Builder::new()
                .max_concurrent_streams(0)
                .handshake::<_, bytes::Bytes>(server_io)
                .await
                .expect("server handshake must succeed");
            let mut ping = server.ping_pong().expect("ping handle must be available");
            tokio::select! {
                request = server.accept() => panic!("server must not accept a request: {request:?}"),
                _ = async {
                    ping.ping(::h2::Ping::opaque())
                        .await
                        .expect("client must acknowledge settings");
                    ready.send(()).expect("client must wait for settings");
                    wait_shutdown.await.expect("client must signal shutdown");
                } => {}
            }
            server.graceful_shutdown();
            while let Some(Ok(_)) = server.accept().await {}
        });

        wait_ready.await.expect("server must signal readiness");
        let _pending_open = conn.call(http_get());
        let response = conn.send_request_or_refuse(http_get());
        shutdown.send(()).expect("server must await shutdown");

        let error = response.await.expect_err("request must be refused");
        let h2 = linkerd_error::cause_ref::<H2Error>(&*error)
            .unwrap_or_else(|| panic!("must carry an H2 error: {error:?}"));
        assert_eq!(h2.reason(), Some(Reason::REFUSED_STREAM));
        server.await.expect("server task must not panic");
    })
    .await
    .expect("test must complete");
}

#[tokio::test(flavor = "current_thread", start_paused = true)]
async fn preserves_request_error_without_goaway() {
    time::timeout(TIMEOUT, async {
        let (mut conn, server_io) = connect().await;
        let (ready, wait_ready) = tokio::sync::oneshot::channel();
        let (disconnect, wait_disconnect) = tokio::sync::oneshot::channel();
        let server = tokio::spawn(async move {
            let mut server = ::h2::server::Builder::new()
                .max_concurrent_streams(0)
                .handshake::<_, bytes::Bytes>(server_io)
                .await
                .expect("server handshake must succeed");
            let mut ping = server.ping_pong().expect("ping handle must be available");
            tokio::select! {
                request = server.accept() => panic!("server must not accept a request: {request:?}"),
                _ = async {
                    ping.ping(::h2::Ping::opaque())
                        .await
                        .expect("client must acknowledge settings");
                    ready.send(()).expect("client must wait for settings");
                    wait_disconnect.await.expect("client must signal disconnect");
                } => {}
            }
        });

        wait_ready.await.expect("server must signal readiness");
        let _pending_open = conn.call(http_get());
        let response = conn.send_request_or_refuse(http_get());
        disconnect.send(()).expect("server must await disconnect");

        let error = time::timeout(Duration::from_secs(1), response)
            .await
            .expect("request must complete")
            .expect_err("request must fail");
        assert_canceled_without_h2(&error, "disconnect");
        server.await.expect("server task must not panic");
    })
    .await
    .expect("test must complete");
}

#[tokio::test(flavor = "current_thread", start_paused = true)]
async fn preserves_recovered_request_when_client_is_dropped() {
    time::timeout(TIMEOUT, async {
        let (mut conn, server_io) = connect().await;
        let response = conn.send_request_or_refuse(http_get());
        drop(conn);
        drop(server_io);

        let error = time::timeout(Duration::from_secs(1), response)
            .await
            .expect("request must complete")
            .expect_err("request must fail");
        assert_canceled_without_h2(&error, "client shutdown");
    })
    .await
    .expect("test must complete");
}

#[tokio::test(flavor = "current_thread", start_paused = true)]
async fn preserves_recovered_request_after_keep_alive_timeout() {
    time::timeout(TIMEOUT, async {
        let params = ClientParams {
            keep_alive: Some(ClientKeepAlive {
                interval: Duration::from_millis(1),
                timeout: Duration::from_millis(1),
                while_idle: true,
            }),
            ..ClientParams::default()
        };
        let (mut conn, server_io) = connect_with_params(params).await;
        let (ready, wait_ready) = tokio::sync::oneshot::channel();
        let (done, wait_done) = tokio::sync::oneshot::channel();
        let server = tokio::spawn(async move {
            let mut server = ::h2::server::Builder::new()
                .max_concurrent_streams(0)
                .handshake::<_, bytes::Bytes>(server_io)
                .await
                .expect("server handshake must succeed");
            let mut ping = server.ping_pong().expect("ping handle must be available");
            tokio::select! {
                request = server.accept() => panic!("server must not accept a request: {request:?}"),
                result = ping.ping(::h2::Ping::opaque()) => {
                    result.expect("client must acknowledge settings");
                    ready.send(()).expect("client must wait for settings");
                }
            }
            wait_done.await.expect("client must signal completion");
        });

        wait_ready.await.expect("server must signal readiness");
        let _pending_open = conn.call(http_get());
        let error = conn
            .send_request_or_refuse(http_get())
            .await
            .expect_err("request must fail");
        assert_canceled_without_h2(&error, "keep-alive timeout");
        done.send(()).expect("server must await completion");
        server.await.expect("server task must not panic");
    })
    .await
    .expect("test must complete");
}

#[tokio::test(flavor = "current_thread", start_paused = true)]
async fn preserves_error_for_accepted_request() {
    time::timeout(TIMEOUT, async {
        let (mut conn, server_io) = connect().await;
        let (done, wait_done) = tokio::sync::oneshot::channel();
        let server = tokio::spawn(async move {
            let mut server = ::h2::server::handshake(server_io)
                .await
                .expect("server handshake must succeed");
            let (_, mut respond) = server
                .accept()
                .await
                .expect("server must remain open")
                .expect("server must accept the request");
            respond.send_reset(Reason::INTERNAL_ERROR);
            tokio::select! {
                _ = wait_done => {}
                request = server.accept() => panic!("server must not accept another request: {request:?}"),
            }
        });

        let error = conn
            .send_request_or_refuse(http_get())
            .await
            .expect_err("request must fail");
        let h2 = linkerd_error::cause_ref::<H2Error>(&*error)
            .unwrap_or_else(|| panic!("must carry an H2 error: {error:?}"));
        assert_eq!(h2.reason(), Some(Reason::INTERNAL_ERROR));
        done.send(()).expect("server must await completion");
        server.await.expect("server task must not panic");
    })
    .await
    .expect("test must complete");
}

async fn connect() -> (Connection<BoxBody>, tokio::io::DuplexStream) {
    connect_with_params(ClientParams::default()).await
}

async fn connect_with_params(
    params: ClientParams,
) -> (Connection<BoxBody>, tokio::io::DuplexStream) {
    let (client_io, server_io) = tokio::io::duplex(64 * 1024);
    let mut client_io = Some(client_io);
    let connect = linkerd_stack::service_fn(move |(_, ()): (crate::Variant, ())| {
        futures::future::ready(Ok::<_, Error>((
            client_io.take().expect("only one connection"),
            (),
        )))
    });
    let conn = Connect::<_, BoxBody>::new(connect, params)
        .oneshot(())
        .await
        .expect("client handshake must succeed");
    (conn, server_io)
}

fn http_get() -> http::Request<BoxBody> {
    http::Request::builder()
        .version(http::Version::HTTP_2)
        .uri("http://server.test/")
        .body(BoxBody::default())
        .expect("request must be valid")
}

fn assert_canceled_without_h2(error: &Error, source: &str) {
    assert!(
        linkerd_error::cause_ref::<H2Error>(&**error).is_none(),
        "{source} must preserve the original error: {error:?}"
    );
    let hyper = linkerd_error::cause_ref::<hyper::Error>(&**error)
        .unwrap_or_else(|| panic!("{source} must preserve the Hyper error: {error:?}"));
    assert!(hyper.is_canceled(), "request must be recovered: {error:?}");
}
