use super::*;
use linkerd_http_box::BoxBody;
use linkerd_stack::ServiceExt;

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
    let conn = Connect::<_, BoxBody>::new(connect, ClientParams::default())
        .oneshot(())
        .await
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
    let mut srv = ::h2::server::handshake(server_io)
        .await
        .expect("server handshake must succeed");
    match reason {
        None => srv.graceful_shutdown(),
        Some(reason) => srv.abrupt_shutdown(reason),
    }
    while let Some(Ok(_)) = srv.accept().await {}
}

/// Issues a request after the connection task has observed the shutdown.
async fn canceled_request(
    conn: &mut Connection<BoxBody>,
) -> hyper::client::conn::TrySendError<http::Request<BoxBody>> {
    // With the clock paused, time only advances once every task is idle.
    tokio::time::sleep(std::time::Duration::from_millis(1)).await;

    let error = conn
        .try_send_request(http_get())
        .await
        .expect_err("request must be canceled");
    let request = error.message().expect("unwritten request must be returned");
    assert_eq!(request.uri(), "http://server.test/");
    error
}

/// A request submitted after graceful GOAWAY is returned to the caller.
/// Requests already queued are covered by the native client tests.
#[tokio::test(flavor = "current_thread", start_paused = true)]
async fn returns_unwritten_requests_on_peer_goaway() {
    let (mut conn, server_io) = connect().await;
    shut_down(server_io, None).await;

    let error = canceled_request(&mut conn).await.into_error();
    assert!(error.is_canceled(), "must be canceled: {error:?}");
}

#[tokio::test(flavor = "current_thread", start_paused = true)]
async fn returns_unwritten_requests_on_peer_goaway_error() {
    let (mut conn, server_io) = connect().await;
    shut_down(server_io, Some(::h2::Reason::ENHANCE_YOUR_CALM)).await;

    let error = canceled_request(&mut conn).await.into_error();
    assert!(error.is_canceled(), "must be canceled: {error:?}");
}

/// Recovery proves the request was not written even without a peer GOAWAY.
#[tokio::test(flavor = "current_thread", start_paused = true)]
async fn returns_unwritten_requests_without_goaway() {
    let (mut conn, server_io) = connect().await;
    drop(server_io);

    let error = canceled_request(&mut conn).await.into_error();
    assert!(error.is_canceled(), "must be canceled: {error:?}");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn does_not_return_an_accepted_request_on_goaway() {
    tokio::time::timeout(std::time::Duration::from_secs(5), async {
        let (mut conn, server_io) = connect().await;
        let server = tokio::spawn(async move {
            let mut srv = ::h2::server::handshake(server_io).await.unwrap();
            let (_request, _respond) = srv.accept().await.unwrap().unwrap();
            srv.abrupt_shutdown(Reason::INTERNAL_ERROR);
            while let Some(Ok(_)) = srv.accept().await {}
        });
        let error = conn
            .ready()
            .await
            .unwrap()
            .try_send_request(http_get())
            .await
            .unwrap_err();
        assert!(
            error.message().is_none(),
            "accepted request cannot be recovered"
        );
        let error = error.into_error();
        assert!(
            !error.is_canceled(),
            "accepted request must not be canceled: {error:?}"
        );
        server.await.unwrap();
    })
    .await
    .expect("request must finish");
}

/// GOAWAY can reject a stream already written on the wire. Its request is
/// not recoverable through this API, even when its ID exceeds last-stream-id.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn does_not_return_a_written_stream_above_goaway_last_stream_id() {
    use tokio::io::{AsyncReadExt, AsyncWriteExt};

    tokio::time::timeout(std::time::Duration::from_secs(5), async {
        let (mut conn, mut server_io) = connect().await;
        let server = tokio::spawn(async move {
            let mut preface = [0; 24];
            server_io.read_exact(&mut preface).await.unwrap();
            assert_eq!(&preface, b"PRI * HTTP/2.0\r\n\r\nSM\r\n\r\n");
            // Initial server SETTINGS, followed by the client's SETTINGS ACK.
            server_io
                .write_all(&[0, 0, 0, 4, 0, 0, 0, 0, 0])
                .await
                .unwrap();
            loop {
                let mut header = [0; 9];
                server_io.read_exact(&mut header).await.unwrap();
                let len = u32::from_be_bytes([0, header[0], header[1], header[2]]) as usize;
                let mut payload = vec![0; len];
                server_io.read_exact(&mut payload).await.unwrap();
                match header[3] {
                    4 if header[4] & 1 == 0 => {
                        server_io
                            .write_all(&[0, 0, 0, 4, 1, 0, 0, 0, 0])
                            .await
                            .unwrap();
                    }
                    1 => {
                        let stream_id =
                            u32::from_be_bytes(header[5..9].try_into().unwrap()) & 0x7fff_ffff;
                        assert_eq!(stream_id, 1);
                        // GOAWAY(NO_ERROR), last-stream-id=0: stream 1 was not processed.
                        server_io
                            .write_all(&[0, 0, 8, 7, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0])
                            .await
                            .unwrap();
                        break;
                    }
                    _ => {}
                }
            }
        });
        let error = conn
            .ready()
            .await
            .unwrap()
            .try_send_request(http_get())
            .await
            .unwrap_err();
        assert!(
            error.message().is_none(),
            "written request cannot be recovered"
        );
        let error = error.into_error();
        assert!(
            !error.is_canceled(),
            "written stream has an HTTP/2 error: {error:?}"
        );
        let h2 = linkerd_error::cause_ref::<H2Error>(&error)
            .unwrap_or_else(|| panic!("original HTTP/2 error: {error:?}"));
        assert!(
            h2.is_go_away() && h2.is_remote(),
            "must retain peer GOAWAY: {h2:?}"
        );
        assert_eq!(h2.reason(), Some(Reason::NO_ERROR));
        server.await.unwrap();
    })
    .await
    .expect("request must finish");
}
