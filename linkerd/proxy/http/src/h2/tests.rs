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
    tokio::spawn(async move {
        let mut srv = ::h2::server::handshake(server_io)
            .await
            .expect("server handshake must succeed");
        match reason {
            None => srv.graceful_shutdown(),
            Some(reason) => srv.abrupt_shutdown(reason),
        }
        while let Some(Ok(_)) = srv.accept().await {}
    })
    .await
    .expect("server task must not panic");
}

/// Issues a request that hyper cancels because the connection is gone, and
/// returns the error with this connection's marking applied.
async fn canceled_request(conn: &mut Connection<BoxBody>) -> Error {
    // With the clock paused, time only advances once every task is idle,
    // so the connection task has necessarily observed the shutdown.
    tokio::time::sleep(std::time::Duration::from_millis(1)).await;

    let error = conn
        .call(http_get())
        .await
        .expect_err("request must be canceled");
    assert!(error.is_canceled(), "must be canceled: {error:?}");
    conn.mark_goaway_cancelations()(error)
}

/// A request canceled because the peer's graceful GOAWAY shut the
/// connection down is marked. This drives the cancelation hyper produces
/// for a request issued after the shutdown; the cancelation of a request
/// already queued in the dispatcher is covered by
/// `client::tests::h2_marks_requests_abandoned_by_a_peer_goaway`.
#[tokio::test(flavor = "current_thread", start_paused = true)]
async fn marks_canceled_requests_on_peer_goaway() {
    let (mut conn, server_io) = connect().await;
    shut_down(server_io, None).await;

    let marked = canceled_request(&mut conn).await;
    let goaway = linkerd_error::cause_ref::<GoAwayCanceled>(&*marked)
        .expect("cancelation must be marked as caused by the GOAWAY");
    assert!(goaway.source.is_canceled(), "must be canceled: {goaway:?}");
}

/// A peer's GOAWAY carrying an error code ends the connection with an
/// error rather than cleanly, so it exercises the other branch of the
/// connection task's attribution.
#[tokio::test(flavor = "current_thread", start_paused = true)]
async fn marks_canceled_requests_on_peer_goaway_error() {
    let (mut conn, server_io) = connect().await;
    shut_down(server_io, Some(::h2::Reason::ENHANCE_YOUR_CALM)).await;

    let marked = canceled_request(&mut conn).await;
    linkerd_error::cause_ref::<GoAwayCanceled>(&*marked)
        .expect("cancelation must be marked as caused by the GOAWAY");
}

/// A request canceled because the connection failed without a peer GOAWAY
/// surfaces hyper's error unmarked.
#[tokio::test(flavor = "current_thread", start_paused = true)]
async fn does_not_mark_canceled_requests_without_goaway() {
    let (mut conn, server_io) = connect().await;

    // The server closes the connection without sending a GOAWAY.
    drop(server_io);

    let marked = canceled_request(&mut conn).await;
    assert!(
        linkerd_error::cause_ref::<GoAwayCanceled>(&*marked).is_none(),
        "cancelation must not be attributed to a GOAWAY: {marked:?}"
    );
    let hyper = linkerd_error::cause_ref::<hyper::Error>(&*marked)
        .expect("must be caused by hyper's client");
    assert!(hyper.is_canceled(), "must be canceled: {hyper:?}");
}
