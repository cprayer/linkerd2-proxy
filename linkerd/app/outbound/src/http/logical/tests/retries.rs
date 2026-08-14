use super::*;
use http_body_util::BodyExt;
use linkerd_app_core::{
    errors, io,
    proxy::http::{self, StatusCode},
    svc::http::stream_timeouts::StreamDeadlineError,
    trace,
};
use linkerd_proxy_client_policy::{
    self as client_policy,
    grpc::{Codes, RouteParams as GrpcParams},
    http::{RouteParams as HttpParams, Timeouts},
};
use std::collections::BTreeSet;
use tokio::time;
use tonic::Code;
use tracing::{info, Instrument};

#[tokio::test(flavor = "current_thread", start_paused = true)]
async fn http_5xx() {
    let _trace = trace::test::trace_init();

    const TIMEOUT: time::Duration = time::Duration::from_secs(2);
    let (svc, mut handle) = mock_http(HttpParams {
        timeouts: Timeouts {
            request: Some(TIMEOUT),
            ..Default::default()
        },
        retry: Some(client_policy::http::Retry {
            max_retries: 1,
            status_ranges: Default::default(),
            max_request_bytes: 1000,
            timeout: None,
            backoff: None,
        }),
        ..Default::default()
    });

    tokio::spawn(
        async move {
            handle.allow(2);
            serve(&mut handle, mk_rsp(StatusCode::INTERNAL_SERVER_ERROR, "")).await;
            serve(&mut handle, mk_rsp(StatusCode::NO_CONTENT, "")).await;
            handle
        }
        .in_current_span(),
    );

    info!("Sending a request that will initially fail and then succeed");
    let rsp = time::timeout(TIMEOUT, send_req(svc.clone(), http_get()))
        .await
        .expect("response");
    info!("Verifying that we see the successful response");
    assert_eq!(rsp.expect("response").status(), StatusCode::NO_CONTENT);
}

#[tokio::test(flavor = "current_thread", start_paused = true)]
async fn http_5xx_limited() {
    let _trace = trace::test::trace_init();

    const TIMEOUT: time::Duration = time::Duration::from_secs(2);
    let (svc, mut handle) = mock_http(HttpParams {
        timeouts: Timeouts {
            request: Some(TIMEOUT),
            ..Default::default()
        },
        retry: Some(client_policy::http::Retry {
            max_retries: 2,
            status_ranges: Default::default(),
            max_request_bytes: 1000,
            timeout: None,
            backoff: None,
        }),
        ..Default::default()
    });

    info!("Sending a request that will initially fail and then succeed");
    tokio::spawn(
        async move {
            handle.allow(3);
            serve(&mut handle, async move {
                info!("Failing the first request");
                mk_rsp(StatusCode::INTERNAL_SERVER_ERROR, "").await
            })
            .await;
            serve(&mut handle, async move {
                info!("Failing the second request");
                mk_rsp(StatusCode::INTERNAL_SERVER_ERROR, "").await
            })
            .await;
            serve(&mut handle, async move {
                info!("Failing the third request");
                mk_rsp(StatusCode::GATEWAY_TIMEOUT, "").await
            })
            .await;
            info!("Prepping the fourth request (shouldn't be served)");
            serve(&mut handle, async move {
                mk_rsp(StatusCode::NO_CONTENT, "").await
            })
            .await;
            handle
        }
        .in_current_span(),
    );

    info!("Verifying that the response fails with the expected error");
    let rsp = time::timeout(TIMEOUT, send_req(svc.clone(), http_get()))
        .await
        .expect("response");
    assert_eq!(rsp.expect("response").status(), StatusCode::GATEWAY_TIMEOUT);
}

#[tokio::test(flavor = "current_thread", start_paused = true)]
async fn http_timeout() {
    let _trace = trace::test::trace_init();

    const TIMEOUT: time::Duration = time::Duration::from_secs(2);
    let (svc, mut handle) = mock_http(HttpParams {
        timeouts: Timeouts {
            request: Some(TIMEOUT),
            ..Default::default()
        },
        retry: Some(client_policy::http::Retry {
            max_retries: 1,
            status_ranges: Default::default(),
            max_request_bytes: 1000,
            timeout: Some(TIMEOUT / 4),
            backoff: None,
        }),
        ..Default::default()
    });

    info!("Sending a request that will initially timeout and then succeed");
    tokio::spawn(
        async move {
            handle.allow(2);

            serve(&mut handle, async move {
                info!("Delaying the first request");
                time::sleep(TIMEOUT / 2).await;
                mk_rsp(StatusCode::NOT_FOUND, "").await
            })
            .await;

            serve(&mut handle, async move {
                info!("Serving the second request");
                mk_rsp(StatusCode::NO_CONTENT, "").await
            })
            .await;

            handle
        }
        .in_current_span(),
    );

    info!("Verifying that the response fails with the expected error");
    let rsp = time::timeout(TIMEOUT, send_req(svc.clone(), http_get()))
        .await
        .expect("response");
    assert_eq!(rsp.expect("response").status(), StatusCode::NO_CONTENT);
}

#[tokio::test(flavor = "current_thread", start_paused = true)]
async fn http_timeout_on_limit() {
    let _trace = trace::test::trace_init();

    const TIMEOUT: time::Duration = time::Duration::from_secs(2);
    let (svc, mut handle) = mock_http(HttpParams {
        timeouts: Timeouts {
            request: Some(TIMEOUT),
            ..Default::default()
        },
        retry: Some(client_policy::http::Retry {
            max_retries: 1,
            status_ranges: Default::default(),
            max_request_bytes: 1000,
            timeout: Some(TIMEOUT / 4),
            backoff: None,
        }),
        ..Default::default()
    });

    tokio::spawn(
        async move {
            handle.allow(2);

            serve(&mut handle, async move {
                info!("Delaying the first request");
                time::sleep(TIMEOUT / 3).await;
                mk_rsp(StatusCode::NOT_FOUND, "").await
            })
            .await;

            serve(&mut handle, async move {
                info!("Delaying the second request");
                time::sleep(TIMEOUT / 3).await;
                mk_rsp(StatusCode::NO_CONTENT, "").await
            })
            .await;

            handle
        }
        .in_current_span(),
    );

    info!("Testing that a retry timeout does not apply when max retries is reached");
    let rsp = time::timeout(TIMEOUT, send_req(svc.clone(), http_get()))
        .await
        .expect("response");

    info!("Verifying that the initial request was retried");
    assert_eq!(rsp.expect("response").status(), StatusCode::NO_CONTENT);
}

#[tokio::test(flavor = "current_thread", start_paused = true)]
async fn http_timeout_with_request_timeout() {
    let _trace = trace::test::trace_init();

    const TIMEOUT: time::Duration = time::Duration::from_millis(100);
    let (svc, mut handle) = mock_http(HttpParams {
        timeouts: Timeouts {
            request: Some(TIMEOUT * 5),
            ..Default::default()
        },
        retry: Some(client_policy::http::Retry {
            max_retries: 2,
            status_ranges: Default::default(),
            max_request_bytes: 1000,
            timeout: Some(TIMEOUT),
            backoff: None,
        }),
        ..Default::default()
    });

    info!("Sending a request that will initially timeout and then succeed");
    tokio::spawn(
        async move {
            handle.allow(6);

            // First request.

            serve(&mut handle, async move {
                info!("Delaying the first request");
                time::sleep(TIMEOUT * 2).await;
                mk_rsp(StatusCode::IM_A_TEAPOT, "").await
            })
            .await;

            serve(&mut handle, async move {
                info!("Delaying the second request");
                time::sleep(TIMEOUT * 2).await;
                mk_rsp(StatusCode::IM_A_TEAPOT, "").await
            })
            .await;

            serve(&mut handle, async move {
                info!("Delaying the third request");
                mk_rsp(StatusCode::NO_CONTENT, "").await
            })
            .await;

            // Second request

            serve(&mut handle, async move {
                info!("Delaying the fourth request");
                time::sleep(TIMEOUT * 2).await;
                mk_rsp(StatusCode::IM_A_TEAPOT, "").await
            })
            .await;

            serve(&mut handle, async move {
                info!("Delaying the fifth request");
                time::sleep(TIMEOUT * 2).await;
                mk_rsp(StatusCode::IM_A_TEAPOT, "").await
            })
            .await;

            serve(&mut handle, async move {
                info!("Delaying the sixth request");
                time::sleep(TIMEOUT * 5).await;
                mk_rsp(StatusCode::NO_CONTENT, "").await
            })
            .await;

            handle
        }
        .in_current_span(),
    );

    info!("Verifying that the response succeeds despite retry timeouts");
    let rsp = time::timeout(TIMEOUT * 10, send_req(svc.clone(), http_get()))
        .await
        .expect("response timed out")
        .expect("response ok");
    assert_eq!(rsp.status(), StatusCode::NO_CONTENT);

    info!("Verifying that retried requests fail with a request timeout");
    let error = time::timeout(TIMEOUT * 10, send_req(svc.clone(), http_get()))
        .await
        .expect("response timed out")
        .expect_err("response should timeout");
    assert!(errors::is_caused_by::<StreamDeadlineError>(&*error));
}

/// Reproduces linkerd/linkerd2#12964: a request abandoned by a graceful
/// GOAWAY was never written to the connection, so it is retried.
#[tokio::test(flavor = "current_thread", start_paused = true)]
async fn http_h2_goaway_canceled() {
    let _trace = trace::test::trace_init();
    let (svc, handle) = mock_http(HttpParams {
        retry: Some(mk_http_retry()),
        ..Default::default()
    });

    let rsp = retry_canceled(
        svc,
        handle,
        http_get(),
        mk_h2_goaway_canceled(),
        mk_rsp(StatusCode::NO_CONTENT, ""),
    )
    .await;
    assert_eq!(rsp.expect("response").status(), StatusCode::NO_CONTENT);
}

/// A canceled request that the h2 client did not attribute to a peer's GOAWAY
/// is not retried, even though a second response is available.
#[tokio::test(flavor = "current_thread", start_paused = true)]
async fn http_h2_canceled_without_goaway() {
    let _trace = trace::test::trace_init();
    let (svc, handle) = mock_http(HttpParams {
        retry: Some(mk_http_retry()),
        ..Default::default()
    });

    let error = retry_canceled(
        svc,
        handle,
        http_get(),
        mk_h2_canceled_no_goaway(),
        mk_rsp(StatusCode::NO_CONTENT, ""),
    )
    .await
    .expect_err("response should fail");
    let cause = errors::cause_ref::<hyper::Error>(&*error).expect("caused by hyper");
    assert!(cause.is_canceled(), "cause must be canceled: {cause:?}");
}

/// A gRPC request abandoned by a graceful GOAWAY is retried, though no
/// response--and so no gRPC status--was ever received.
#[tokio::test(flavor = "current_thread", start_paused = true)]
async fn grpc_h2_goaway_canceled() {
    let _trace = trace::test::trace_init();
    let (svc, handle) = mock_grpc(GrpcParams {
        retry: Some(client_policy::grpc::Retry {
            max_retries: 1,
            codes: Codes(Default::default()),
            max_request_bytes: 1000,
            timeout: None,
            backoff: None,
        }),
        ..Default::default()
    });

    let rsp = retry_canceled(
        svc,
        handle,
        http::Request::post("/svc/method")
            .body(Default::default())
            .unwrap(),
        mk_h2_goaway_canceled(),
        mk_grpc_rsp(tonic::Code::Ok),
    )
    .await;
    assert_eq!(rsp.expect("response").status(), StatusCode::OK);
}

#[tokio::test(flavor = "current_thread", start_paused = true)]
async fn grpc_internal() {
    let _trace = trace::test::with_default_filter("linkerd=debug");

    const TIMEOUT: time::Duration = time::Duration::from_millis(100);
    let (svc, mut handle) = mock_grpc(GrpcParams {
        timeouts: Timeouts {
            request: Some(TIMEOUT),
            ..Default::default()
        },
        retry: Some(client_policy::grpc::Retry {
            max_retries: 1,
            codes: Codes(
                Some(Code::Internal as u16)
                    .into_iter()
                    .collect::<BTreeSet<_>>()
                    .into(),
            ),
            max_request_bytes: 1000,
            timeout: None,
            backoff: None,
        }),
        ..Default::default()
    });

    info!("Sending a request that will initially fail and then succeed");
    tokio::spawn(
        async move {
            handle.allow(2);
            info!("Failing the first request");
            serve(&mut handle, mk_grpc_rsp(tonic::Code::Internal)).await;
            info!("Serving the second request");
            serve(&mut handle, mk_grpc_rsp(tonic::Code::Ok)).await;
            handle
        }
        .in_current_span(),
    );

    info!("Verifying that we see the successful response");
    let (parts, mut body) = time::timeout(
        TIMEOUT * 10,
        send_req(
            svc.clone(),
            http::Request::post("/svc/method")
                .body(Default::default())
                .unwrap(),
        ),
    )
    .await
    .expect("response")
    .expect("response ok")
    .into_parts();
    assert_eq!(parts.status, StatusCode::OK);
    let trailers = loop {
        match body.frame().await {
            Some(Ok(frame)) => {
                if let Ok(trailers) = frame.into_trailers() {
                    break trailers;
                } else {
                    continue;
                }
            }
            None | Some(Err(_)) => panic!("body did not yield trailers"),
        }
    };
    assert_eq!(
        trailers
            .get("grpc-status")
            .expect("grpc-status")
            .to_str()
            .unwrap(),
        "0"
    );
}

#[tokio::test(flavor = "current_thread", start_paused = true)]
async fn grpc_timeout() {
    let _trace = trace::test::with_default_filter("linkerd=debug");

    const TIMEOUT: time::Duration = time::Duration::from_millis(100);
    let (svc, mut handle) = mock_grpc(GrpcParams {
        timeouts: Timeouts {
            request: Some(TIMEOUT * 5),
            ..Default::default()
        },
        retry: Some(client_policy::grpc::Retry {
            max_retries: 1,
            timeout: Some(TIMEOUT),
            codes: Codes(Default::default()),
            max_request_bytes: 1000,
            backoff: None,
        }),
        ..Default::default()
    });

    info!("Sending a request that will initially fail and then succeed");
    tokio::spawn(
        async move {
            handle.allow(2);
            info!("Delaying the first request");
            serve(&mut handle, async move {
                time::sleep(TIMEOUT * 2).await;
                mk_grpc_rsp(tonic::Code::NotFound).await
            })
            .await;
            info!("Serving the second request");
            serve(&mut handle, mk_grpc_rsp(tonic::Code::Ok)).await;
            handle
        }
        .in_current_span(),
    );

    info!("Verifying that we see the successful response");
    let (parts, mut body) = time::timeout(
        TIMEOUT * 10,
        send_req(
            svc.clone(),
            http::Request::post("/svc/method")
                .body(Default::default())
                .unwrap(),
        ),
    )
    .await
    .expect("response")
    .expect("response ok")
    .into_parts();
    assert_eq!(parts.status, StatusCode::OK);
    let trailers = loop {
        match body.frame().await {
            Some(Ok(frame)) => {
                if let Ok(trailers) = frame.into_trailers() {
                    break trailers;
                } else {
                    continue;
                }
            }
            None | Some(Err(_)) => panic!("body did not yield trailers"),
        }
    };
    assert_eq!(
        trailers
            .get("grpc-status")
            .expect("grpc-status")
            .to_str()
            .unwrap(),
        "0"
    );
}

// === Utils ===

fn mk_http_retry() -> client_policy::http::Retry {
    client_policy::http::Retry {
        max_retries: 1,
        status_ranges: Default::default(),
        max_request_bytes: 1000,
        timeout: None,
        backoff: None,
    }
}

/// Fails the first request with `error` and makes `retried` available to a
/// second, so that the response observed by the caller distinguishes a request
/// that was retried from one that was not.
async fn retry_canceled(
    svc: svc::BoxCloneHttp,
    mut handle: Handle,
    req: ::http::Request<BoxBody>,
    error: impl Future<Output = Error> + Send + 'static,
    retried: impl Future<Output = Result<Response>> + Send + 'static,
) -> Result<Response> {
    const TIMEOUT: time::Duration = time::Duration::from_secs(2);

    tokio::spawn(
        async move {
            handle.allow(2);
            info!("Failing the first request with a canceled dispatch");
            serve(&mut handle, async move { Err(error.await) }).await;
            info!("Serving the second request");
            serve(&mut handle, retried).await;
            handle
        }
        .in_current_span(),
    );

    time::timeout(TIMEOUT, send_req(svc, req))
        .await
        .expect("response")
}

type H2ClientConn = hyper::client::conn::http2::Connection<
    hyper_util::rt::TokioIo<io::DuplexStream>,
    BoxBody,
    http::TokioExecutor,
>;

async fn mk_h2_client(
    io: io::DuplexStream,
) -> (
    hyper::client::conn::http2::SendRequest<BoxBody>,
    H2ClientConn,
) {
    hyper::client::conn::http2::Builder::new(http::TokioExecutor::new())
        .handshake::<_, BoxBody>(hyper_util::rt::TokioIo::new(io))
        .await
        .expect("client handshake must succeed")
}

/// Builds the error hyper produces when a request is queued onto a connection
/// that the server terminates with a graceful GOAWAY before the request can be
/// written to it, as a grpc-go server does when it enforces `MaxConnectionAge`.
async fn mk_h2_goaway_canceled() -> Error {
    let (client_io, server_io) = io::duplex(64 * 1024);
    let (mut tx, conn) = mk_h2_client(client_io).await;

    // Queue a request; this only places it on hyper's dispatch channel. The
    // dispatcher (`conn`) is not polled until after the server has shut down,
    // so the request provably never reaches the connection.
    let fut = tx.send_request(http_post());
    drop(tx);

    // Run a real server through the same two-phase graceful shutdown that
    // grpc-go's MaxConnectionAge enforcement performs: GOAWAY(2^31-1,
    // NO_ERROR), a shutdown PING, then GOAWAY(last-stream-id, NO_ERROR).
    // hyper's connection task (spawned by the handshake above) answers the
    // PING, so awaiting the server task guarantees that the whole frame
    // exchange completed on the wire.
    tokio::spawn(
        async move {
            let mut srv = h2::server::handshake(server_io)
                .await
                .expect("server handshake must succeed");
            srv.graceful_shutdown();
            while let Some(Ok(_)) = srv.accept().await {}
        }
        .in_current_span(),
    )
    .await
    .expect("server task must not panic");

    // Once polled, the dispatcher observes the GOAWAY, shuts down cleanly
    // (NO_ERROR), and abandons the queued request.
    let (conn, res) = tokio::join!(conn, fut);
    conn.expect("dispatcher must shut down cleanly on a NO_ERROR GOAWAY");
    let err = res.expect_err("request must be canceled");
    assert!(err.is_canceled(), "error must be canceled: {err:?}");
    assert!(
        errors::cause_ref::<errors::H2Error>(&err).is_none(),
        "error must not carry an h2 error: {err:?}"
    );
    // The h2 client's connection task observes the clean shutdown and marks
    // the cancelation as caused by the peer's GOAWAY.
    http::h2::GoAwayCanceled::new(err).into()
}

/// Builds the error hyper produces when a request is queued onto a connection
/// whose dispatcher goes away without a peer GOAWAY, e.g. because the
/// connection failed with an I/O error. hyper cancels the request exactly as
/// in the GOAWAY case, so nothing distinguishes the two at this layer; the
/// h2 client's connection task only marks the GOAWAY case.
async fn mk_h2_canceled_no_goaway() -> Error {
    let (client_io, _server_io) = io::duplex(64 * 1024);
    let (mut tx, conn) = mk_h2_client(client_io).await;

    // Queue a request, then drop the dispatcher with the request still
    // queued: it is canceled without ever reaching the connection.
    let fut = tx.send_request(http_post());
    drop(tx);
    drop(conn);

    let err = fut.await.expect_err("request must be canceled");
    assert!(err.is_canceled(), "error must be canceled: {err:?}");
    err.into()
}
