use super::*;
use http_body_util::BodyExt;
use linkerd_app_core::{
    errors, io,
    proxy::http::{self, StatusCode},
    svc::{http::stream_timeouts::StreamDeadlineError, Service},
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
        mk_h2_unwritten(true).await,
        mk_rsp(StatusCode::NO_CONTENT, ""),
    )
    .await;
    assert_eq!(rsp.expect("response").status(), StatusCode::NO_CONTENT);
}

/// A raw canceled error carries no proof that the request was not written.
/// The retry layer must not retry it without the native client's refusal.
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
        mk_h2_canceled_no_goaway().await,
        mk_rsp(StatusCode::NO_CONTENT, ""),
    )
    .await
    .expect_err("response should fail");
    let cause = errors::cause_ref::<hyper::Error>(&*error).expect("caused by hyper");
    assert!(cause.is_canceled(), "cause must be canceled: {cause:?}");
}

/// A native HTTP/2 client also refuses a request recovered after an I/O
/// failure without GOAWAY. The same retry policy can retry this unwritten POST.
#[tokio::test(flavor = "current_thread", start_paused = true)]
async fn http_h2_unwritten_without_goaway() {
    let _trace = trace::test::trace_init();
    let (svc, handle) = mock_http(HttpParams {
        retry: Some(mk_http_retry()),
        ..Default::default()
    });

    let rsp = retry_canceled(
        svc,
        handle,
        http_post(),
        mk_h2_unwritten(false).await,
        mk_rsp(StatusCode::NO_CONTENT, ""),
    )
    .await;
    assert_eq!(rsp.expect("response").status(), StatusCode::NO_CONTENT);
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
        mk_h2_unwritten(true).await,
        mk_grpc_rsp(tonic::Code::Ok),
    )
    .await;
    let rsp = rsp.expect("response");
    assert_eq!(rsp.status(), StatusCode::OK);
    let body = rsp.into_body().collect().await.expect("response body");
    assert_eq!(body.trailers().expect("gRPC trailers")["grpc-status"], "0");
}

#[tokio::test(flavor = "current_thread", start_paused = true)]
async fn http_h2_goaway_canceled_respects_policy() {
    for (retry, body) in [
        (None, "".to_owned()),
        (
            Some(client_policy::http::Retry {
                max_retries: 0,
                ..mk_http_retry()
            }),
            "".to_owned(),
        ),
        (Some(mk_http_retry()), "x".repeat(1001)),
    ] {
        let (svc, handle) = mock_http(HttpParams {
            retry,
            ..Default::default()
        });
        let req = http::Request::post("/").body(BoxBody::new(body)).unwrap();
        let error = retry_canceled(
            svc,
            handle,
            req,
            mk_h2_unwritten(true).await,
            mk_rsp(StatusCode::NO_CONTENT, ""),
        )
        .await
        .expect_err("policy must prevent retry");
        let h2 = errors::cause_ref::<http::h2::H2Error>(&*error).expect("HTTP/2 error");
        assert_eq!(h2.reason(), Some(http::h2::Reason::REFUSED_STREAM));
    }
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
    error: Error,
    retried: impl Future<Output = Result<Response>> + Send + 'static,
) -> Result<Response> {
    const TIMEOUT: time::Duration = time::Duration::from_secs(2);

    tokio::spawn(
        async move {
            handle.allow(2);
            info!("Failing the first request with a canceled dispatch");
            serve(&mut handle, async move { Err(error) }).await;
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

/// Builds the native client's refusal when the peer closes a connection
/// before the queued request can be written, with or without graceful GOAWAY.
async fn mk_h2_unwritten(goaway: bool) -> Error {
    let (client_io, server_io) = io::duplex(64 * 1024);
    let client_io = Arc::new(Mutex::new(Some(client_io)));
    let connect = svc::service_fn(move |_: (http::Variant, ())| {
        futures::future::ok::<_, Error>((client_io.lock().take().expect("only one connection"), ()))
    });
    let (shutdown, wait) = tokio::sync::oneshot::channel::<()>();
    let server = tokio::spawn(async move {
        let mut srv = h2::server::Builder::new()
            .max_concurrent_streams(0)
            .handshake::<_, bytes::Bytes>(server_io)
            .await
            .expect("server handshake must succeed");
        tokio::select! {
            accepted = srv.accept() => panic!("server must not accept a stream: {accepted:?}"),
            _ = wait => {}
        }
        if !goaway {
            return;
        }
        srv.graceful_shutdown();
        match srv.accept().await {
            Some(Ok(_)) => panic!("server must not accept a stream during shutdown"),
            Some(Err(error)) => assert!(error
                .get_io()
                .is_some_and(|e| e.kind() == std::io::ErrorKind::BrokenPipe)),
            None => {}
        }
    });
    let mut client = svc::stack(connect)
        .push(http::client::layer_via(|_: &()| {
            http::client::Params::H2(http::h2::ClientParams::default())
        }))
        .into_inner()
        .oneshot(())
        .await
        .expect("client must connect");
    let mut req = http_post();
    *req.version_mut() = ::http::Version::HTTP_2;
    let rsp = client
        .ready()
        .await
        .expect("client must be ready")
        .call(req);
    shutdown.send(()).expect("server must await shutdown");
    server.await.expect("server task must not panic");

    let err = rsp.await.expect_err("request must be canceled");
    let h2 = errors::cause_ref::<http::h2::H2Error>(&*err).expect("HTTP/2 error");
    assert_eq!(h2.reason(), Some(http::h2::Reason::REFUSED_STREAM));
    err
}

/// Builds the error hyper produces when a request is queued onto a connection
/// whose dispatcher is dropped. This bypasses the native client's request
/// recovery so the retry layer receives the original, unclassified error.
async fn mk_h2_canceled_no_goaway() -> Error {
    let (client_io, _server_io) = io::duplex(64 * 1024);
    let (mut tx, conn) = hyper::client::conn::http2::Builder::new(http::TokioExecutor::new())
        .handshake::<_, BoxBody>(hyper_util::rt::TokioIo::new(client_io))
        .await
        .expect("client handshake must succeed");

    // Queue a request, then drop the dispatcher with the request still
    // queued: it is canceled without ever reaching the connection.
    let fut = tx.send_request(http_post());
    drop(tx);
    drop(conn);

    let err = fut.await.expect_err("request must be canceled");
    assert!(err.is_canceled(), "error must be canceled: {err:?}");
    err.into()
}
