use std::sync::{
    Arc, Mutex,
    atomic::{AtomicUsize, Ordering},
};

use bytes::Bytes;
use futures_util::StreamExt;
use http::{
    Request, Response, StatusCode,
    header::{RANGE, USER_AGENT},
};
use http_body_util::{BodyExt, Full, StreamBody};
use hyper::body::Frame;
use librespot_core::{
    Error, Session, SessionConfig,
    authentication::Credentials,
    error::ErrorKind,
    http_client::{HttpResponseBody, HttpTransport},
    session::UserAttributes,
};

const NATIVE_UA: &str = "Spotify/1.2.99.1234 OSX/0 (exact synthetic native fixture)";

fn response(body: &'static [u8]) -> Response<HttpResponseBody> {
    Response::new(
        Full::new(Bytes::from_static(body))
            .map_err(|never| match never {})
            .boxed_unsync(),
    )
}

fn enabled_config(transport: HttpTransport) -> SessionConfig {
    SessionConfig {
        network_enabled: true,
        http_user_agent: Some(NATIVE_UA.to_owned()),
        http_transport: Some(transport),
        client_id: "explicit-client".to_owned(),
        device_id: "explicit-device".to_owned(),
        ..SessionConfig::default()
    }
}

#[tokio::test]
async fn free_product_attributes_remain_unmodified_in_the_embedded_session() {
    let session = Session::new(SessionConfig::default(), None);
    session.set_user_attributes(UserAttributes::from([
        ("type".to_owned(), "free".to_owned()),
        ("catalogue".to_owned(), "free".to_owned()),
        ("ads".to_owned(), "1".to_owned()),
    ]));
    assert_eq!(session.get_user_attribute("type").as_deref(), Some("free"));
    assert_eq!(
        session.get_user_attribute("catalogue").as_deref(),
        Some("free")
    );
    assert_eq!(session.get_user_attribute("ads").as_deref(), Some("1"));
    assert_eq!(
        session.set_user_attribute("type", "open").as_deref(),
        Some("free")
    );
    assert_eq!(session.get_user_attribute("type").as_deref(), Some("open"));
}

#[tokio::test]
async fn disabled_sessions_reject_http_and_ap_dealer_entry_points_before_dispatch() {
    let dispatched = Arc::new(AtomicUsize::new(0));
    let calls = dispatched.clone();
    let transport = HttpTransport::new(move |_| {
        calls.fetch_add(1, Ordering::SeqCst);
        async { Ok(response(b"must not dispatch")) }
    });
    let mut config = enabled_config(transport);
    config.network_enabled = false;
    let session = Session::new(config, None);
    let request = || {
        Request::builder()
            .uri("http://127.0.0.1:1/synthetic")
            .body(Bytes::new())
            .unwrap()
    };
    let failures = [
        session
            .connect(Credentials::with_access_token("synthetic"), false)
            .await
            .unwrap_err(),
        session
            .apresolver()
            .resolve("accesspoint")
            .await
            .unwrap_err(),
        session.dealer().start().await.unwrap_err(),
        session
            .http_client()
            .request_body(request())
            .await
            .unwrap_err(),
        session.http_client().request_stream(request()).unwrap_err(),
    ];
    for failure in failures {
        assert_eq!(failure.kind, ErrorKind::FailedPrecondition);
        assert!(
            failure.to_string().contains("networking is disabled"),
            "{failure}"
        );
    }
    assert_eq!(dispatched.load(Ordering::SeqCst), 0);
    assert_eq!(
        SessionConfig::default()
            .check_network_permission()
            .unwrap_err()
            .kind,
        ErrorKind::FailedPrecondition
    );
}

#[tokio::test]
async fn delegated_resolution_failure_cannot_produce_a_fallback_socket_address() {
    let dispatched = Arc::new(AtomicUsize::new(0));
    let calls = dispatched.clone();
    let session = Session::new(
        enabled_config(HttpTransport::new(move |_| {
            calls.fetch_add(1, Ordering::SeqCst);
            async { Err(Error::permission_denied("synthetic Dart transport denial")) }
        })),
        None,
    );

    // This is deliberately checked before Session::connect: the unfixed resolver
    // returns a static remote address here, so its regression never opens a socket.
    let failure = session
        .apresolver()
        .resolve("accesspoint")
        .await
        .unwrap_err();
    assert_eq!(failure.kind, ErrorKind::PermissionDenied);
    assert!(
        failure
            .to_string()
            .contains("synthetic Dart transport denial")
    );
    assert_eq!(dispatched.load(Ordering::SeqCst), 1);
    // Only reach the full connection boundary after the no-address assertion.
    let failure = session
        .connect(Credentials::with_access_token("synthetic"), false)
        .await
        .unwrap_err();
    assert_eq!(failure.kind, ErrorKind::PermissionDenied);
    assert!(
        failure
            .to_string()
            .contains("synthetic Dart transport denial")
    );
    assert_eq!(dispatched.load(Ordering::SeqCst), 2);

    for (mode, expected) in [
        ("http", ErrorKind::PermissionDenied),
        ("json", ErrorKind::FailedPrecondition),
        ("empty", ErrorKind::Unavailable),
    ] {
        let session = Session::new(
            enabled_config(HttpTransport::new(move |_| async move {
                Ok(match mode {
                    "http" => {
                        let mut denied = response(b"denied");
                        *denied.status_mut() = StatusCode::FORBIDDEN;
                        denied
                    }
                    "json" => response(b"invalid synthetic JSON"),
                    _ => response(br#"{"accesspoint":[],"dealer":[],"spclient":[]}"#),
                })
            })),
            None,
        );
        for endpoint in ["accesspoint", "dealer", "spclient"] {
            assert_eq!(
                session
                    .apresolver()
                    .resolve(endpoint)
                    .await
                    .unwrap_err()
                    .kind,
                expected,
                "{mode}/{endpoint}"
            );
        }
    }
}

#[tokio::test]
async fn http_requires_explicit_transport_and_exact_identity_without_fallback_or_overwrite() {
    let dispatched = Arc::new(AtomicUsize::new(0));
    let calls = dispatched.clone();
    let config = enabled_config(HttpTransport::new(move |_| {
        calls.fetch_add(1, Ordering::SeqCst);
        async { Ok(response(b"must not dispatch")) }
    }));
    for missing in [
        "adapter",
        "user-agent",
        "client",
        "device",
        "invalid-user-agent",
    ] {
        let mut incomplete = config.clone();
        match missing {
            "adapter" => incomplete.http_transport = None,
            "user-agent" => incomplete.http_user_agent = None,
            "client" => incomplete.client_id.clear(),
            "device" => incomplete.device_id.clear(),
            _ => incomplete.http_user_agent = Some("invalid\r\nInjected: value".to_owned()),
        }
        let session = Session::new(incomplete, None);
        assert!(
            session
                .connect(Credentials::with_access_token("synthetic"), false)
                .await
                .is_err(),
            "{missing}"
        );
        assert!(
            session
                .http_client()
                .request_body(Request::new(Bytes::new()))
                .await
                .is_err(),
            "{missing}"
        );
    }
    let session = Session::new(config, None);
    let request = Request::builder()
        .uri("http://127.0.0.1:1/synthetic")
        .header(USER_AGENT, "wrong synthetic identity")
        .body(Bytes::new())
        .unwrap();
    let failure = session
        .http_client()
        .request_body(request)
        .await
        .unwrap_err();
    assert_eq!(failure.kind, ErrorKind::FailedPrecondition);
    assert!(failure.to_string().contains("does not match"));
    assert_eq!(dispatched.load(Ordering::SeqCst), 0);
}

#[tokio::test]
async fn delegated_http_preserves_request_data_and_streams_cdn_frames_and_failures() {
    let requests = Arc::new(Mutex::new(Vec::new()));
    let captured = requests.clone();
    let transport = HttpTransport::new(move |request: Request<Bytes>| {
        let streaming = request.uri().path() == "/audio";
        let denied = request.uri().path() == "/denied";
        captured.lock().unwrap().push(request);
        async move {
            if streaming {
                let frames = futures_util::stream::iter([
                    Ok::<_, Error>(Frame::data(Bytes::from_static(b"first chunk"))),
                    Err(Error::data_loss("synthetic body interruption")),
                ]);
                Ok(Response::builder()
                    .status(StatusCode::PARTIAL_CONTENT)
                    .body(StreamBody::new(frames).boxed_unsync())
                    .unwrap())
            } else if denied {
                let mut response = response(b"denied");
                *response.status_mut() = StatusCode::FORBIDDEN;
                Ok(response)
            } else {
                Ok(response(b"\x00\xffresponse"))
            }
        }
    });
    let session = Session::new(enabled_config(transport), None);
    let request = Request::builder()
        .method("POST")
        .uri("http://127.0.0.1:1/synthetic?q=%2F+%20&q=second")
        .header("x-synthetic", "first")
        .header("x-synthetic", "second")
        .header(USER_AGENT, NATIVE_UA)
        .body(Bytes::from_static(b"\x00\xffrequest"))
        .unwrap();
    assert_eq!(
        session
            .http_client()
            .request_body(request)
            .await
            .unwrap()
            .as_ref(),
        b"\x00\xffresponse"
    );
    let mut stream = session
        .spclient()
        .stream_from_cdn("http://127.0.0.1:1/audio", 64, 1024)
        .unwrap();
    let response = stream.next().await.unwrap().unwrap();
    assert_eq!(response.status(), StatusCode::PARTIAL_CONTENT);
    let mut body = response.into_body();
    assert_eq!(
        body.frame()
            .await
            .unwrap()
            .unwrap()
            .into_data()
            .unwrap()
            .as_ref(),
        b"first chunk"
    );
    let failure = body.frame().await.unwrap().unwrap_err();
    assert_eq!(failure.kind, ErrorKind::DataLoss);
    let denied = Request::builder()
        .uri("http://127.0.0.1:1/denied")
        .body(Bytes::new())
        .unwrap();
    assert_eq!(
        session
            .http_client()
            .request_body(denied)
            .await
            .unwrap_err()
            .kind,
        ErrorKind::PermissionDenied
    );
    let requests = requests.lock().unwrap();
    assert_eq!(requests.len(), 3);
    assert_eq!(requests[0].method(), "POST");
    assert_eq!(
        requests[0].uri().to_string(),
        "http://127.0.0.1:1/synthetic?q=%2F+%20&q=second"
    );
    assert_eq!(
        requests[0]
            .headers()
            .get_all("x-synthetic")
            .iter()
            .map(|v| v.as_bytes())
            .collect::<Vec<_>>(),
        [b"first".as_slice(), b"second".as_slice()]
    );
    assert_eq!(requests[0].body().as_ref(), b"\x00\xffrequest");
    assert!(
        requests
            .iter()
            .all(|request| request.headers()[USER_AGENT].as_bytes() == NATIVE_UA.as_bytes())
    );
    assert_eq!(requests[1].headers()[RANGE], "bytes=64-1087");
}
