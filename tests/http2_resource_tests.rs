use std::io;
use std::net::SocketAddr;
use std::time::Duration;

use axum::Router;
use axum::response::sse::{Event, Sse};
use axum::routing::get;
use futures_util::stream;
use qubit_web::{ServerOptions, WebServer};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpStream, tcp::OwnedReadHalf, tcp::OwnedWriteHalf};
use tokio::sync::oneshot;
use tokio::task::JoinHandle;
use tokio::time;

type ServingTask = JoinHandle<Result<qubit_web::ShutdownReport, qubit_web::WebServerError>>;

const PREFACE: &[u8] = b"PRI * HTTP/2.0\r\n\r\nSM\r\n\r\n";

async fn start_server() -> (SocketAddr, oneshot::Sender<()>, ServingTask) {
    let app = Router::new()
        .route(
            "/events",
            get(|| async {
                let events = stream::unfold(0_u64, |index| async move {
                    time::sleep(Duration::from_millis(80)).await;
                    Some((
                        Ok::<_, std::convert::Infallible>(Event::default().data(format!("tick-{index}"))),
                        index + 1,
                    ))
                });
                Sse::new(events)
            }),
        )
        .route("/health", get(|| async { "ok" }));
    let server = WebServer::bind_http(
        ServerOptions::new("127.0.0.1:0".parse().unwrap())
            .with_max_transport_connections(1)
            .unwrap(),
    )
    .await
    .expect("bind test server");
    let address = server.local_addr();
    let (shutdown_tx, shutdown_rx) = oneshot::channel();
    let serving = tokio::spawn(server.serve(app, async move {
        let _ = shutdown_rx.await;
    }));
    (address, shutdown_tx, serving)
}

fn frame(kind: u8, flags: u8, stream_id: u32, payload: &[u8]) -> Vec<u8> {
    let len = payload.len();
    let mut bytes = vec![
        ((len >> 16) & 0xff) as u8,
        ((len >> 8) & 0xff) as u8,
        (len & 0xff) as u8,
        kind,
        flags,
    ];
    bytes.extend_from_slice(&(stream_id & 0x7fff_ffff).to_be_bytes());
    bytes.extend_from_slice(payload);
    bytes
}

async fn read_frame(reader: &mut OwnedReadHalf) -> io::Result<(u8, u8, u32, Vec<u8>)> {
    let mut header = [0; 9];
    reader.read_exact(&mut header).await?;
    let len = (usize::from(header[0]) << 16) | (usize::from(header[1]) << 8) | usize::from(header[2]);
    let stream_id = u32::from_be_bytes([header[5], header[6], header[7], header[8]]) & 0x7fff_ffff;
    let mut payload = vec![0; len];
    reader.read_exact(&mut payload).await?;
    Ok((header[3], header[4], stream_id, payload))
}

async fn send_headers(writer: &mut OwnedWriteHalf, stream_id: u32, end_headers: bool, block: &[u8]) {
    let flags = (if end_headers { 0x04 } else { 0 }) | 0x01; // END_STREAM
    writer.write_all(&frame(0x01, flags, stream_id, block)).await.unwrap();
}

// Valid HPACK block: GET, http, /events, authority=localhost.
const EVENTS_HEADERS: &[u8] = b"\x82\x86\x04\x07/events\x01\x09localhost";
const HEALTH_HEADERS: &[u8] = b"\x82\x86\x84\x01\x09localhost";

async fn begin_h2(address: SocketAddr) -> (OwnedReadHalf, OwnedWriteHalf) {
    let tcp = time::timeout(Duration::from_secs(3), TcpStream::connect(address))
        .await
        .unwrap()
        .expect("connect");
    let (mut reader, mut writer) = tcp.into_split();
    writer.write_all(PREFACE).await.unwrap();
    writer.write_all(&frame(0x04, 0, 0, &[])).await.unwrap(); // SETTINGS
    let settings = time::timeout(Duration::from_secs(3), read_frame(&mut reader))
        .await
        .unwrap()
        .expect("read SETTINGS frame");
    assert_eq!(settings.0, 0x04, "expected server SETTINGS before requests");
    writer.write_all(&frame(0x04, 0x01, 0, &[])).await.unwrap(); // ACK
    (reader, writer)
}

async fn read_until_stream_data(reader: &mut OwnedReadHalf, stream_id: u32) -> bool {
    loop {
        let Ok(Ok((kind, _, id, payload))) = time::timeout(Duration::from_secs(3), read_frame(reader)).await else {
            return false;
        };
        if kind == 0x00 && id == stream_id && !payload.is_empty() {
            return true;
        }
        if kind == 0x03 || kind == 0x07 {
            return false;
        }
    }
}

async fn finish_server(shutdown: oneshot::Sender<()>, serving: ServingTask) {
    let _ = shutdown.send(());
    let report = time::timeout(Duration::from_secs(3), serving)
        .await
        .unwrap()
        .expect("server task")
        .expect("server shutdown");
    assert!(report.graceful);
}

#[tokio::test]
async fn investigates_incomplete_h2_headers_with_active_sse() {
    let (address, shutdown, serving) = start_server().await;
    let (mut reader, mut writer) = begin_h2(address).await;

    // Baseline: complete SSE request headers, then confirm its response advances.
    send_headers(&mut writer, 1, true, EVENTS_HEADERS).await;
    assert!(
        read_until_stream_data(&mut reader, 1).await,
        "SSE should produce data before the next stream"
    );

    // A second complete request is a protocol control and must be served on the same connection.
    send_headers(&mut writer, 3, true, HEALTH_HEADERS).await;
    let mut health_headers_seen = false;
    while !health_headers_seen {
        let (kind, _, id, _) = time::timeout(Duration::from_secs(3), read_frame(&mut reader))
            .await
            .unwrap()
            .expect("read frame");
        health_headers_seen = kind == 0x01 && id == 3;
    }
    assert!(
        read_until_stream_data(&mut reader, 1).await,
        "the first SSE remains active during a complete request"
    );

    // A legal HEADERS frame whose block is explicitly unfinished. Do not send CONTINUATION.
    // The fragment begins with a valid HPACK indexed :method field.
    send_headers(&mut writer, 5, false, b"\x82").await;
    let advanced_while_incomplete = read_until_stream_data(&mut reader, 1).await;
    eprintln!("SSE_DATA_WHILE_SECOND_HEADER_BLOCK_INCOMPLETE={advanced_while_incomplete}");

    // Completing the header block allows the connection parser to resume.
    writer
        .write_all(&frame(0x09, 0x04, 5, b"\x86\x84\x01\x09localhost"))
        .await
        .unwrap();
    let _ = time::timeout(Duration::from_secs(1), read_frame(&mut reader)).await;
    drop(writer);
    drop(reader);

    // Closing the only transport releases its server-side connection permit.
    let mut followup = time::timeout(Duration::from_secs(3), TcpStream::connect(address))
        .await
        .unwrap()
        .expect("transport capacity should be available");
    followup
        .write_all(b"GET /health HTTP/1.1\r\nHost: localhost\r\nConnection: close\r\n\r\n")
        .await
        .unwrap();
    let mut response = Vec::new();
    time::timeout(Duration::from_secs(3), followup.read_to_end(&mut response))
        .await
        .unwrap()
        .expect("follow-up response should finish");
    assert!(
        response.starts_with(b"HTTP/1.1 200"),
        "capacity follow-up failed: {:?}",
        String::from_utf8_lossy(&response)
    );

    finish_server(shutdown, serving).await;
}

#[tokio::test]
async fn incomplete_h2_header_block_on_idle_connection_waits_for_continuation() {
    let (address, shutdown, serving) = start_server().await;
    let (mut reader, mut writer) = begin_h2(address).await;

    // Complete and drain one finite response so no stream remains active.
    send_headers(&mut writer, 1, true, HEALTH_HEADERS).await;
    let mut response_ended = false;
    while !response_ended {
        let (_, flags, id, _) = time::timeout(Duration::from_secs(3), read_frame(&mut reader))
            .await
            .unwrap()
            .expect("read idle-control response frame");
        if id == 1 && flags & 0x01 != 0 {
            response_ended = true;
        }
    }

    // With the connection otherwise idle, an incomplete stream produces no response frame.
    send_headers(&mut writer, 3, false, b"\x82").await;
    let idle_observation = time::timeout(Duration::from_millis(300), read_frame(&mut reader)).await;
    assert!(
        idle_observation.is_err(),
        "an unfinished header block should wait for CONTINUATION"
    );

    // Completing the same valid HPACK header block lets the server produce its response.
    writer
        .write_all(&frame(0x09, 0x04, 3, b"\x86\x84\x01\x09localhost"))
        .await
        .unwrap();
    let mut response_ended = false;
    while !response_ended {
        let (_, flags, id, _) = time::timeout(Duration::from_secs(3), read_frame(&mut reader))
            .await
            .unwrap()
            .expect("read completed idle-control response frame");
        if id == 3 && flags & 0x01 != 0 {
            response_ended = true;
        }
    }

    drop(writer);
    drop(reader);
    finish_server(shutdown, serving).await;
}
