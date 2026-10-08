// =============================================================================
//    Copyright (c) 2025 - 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================
//! A transport-only prompt workflow: HTTP, bounded SSE progress, and a WS echo.

use std::convert::Infallible;
use std::net::SocketAddr;
use std::pin::Pin;
use std::sync::Arc;
use std::sync::Mutex;
use std::task::Context;
use std::task::Poll;
use std::time::Duration;

use axum::Router;
use axum::extract::State;
use axum::extract::WebSocketUpgrade;
use axum::extract::ws::Message;
use axum::http::HeaderMap;
use axum::http::StatusCode;
use axum::http::header;
use axum::response::IntoResponse;
use axum::response::Response;
use axum::response::sse::Event;
use axum::routing::get;
use axum::routing::post;
use futures_core::Stream;
use qubit_web::BoundedJson;
use qubit_web::RequestLimitLayer;
use qubit_web::ServerContext;
use qubit_web::ServerOptions;
use qubit_web::SseConnectionPolicy;
use qubit_web::WebServer;
use qubit_web::WsUpgradePolicy;
use qubit_web::json_response;
use serde::Deserialize;
use serde::Serialize;
use tokio::sync::mpsc;
use tokio::sync::oneshot;

#[derive(Clone)]
struct AppState {
    context: ServerContext,
    sse: SseConnectionPolicy,
    ws: WsUpgradePolicy,
    shutdown: Arc<Mutex<Option<oneshot::Sender<()>>>>,
}

#[derive(Deserialize)]
struct PromptInput {
    prompt: String,
}

#[derive(Serialize)]
struct Accepted<'a> {
    job_id: &'a str,
    prompt: &'a str,
}

async fn submit(BoundedJson(input): BoundedJson<PromptInput>) -> Response {
    // This endpoint only demonstrates bounded JSON transport; it does not run an
    // agent.
    json_response(
        &Accepted {
            job_id: "demo-1",
            prompt: &input.prompt,
        },
        &Default::default(),
    )
    .unwrap_or_else(|_| StatusCode::INTERNAL_SERVER_ERROR.into_response())
}

struct EventReceiver(mpsc::Receiver<Event>);

impl Stream for EventReceiver {
    type Item = Result<Event, Infallible>;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        self.0.poll_recv(cx).map(|event| event.map(Ok))
    }
}

async fn progress(State(state): State<AppState>) -> Response {
    let connection = match state.sse.begin(&state.context) {
        Ok(connection) => connection,
        Err(error) => return error.into_response(),
    };
    let cancellation = connection.cancellation_token();
    let (sender, receiver) = mpsc::channel(4);
    tokio::spawn(async move {
        for event in ["queued", "working", "complete"] {
            tokio::select! {
                biased;
                _ = cancellation.cancelled() => break,
                result = sender.send(Event::default().event("progress").data(event)) => {
                    if result.is_err() { break; }
                }
            }
            tokio::time::sleep(Duration::from_millis(500)).await;
        }
    });
    connection.into_sse(EventReceiver(receiver)).into_response()
}

async fn websocket(State(state): State<AppState>, headers: HeaderMap, ws: WebSocketUpgrade) -> Response {
    // Demonstration-only authentication. Replace with application middleware and
    // real identity checks.
    if headers.get(header::AUTHORIZATION).and_then(|value| value.to_str().ok()) != Some("Bearer demo-only") {
        return StatusCode::UNAUTHORIZED.into_response();
    }
    let policy = state.ws.clone();
    policy.on_upgrade_with_context(ws, &headers, state.context.clone(), |mut session| async move {
        while let Some(Ok(message)) = session.recv().await {
            match message {
                Message::Text(text) => {
                    if session.try_send(Message::text(format!("demo echo: {text}"))).is_err() {
                        break;
                    }
                }
                Message::Close(_) => break,
                _ => {}
            }
        }
    })
}

async fn shutdown(State(state): State<AppState>) -> StatusCode {
    match state.shutdown.lock().expect("shutdown lock").take() {
        Some(sender) => {
            let _ = sender.send(());
            StatusCode::ACCEPTED
        }
        None => StatusCode::GONE,
    }
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let options = ServerOptions::new("127.0.0.1:3001".parse::<SocketAddr>()?);
    let http_limits = options.http_limits();
    let server = WebServer::bind_http(options.clone()).await?;
    let context = server.context();
    let (shutdown_tx, shutdown_rx) = oneshot::channel();
    let state = AppState {
        context,
        sse: SseConnectionPolicy::default(),
        ws: WsUpgradePolicy::new().allowed_origins(["http://localhost:3000"]),
        shutdown: Arc::new(Mutex::new(Some(shutdown_tx))),
    };
    let app = Router::new()
        .route("/prompt", post(submit).layer(RequestLimitLayer::new(http_limits)))
        .route("/prompt/demo-1/events", get(progress))
        .route("/prompt/demo-1/ws", get(websocket))
        .route(
            "/admin/shutdown",
            post(shutdown).layer(RequestLimitLayer::new(http_limits)),
        )
        .with_state(state);
    println!(
        "listening on http://{}; POST /admin/shutdown to stop",
        server.local_addr()
    );
    server
        .serve(app, async move {
            let _ = shutdown_rx.await;
        })
        .await?;
    Ok(())
}
