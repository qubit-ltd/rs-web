// =============================================================================
//    Copyright (c) 2025 - 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================
//! A small in-memory CRUD consumer using Controller routes and native Axum
//! routes.

use std::collections::HashMap;
use std::net::SocketAddr;
use std::sync::Arc;
use std::sync::Mutex;
use std::sync::atomic::AtomicU64;
use std::sync::atomic::Ordering;

use axum::Router;
use axum::extract::Path;
use axum::extract::Query;
use axum::http::StatusCode;
use axum::http::header;
use axum::response::IntoResponse;
use axum::response::Response;
use axum::routing::get;
use axum::routing::post;
use qubit_web::BoundedJson;
use qubit_web::ControllerRoutes;
use qubit_web::HttpLimits;
use qubit_web::ServerOptions;
use qubit_web::WebServer;
use qubit_web::json_response;
use qubit_web::rest_controller;
use serde::Deserialize;
use serde::Serialize;
use tokio::sync::oneshot;

#[derive(Clone, Debug, Deserialize, Serialize)]
struct User {
    id: u64,
    name: String,
}

#[derive(Deserialize)]
struct NewUser {
    name: String,
}

#[derive(Default)]
struct Users {
    rows: Mutex<HashMap<u64, User>>,
    next_id: AtomicU64,
}

#[rest_controller("/users")]
impl Users {
    #[get_mapping("")]
    async fn list(&self, Query(query): Query<HashMap<String, String>>) -> Response {
        let mut users: Vec<_> = self.rows.lock().expect("users lock").values().cloned().collect();
        if let Some(prefix) = query.get("name_prefix") {
            users.retain(|user| user.name.starts_with(prefix));
        }
        json_response(&users, &Default::default()).unwrap_or_else(|_| StatusCode::INTERNAL_SERVER_ERROR.into_response())
    }

    #[get_mapping("/{id}")]
    async fn get(&self, Path(id): Path<u64>) -> Response {
        match self.rows.lock().expect("users lock").get(&id).cloned() {
            Some(user) => json_response(&user, &Default::default())
                .unwrap_or_else(|_| StatusCode::INTERNAL_SERVER_ERROR.into_response()),
            None => StatusCode::NOT_FOUND.into_response(),
        }
    }

    #[post_mapping("")]
    async fn create(&self, BoundedJson(input): BoundedJson<NewUser>) -> Result<Response, StatusCode> {
        let id = self.next_id.fetch_add(1, Ordering::Relaxed) + 1;
        let user = User { id, name: input.name };
        self.rows
            .lock()
            .map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?
            .insert(id, user.clone());
        let mut response = json_response(&user, &Default::default()).map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?;
        *response.status_mut() = StatusCode::CREATED;
        response
            .headers_mut()
            .insert(header::LOCATION, format!("/users/{id}").parse().unwrap());
        Ok(response)
    }
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let options = ServerOptions::new("127.0.0.1:3000".parse::<SocketAddr>()?).with_max_transport_connections(128)?;
    let http_limits = HttpLimits::default();
    let users = Arc::new(Users::default());
    let controller = ControllerRoutes::new()
        .with_http_limits(http_limits)
        .add(users)?
        .finish();
    let (shutdown_tx, shutdown_rx) = oneshot::channel::<()>();
    let shutdown_tx = Arc::new(Mutex::new(Some(shutdown_tx)));
    let app = Router::new()
        .route("/health", get(|| async { "ok" }))
        .route(
            "/admin/shutdown",
            post({
                let shutdown_tx = shutdown_tx.clone();
                move || {
                    let shutdown_tx = shutdown_tx.clone();
                    async move {
                        if let Some(sender) = shutdown_tx.lock().expect("shutdown lock").take() {
                            let _ = sender.send(());
                            StatusCode::ACCEPTED
                        } else {
                            StatusCode::GONE
                        }
                    }
                }
            }),
        )
        .merge(controller);

    let server = WebServer::bind_http(options).await?;
    println!(
        "listening on http://{}; POST /admin/shutdown to stop",
        server.local_addr()
    );
    let _ = server
        .serve(app, async move {
            let _ = shutdown_rx.await;
        })
        .await?;
    Ok(())
}
