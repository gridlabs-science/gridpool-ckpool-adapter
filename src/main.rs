use anyhow::{Context, Result, bail};
use axum::{Json, Router, extract::State, routing::get};
use clap::Parser;
use eventsource_stream::Eventsource;
use futures_util::StreamExt;
use gridpool_ckpool_adapter::{
    Config, IPC_SCHEMA_VERSION, IpcRequest, IpcResponse, WorkPlan, fee_bucket,
};
use reqwest::header::{HeaderMap, HeaderValue};
use rusqlite::{Connection, params};
use serde_json::{Value, json};
use std::{
    collections::HashMap,
    fs,
    net::SocketAddr,
    os::unix::fs::PermissionsExt,
    path::{Path, PathBuf},
    sync::{Arc, Mutex},
    time::{Duration, SystemTime, UNIX_EPOCH},
};
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::{UnixListener, UnixStream},
    sync::RwLock,
};
use tracing::{error, info, warn};

#[derive(Parser)]
struct Args {
    #[arg(long, default_value = "config/local.toml")]
    config: PathBuf,
}

#[derive(Clone, Copy, Default)]
struct Metrics {
    plan_updates: u64,
    plan_errors: u64,
    ipc_requests: u64,
    queued_proofs: u64,
    submitted_proofs: u64,
    failed_proofs: u64,
    telemetry_batches: u64,
}

struct AppState {
    config: Config,
    client: reqwest::Client,
    token: String,
    fee_secret: Vec<u8>,
    plan: RwLock<Option<WorkPlan>>,
    plan_updated_unix_ms: RwLock<Option<u128>>,
    metrics: Mutex<Metrics>,
    database: Mutex<Connection>,
    telemetry: Mutex<HashMap<String, TelemetryAccumulator>>,
}

#[derive(Clone)]
struct TelemetryAccumulator {
    channel_id: String,
    payout_address: String,
    username: String,
    window_start_unix_ms: i64,
    window_end_unix_ms: i64,
    accepted_shares: u64,
    rejected_shares: u64,
    accepted_difficulty: f64,
    fee_difficulty: f64,
    best_difficulty: f64,
}

#[tokio::main]
async fn main() -> Result<()> {
    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::from_default_env()
                .add_directive("gridpool_ckpool_adapter=info".parse()?),
        )
        .init();
    let args = Args::parse();
    let config = Config::load(&args.config)?;
    let token = fs::read_to_string(&config.adapter_token_file)
        .with_context(|| format!("read adapter token {}", config.adapter_token_file))?
        .trim()
        .to_owned();
    let fee_secret = load_or_create_secret(Path::new(&config.fee_secret_file))?;
    let database = open_database(Path::new(&config.queue_database))?;
    let state = Arc::new(AppState {
        config,
        client: reqwest::Client::builder()
            .timeout(Duration::from_secs(15))
            .build()?,
        token,
        fee_secret,
        plan: RwLock::new(None),
        plan_updated_unix_ms: RwLock::new(None),
        metrics: Mutex::new(Metrics::default()),
        database: Mutex::new(database),
        telemetry: Mutex::new(HashMap::new()),
    });

    let plan_state = state.clone();
    tokio::spawn(async move { plan_loop(plan_state).await });
    let queue_state = state.clone();
    tokio::spawn(async move { proof_queue_loop(queue_state).await });
    let ipc_state = state.clone();
    tokio::spawn(async move {
        if let Err(error) = ipc_loop(ipc_state).await {
            error!(%error, "IPC listener stopped");
        }
    });
    let telemetry_state = state.clone();
    tokio::spawn(async move { telemetry_loop(telemetry_state).await });

    let address: SocketAddr = state
        .config
        .health_listen
        .parse()
        .context("parse health_listen")?;
    let app = Router::new()
        .route("/health", get(health))
        .with_state(state);
    info!(%address, "GridPool CKPool adapter started");
    let listener = tokio::net::TcpListener::bind(address).await?;
    axum::serve(listener, app).await?;
    Ok(())
}

fn load_or_create_secret(path: &Path) -> Result<Vec<u8>> {
    if let Some(parent) = path.parent() {
        fs::create_dir_all(parent)?;
    }
    if path.exists() {
        let raw = fs::read_to_string(path)?;
        let secret = hex::decode(raw.trim()).context("decode fee secret")?;
        if secret.len() < 32 {
            bail!("fee secret must be at least 32 bytes");
        }
        return Ok(secret);
    }
    use std::io::Read;
    let mut secret = vec![0u8; 32];
    fs::File::open("/dev/urandom")?.read_exact(&mut secret)?;
    fs::write(path, format!("{}\n", hex::encode(&secret)))?;
    fs::set_permissions(path, fs::Permissions::from_mode(0o600))?;
    Ok(secret)
}

fn open_database(path: &Path) -> Result<Connection> {
    if let Some(parent) = path.parent() {
        fs::create_dir_all(parent)?;
    }
    let connection = Connection::open(path)?;
    connection.execute_batch(
        "PRAGMA journal_mode=WAL; PRAGMA synchronous=FULL;
        CREATE TABLE IF NOT EXISTS proof_queue (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            payload TEXT NOT NULL,
            attempts INTEGER NOT NULL DEFAULT 0,
            next_attempt_unix INTEGER NOT NULL DEFAULT 0,
            last_error TEXT
        );",
    )?;
    Ok(connection)
}

async fn plan_loop(state: Arc<AppState>) {
    loop {
        if let Err(error) = stream_plans(state.clone()).await {
            warn!(%error, "work-plan stream unavailable; using polling fallback");
            if let Err(poll_error) = fetch_plan(&state).await {
                state.metrics.lock().unwrap().plan_errors += 1;
                warn!(%poll_error, "work-plan polling failed");
            }
            tokio::time::sleep(Duration::from_millis(state.config.poll_interval_ms)).await;
        }
    }
}

fn adapter_headers(state: &AppState) -> Result<HeaderMap> {
    let mut headers = HeaderMap::new();
    headers.insert(
        "X-GridPool-Adapter-Token",
        HeaderValue::from_str(&state.token)?,
    );
    headers.insert(
        "X-GridPool-Adapter-Type",
        HeaderValue::from_static("ckpool"),
    );
    Ok(headers)
}

async fn stream_plans(state: Arc<AppState>) -> Result<()> {
    let url = format!(
        "{}/api/mining/local/work-plan/events",
        state.config.gridpool_url.trim_end_matches('/')
    );
    let response = state
        .client
        .get(url)
        .headers(adapter_headers(&state)?)
        .send()
        .await?
        .error_for_status()?;
    let mut stream = response.bytes_stream().eventsource();
    while let Some(event) = stream.next().await {
        let event = event?;
        if event.event == "work-plan" || event.event == "heartbeat" || event.event.is_empty() {
            let plan: WorkPlan =
                serde_json::from_str(&event.data).context("decode streamed work plan")?;
            install_plan(&state, plan).await?;
        }
    }
    bail!("work-plan stream ended")
}

async fn fetch_plan(state: &Arc<AppState>) -> Result<()> {
    let url = format!(
        "{}/api/mining/local/work-plan",
        state.config.gridpool_url.trim_end_matches('/')
    );
    let plan = state
        .client
        .get(url)
        .headers(adapter_headers(state)?)
        .send()
        .await?
        .error_for_status()?
        .json::<WorkPlan>()
        .await?;
    install_plan(state, plan).await
}

async fn install_plan(state: &Arc<AppState>, plan: WorkPlan) -> Result<()> {
    plan.validate(&state.config)?;
    let changed = {
        let mut current = state.plan.write().await;
        let changed = current.as_ref().map(|existing| existing.plan_id.as_str())
            != Some(plan.plan_id.as_str());
        if changed {
            info!(plan_id = %plan.plan_id, snapshot = %plan.active_snapshot_id, parent = ?plan.current_tip_block_hash, "installed GridPool work plan");
            *current = Some(plan);
        }
        changed
    };
    if changed {
        state.metrics.lock().unwrap().plan_updates += 1;
    }
    *state.plan_updated_unix_ms.write().await = Some(now_unix_ms());
    Ok(())
}

async fn current_plan(state: &AppState) -> Result<WorkPlan> {
    let updated = *state.plan_updated_unix_ms.read().await;
    let age_ms = updated
        .map(|value| now_unix_ms().saturating_sub(value))
        .context("no valid GridPool work plan")?;
    if age_ms > u128::from(state.config.maximum_plan_age_seconds) * 1_000 {
        bail!("GridPool work plan is stale ({age_ms} ms old)");
    }
    state
        .plan
        .read()
        .await
        .clone()
        .context("no valid GridPool work plan")
}

async fn ipc_loop(state: Arc<AppState>) -> Result<()> {
    let path = Path::new(&state.config.socket_path);
    if let Some(parent) = path.parent() {
        fs::create_dir_all(parent)?;
    }
    if path.exists() {
        fs::remove_file(path)?;
    }
    let listener = UnixListener::bind(path)?;
    fs::set_permissions(path, fs::Permissions::from_mode(0o660))?;
    loop {
        let (stream, _) = listener.accept().await?;
        let connection_state = state.clone();
        tokio::spawn(async move {
            if let Err(error) = handle_ipc(stream, connection_state).await {
                warn!(%error, "IPC request failed");
            }
        });
    }
}

async fn handle_ipc(mut stream: UnixStream, state: Arc<AppState>) -> Result<()> {
    let length = stream.read_u32().await? as usize;
    if length == 0 || length > state.config.maximum_message_bytes {
        bail!("invalid IPC message length {length}");
    }
    let mut body = vec![0u8; length];
    stream.read_exact(&mut body).await?;
    let request: IpcRequest = serde_json::from_slice(&body)?;
    state.metrics.lock().unwrap().ipc_requests += 1;
    let response = process_ipc(request, &state).await;
    let encoded = serde_json::to_vec(&response)?;
    if encoded.len() > state.config.maximum_message_bytes {
        bail!("IPC response exceeds configured limit");
    }
    stream.write_u32(encoded.len() as u32).await?;
    stream.write_all(&encoded).await?;
    Ok(())
}

async fn process_ipc(request: IpcRequest, state: &Arc<AppState>) -> IpcResponse {
    let result: Result<Value> = async {
        match request {
            IpcRequest::GetPlan { schema_version } => {
                check_schema(schema_version)?;
                let plan = current_plan(state).await?;
                Ok(serde_json::to_value(plan)?)
            }
            IpcRequest::FeeDecision { schema_version, parent_hash, payout_script_hex, unix_seconds } => {
                check_schema(schema_version)?;
                let plan = current_plan(state).await?;
                if plan.current_tip_block_hash.as_deref() != Some(parent_hash.as_str()) { bail!("fee request parent does not match current plan"); }
                hex::decode(&payout_script_hex).context("invalid payout script")?;
                let (bucket, fee_active) = fee_bucket(&state.fee_secret, &plan.bitcoin_network, &parent_hash, &payout_script_hex, unix_seconds, state.config.fee_basis_points)?;
                Ok(json!({ "bucket": bucket, "bucketSeconds": 10, "feeActive": fee_active, "feeBasisPoints": state.config.fee_basis_points }))
            }
            IpcRequest::SubmitProof { schema_version, proof } => {
                check_schema(schema_version)?;
                let payload = serde_json::to_string(&proof)?;
                state.database.lock().unwrap().execute("INSERT INTO proof_queue(payload) VALUES (?1)", params![payload])?;
                state.metrics.lock().unwrap().queued_proofs += 1;
                Ok(json!({ "queued": true }))
            }
            IpcRequest::SubmitTelemetry { schema_version, batch } => {
                check_schema(schema_version)?;
                post_json(state, "/api/mining/local/share-telemetry", &batch).await?;
                state.metrics.lock().unwrap().telemetry_batches += 1;
                Ok(json!({ "accepted": true }))
            }
            IpcRequest::RecordShare {
                schema_version,
                channel_id,
                payout_address,
                username,
                accepted,
                difficulty,
                fee_work,
                observed_unix_ms,
            } => {
                check_schema(schema_version)?;
                if !difficulty.is_finite() || difficulty < 0.0 {
                    bail!("invalid telemetry difficulty");
                }
                let key = format!("{channel_id}\0{payout_address}\0{username}");
                let mut telemetry = state.telemetry.lock().unwrap();
                let entry = telemetry.entry(key).or_insert_with(|| TelemetryAccumulator {
                    channel_id,
                    payout_address,
                    username,
                    window_start_unix_ms: observed_unix_ms,
                    window_end_unix_ms: observed_unix_ms,
                    accepted_shares: 0,
                    rejected_shares: 0,
                    accepted_difficulty: 0.0,
                    fee_difficulty: 0.0,
                    best_difficulty: 0.0,
                });
                entry.window_end_unix_ms = entry.window_end_unix_ms.max(observed_unix_ms);
                entry.best_difficulty = entry.best_difficulty.max(difficulty);
                if accepted {
                    entry.accepted_shares += 1;
                    entry.accepted_difficulty += difficulty;
                    if fee_work {
                        entry.fee_difficulty += difficulty;
                    }
                } else {
                    entry.rejected_shares += 1;
                }
                Ok(json!({ "recorded": true }))
            }
            IpcRequest::Health { schema_version } => {
                check_schema(schema_version)?;
                Ok(health_value(state).await)
            }
        }
    }.await;
    match result {
        Ok(data) => IpcResponse::Ok {
            schema_version: IPC_SCHEMA_VERSION,
            data,
        },
        Err(error) => IpcResponse::Error {
            schema_version: IPC_SCHEMA_VERSION,
            code: "request_failed".into(),
            message: error.to_string(),
        },
    }
}

async fn telemetry_loop(state: Arc<AppState>) {
    loop {
        tokio::time::sleep(Duration::from_secs(10)).await;
        let pending = {
            let mut telemetry = state.telemetry.lock().unwrap();
            std::mem::take(&mut *telemetry)
        };
        if pending.is_empty() {
            continue;
        }
        let entries = pending.values().map(|entry| json!({
            "channelId": entry.channel_id,
            "payoutAddress": entry.payout_address,
            "username": entry.username,
            "windowStartUtc": chrono::DateTime::from_timestamp_millis(entry.window_start_unix_ms).map(|value| value.to_rfc3339()),
            "windowEndUtc": chrono::DateTime::from_timestamp_millis(entry.window_end_unix_ms).map(|value| value.to_rfc3339()),
            "acceptedShareCount": entry.accepted_shares,
            "rejectedShareCount": entry.rejected_shares,
            "acceptedWorkDifficulty": entry.accepted_difficulty,
            "feeWorkDifficulty": entry.fee_difficulty,
            "bestDifficulty": entry.best_difficulty
        })).collect::<Vec<_>>();
        let batch = json!({ "sourceInstance": state.config.source_instance, "entries": entries });
        if let Err(error) = post_json(&state, "/api/mining/local/share-telemetry", &batch).await {
            warn!(%error, "telemetry flush failed; restoring batch");
            let mut telemetry = state.telemetry.lock().unwrap();
            for (key, old) in pending {
                telemetry
                    .entry(key)
                    .and_modify(|entry| {
                        entry.window_start_unix_ms =
                            entry.window_start_unix_ms.min(old.window_start_unix_ms);
                        entry.window_end_unix_ms =
                            entry.window_end_unix_ms.max(old.window_end_unix_ms);
                        entry.accepted_shares += old.accepted_shares;
                        entry.rejected_shares += old.rejected_shares;
                        entry.accepted_difficulty += old.accepted_difficulty;
                        entry.fee_difficulty += old.fee_difficulty;
                        entry.best_difficulty = entry.best_difficulty.max(old.best_difficulty);
                    })
                    .or_insert(old);
            }
        } else {
            state.metrics.lock().unwrap().telemetry_batches += 1;
        }
    }
}

fn check_schema(schema: u32) -> Result<()> {
    if schema != IPC_SCHEMA_VERSION {
        bail!("unsupported IPC schema {schema}");
    }
    Ok(())
}

async fn proof_queue_loop(state: Arc<AppState>) {
    loop {
        if let Err(error) = submit_next_proof(&state).await {
            warn!(%error, "queued proof submission failed");
        }
        tokio::time::sleep(Duration::from_millis(250)).await;
    }
}

async fn submit_next_proof(state: &Arc<AppState>) -> Result<()> {
    let now = SystemTime::now().duration_since(UNIX_EPOCH)?.as_secs() as i64;
    let queued = {
        let database = state.database.lock().unwrap();
        let mut statement = database.prepare("SELECT id, payload, attempts FROM proof_queue WHERE next_attempt_unix <= ?1 ORDER BY id LIMIT 1")?;
        statement
            .query_row(params![now], |row| {
                Ok((
                    row.get::<_, i64>(0)?,
                    row.get::<_, String>(1)?,
                    row.get::<_, i64>(2)?,
                ))
            })
            .ok()
    };
    let Some((id, payload, attempts)) = queued else {
        return Ok(());
    };
    let proof: Value = serde_json::from_str(&payload)?;
    match post_json(state, "/api/mining/local/share", &proof).await {
        Ok(_) => {
            state
                .database
                .lock()
                .unwrap()
                .execute("DELETE FROM proof_queue WHERE id=?1", params![id])?;
            state.metrics.lock().unwrap().submitted_proofs += 1;
        }
        Err(error) => {
            let permanent = error
                .downcast_ref::<reqwest::Error>()
                .and_then(reqwest::Error::status)
                .is_some_and(|status| {
                    status.is_client_error()
                        && status != reqwest::StatusCode::REQUEST_TIMEOUT
                        && status != reqwest::StatusCode::TOO_MANY_REQUESTS
                });
            if permanent {
                state
                    .database
                    .lock()
                    .unwrap()
                    .execute("DELETE FROM proof_queue WHERE id=?1", params![id])?;
                state.metrics.lock().unwrap().failed_proofs += 1;
                return Err(error.context("permanent proof rejection; dropped from retry queue"));
            }
            let delay = 2i64.pow((attempts.min(8) + 1) as u32);
            state.database.lock().unwrap().execute(
                "UPDATE proof_queue SET attempts=attempts+1, next_attempt_unix=?1, last_error=?2 WHERE id=?3",
                params![now + delay, error.to_string(), id])?;
            state.metrics.lock().unwrap().failed_proofs += 1;
            return Err(error);
        }
    }
    Ok(())
}

async fn post_json(state: &AppState, path: &str, payload: &Value) -> Result<Value> {
    let url = format!(
        "{}{}",
        state.config.gridpool_url.trim_end_matches('/'),
        path
    );
    Ok(state
        .client
        .post(url)
        .headers(adapter_headers(state)?)
        .json(payload)
        .send()
        .await?
        .error_for_status()?
        .json()
        .await?)
}

async fn health(State(state): State<Arc<AppState>>) -> Json<Value> {
    Json(health_value(&state).await)
}

async fn health_value(state: &AppState) -> Value {
    let metrics = *state.metrics.lock().unwrap();
    let queued: i64 = state
        .database
        .lock()
        .unwrap()
        .query_row("SELECT COUNT(*) FROM proof_queue", [], |row| row.get(0))
        .unwrap_or(-1);
    let plan = state.plan.read().await.clone();
    let plan_age_ms = state
        .plan_updated_unix_ms
        .read()
        .await
        .map(|value| now_unix_ms().saturating_sub(value));
    let plan_fresh = plan_age_ms
        .is_some_and(|value| value <= u128::from(state.config.maximum_plan_age_seconds) * 1_000);
    json!({
        "status": if plan.is_some() && plan_fresh { "ready" } else { "waiting-for-plan" },
        "ipcSchemaVersion": IPC_SCHEMA_VERSION,
        "planId": plan.as_ref().map(|value| &value.plan_id),
        "activeSnapshotId": plan.as_ref().map(|value| &value.active_snapshot_id),
        "parentHash": plan.as_ref().and_then(|value| value.current_tip_block_hash.as_ref()),
        "planUpdatedUnixMs": *state.plan_updated_unix_ms.read().await,
        "planAgeMs": plan_age_ms,
        "maximumPlanAgeSeconds": state.config.maximum_plan_age_seconds,
        "queueDepth": queued,
        "feeBasisPoints": state.config.fee_basis_points,
        "metrics": {
            "planUpdates": metrics.plan_updates,
            "planErrors": metrics.plan_errors,
            "ipcRequests": metrics.ipc_requests,
            "queuedProofs": metrics.queued_proofs,
            "submittedProofs": metrics.submitted_proofs,
            "failedProofs": metrics.failed_proofs,
            "telemetryBatches": metrics.telemetry_batches
        }
    })
}

fn now_unix_ms() -> u128 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|value| value.as_millis())
        .unwrap_or(0)
}
