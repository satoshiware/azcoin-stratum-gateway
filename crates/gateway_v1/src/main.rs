use rand::random;
use reqwest::Client;
use serde::Serialize;
use serde_json::{json, Value};
use std::collections::{HashMap, HashSet, VecDeque};
use std::env;
use std::io::{self, BufRead, BufReader, BufWriter, Write};
use std::net::{SocketAddr, TcpListener, TcpStream};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::thread;
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};
use sv2_core::stratum_core::bitcoin::hashes::{sha256d, Hash};
use tokio::sync::mpsc::{self, error::TrySendError};

mod gbt;
mod version;
use gbt::poller::TemplatePollerState;

const SUBSCRIPTION_ID: &str = "1";
const EXTRANONCE_2_SIZE: u64 = 8;
const STARTING_DIFFICULTY: u64 = 1;
const VERSION_ROLLING_EXTENSION: &str = "version-rolling";
const DEFAULT_VERSION_ROLLING_MASK: &str = "ffffffff";
const MAX_ACTIVE_JOBS: usize = 2048;
const MAX_SEEN_SHARES: usize = 10_000;
const DEFAULT_SHARE_SINK: &str = "log";
const DEFAULT_NODE_API_URL: &str = "http://node-api:8000";
const DEFAULT_NODE_API_SHARE_PATH: &str = "/v1/mining/share";
const DEFAULT_SHARE_QUEUE_MAX: usize = 5000;
const DEFAULT_SHARE_HTTP_TIMEOUT_MS: u64 = 2000;
const DIFF1_TARGET_HEX: &str = "00000000FFFF0000000000000000000000000000000000000000000000000000";

static NEXT_JOB_ID_COUNTER: AtomicU64 = AtomicU64::new(1);
static NEXT_SESSION_ID_COUNTER: AtomicU64 = AtomicU64::new(1);

struct GatewayState {
    sessions: AtomicU64,
    shares_ok: AtomicU64,
    shares_rej: AtomicU64,
    shares_dup: AtomicU64,
    forwarding_counters: Arc<ForwardingCounters>,
    active_jobs: Mutex<BoundedStringSet>,
    job_store: Mutex<BoundedJobStore>,
    seen_shares: Mutex<BoundedShareSet>,
    worker_stats: Mutex<HashMap<String, WorkerStats>>,
    notify_state: Mutex<NotifyJobState>,
    share_sink: Arc<dyn ShareSink>,
}

impl GatewayState {
    fn new() -> Self {
        Self::with_share_sink(Arc::new(LogShareSink))
    }

    fn from_env() -> Self {
        let forwarding_counters = Arc::new(ForwardingCounters::default());
        let share_sink = build_share_sink_from_env(Arc::clone(&forwarding_counters));
        Self::with_components(share_sink, forwarding_counters)
    }

    fn with_share_sink(share_sink: Arc<dyn ShareSink>) -> Self {
        let forwarding_counters = Arc::new(ForwardingCounters::default());
        Self::with_components(share_sink, forwarding_counters)
    }

    fn with_components(
        share_sink: Arc<dyn ShareSink>,
        forwarding_counters: Arc<ForwardingCounters>,
    ) -> Self {
        Self {
            sessions: AtomicU64::new(0),
            shares_ok: AtomicU64::new(0),
            shares_rej: AtomicU64::new(0),
            shares_dup: AtomicU64::new(0),
            forwarding_counters,
            active_jobs: Mutex::new(BoundedStringSet::new(MAX_ACTIVE_JOBS)),
            job_store: Mutex::new(BoundedJobStore::new(MAX_ACTIVE_JOBS)),
            seen_shares: Mutex::new(BoundedShareSet::new(MAX_SEEN_SHARES)),
            worker_stats: Mutex::new(HashMap::new()),
            notify_state: Mutex::new(NotifyJobState::default()),
            share_sink,
        }
    }

    fn register_active_job(&self, job_id: String) {
        let mut guard = self
            .active_jobs
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        guard.insert(job_id.clone());
        self.upsert_job_template(job_id, JobTemplate::default());
    }

    fn has_active_job(&self, job_id: &str) -> bool {
        let guard = self
            .active_jobs
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        guard.contains(job_id)
    }

    fn insert_share_key_if_new(&self, key: ShareKey) -> bool {
        let mut guard = self
            .seen_shares
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        guard.insert_if_new(key)
    }

    fn record_accepted_share(&self) {
        self.shares_ok.fetch_add(1, Ordering::Relaxed);
    }

    fn record_rejected_share(&self, duplicate: bool) {
        self.shares_rej.fetch_add(1, Ordering::Relaxed);
        if duplicate {
            self.shares_dup.fetch_add(1, Ordering::Relaxed);
        }
    }

    fn share_counters(&self) -> (u64, u64, u64) {
        (
            self.shares_ok.load(Ordering::Relaxed),
            self.shares_rej.load(Ordering::Relaxed),
            self.shares_dup.load(Ordering::Relaxed),
        )
    }

    fn forwarding_counters(&self) -> (u64, u64, u64) {
        self.forwarding_counters.snapshot()
    }

    fn allocate_or_reuse_job_id(&self, work_key: &str, force_clean_jobs: bool) -> (String, bool) {
        let (job_id, allocated_new) = {
            let mut notify_state = self
                .notify_state
                .lock()
                .unwrap_or_else(|poisoned| poisoned.into_inner());
            let work_changed = notify_state.work_key.as_deref() != Some(work_key);
            let needs_new_job = notify_state.job_id.is_none() || work_changed || force_clean_jobs;

            if needs_new_job {
                let job_id = allocate_job_id();
                notify_state.work_key = Some(work_key.to_string());
                notify_state.job_id = Some(job_id.clone());
                (job_id, true)
            } else {
                (
                    notify_state
                        .job_id
                        .clone()
                        .expect("job_id should exist when reusing work"),
                    false,
                )
            }
        };

        if allocated_new {
            self.register_active_job(job_id.clone());
        }

        (job_id, allocated_new)
    }

    fn emit_share_event(&self, event: ShareEvent) {
        self.share_sink.submit(event);
    }

    fn upsert_job_template(&self, job_id: String, job_template: JobTemplate) {
        let mut guard = self
            .job_store
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        guard.upsert(job_id, job_template);
    }

    fn get_job_template(&self, job_id: &str) -> Option<JobTemplate> {
        let guard = self
            .job_store
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        guard.get(job_id)
    }

    fn record_worker_share(&self, worker: &str, accepted: bool, share_diff: f64, last_seen: u64) {
        if worker.is_empty() {
            return;
        }

        let mut guard = self
            .worker_stats
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        let stats = guard.entry(worker.to_string()).or_default();
        if accepted {
            stats.accepted += 1;
        } else {
            stats.rejected += 1;
        }
        stats.last_seen = last_seen;
        if share_diff > stats.best_share_diff {
            stats.best_share_diff = share_diff;
        }
    }
}

#[derive(Debug, Default)]
struct ForwardingCounters {
    shares_fwd_ok: AtomicU64,
    shares_fwd_fail: AtomicU64,
    shares_drop: AtomicU64,
}

impl ForwardingCounters {
    fn inc_fwd_ok(&self) {
        self.shares_fwd_ok.fetch_add(1, Ordering::Relaxed);
    }

    fn inc_fwd_fail(&self) {
        self.shares_fwd_fail.fetch_add(1, Ordering::Relaxed);
    }

    fn inc_drop(&self) {
        self.shares_drop.fetch_add(1, Ordering::Relaxed);
    }

    fn snapshot(&self) -> (u64, u64, u64) {
        (
            self.shares_fwd_ok.load(Ordering::Relaxed),
            self.shares_fwd_fail.load(Ordering::Relaxed),
            self.shares_drop.load(Ordering::Relaxed),
        )
    }
}

#[derive(Debug, Default)]
struct NotifyJobState {
    work_key: Option<String>,
    job_id: Option<String>,
}

#[derive(Debug, Clone, Default)]
struct JobTemplate {
    prevhash: String,
    coinb1: String,
    coinb2: String,
    merkle_branch: Vec<String>,
    version: String,
    nbits: String,
    job_ntime: String,
}

#[derive(Debug, Clone)]
struct BoundedJobStore {
    max_size: usize,
    order: VecDeque<String>,
    map: HashMap<String, JobTemplate>,
}

impl BoundedJobStore {
    fn new(max_size: usize) -> Self {
        Self {
            max_size,
            order: VecDeque::new(),
            map: HashMap::new(),
        }
    }

    fn upsert(&mut self, job_id: String, job_template: JobTemplate) {
        let is_new = !self.map.contains_key(&job_id);
        if is_new {
            if self.order.len() >= self.max_size {
                if let Some(oldest) = self.order.pop_front() {
                    self.map.remove(&oldest);
                }
            }
            self.order.push_back(job_id.clone());
        }
        self.map.insert(job_id, job_template);
    }

    fn get(&self, job_id: &str) -> Option<JobTemplate> {
        self.map.get(job_id).cloned()
    }
}

#[derive(Debug, Clone, Default)]
struct WorkerStats {
    accepted: u64,
    rejected: u64,
    last_seen: u64,
    best_share_diff: f64,
}

#[derive(Debug, Clone)]
struct BoundedStringSet {
    max_size: usize,
    order: VecDeque<String>,
    set: HashSet<String>,
}

impl BoundedStringSet {
    fn new(max_size: usize) -> Self {
        Self {
            max_size,
            order: VecDeque::new(),
            set: HashSet::new(),
        }
    }

    fn insert(&mut self, value: String) {
        if self.set.contains(&value) {
            return;
        }

        if self.order.len() >= self.max_size {
            if let Some(oldest) = self.order.pop_front() {
                self.set.remove(&oldest);
            }
        }

        self.set.insert(value.clone());
        self.order.push_back(value);
    }

    fn contains(&self, value: &str) -> bool {
        self.set.contains(value)
    }
}

#[derive(Debug, Clone, Eq, PartialEq, Hash)]
struct ShareKey {
    worker: String,
    job_id: String,
    extranonce2: String,
    ntime: String,
    nonce: String,
    version_bits: String,
}

#[derive(Debug, Clone)]
struct BoundedShareSet {
    max_size: usize,
    order: VecDeque<ShareKey>,
    set: HashSet<ShareKey>,
}

impl BoundedShareSet {
    fn new(max_size: usize) -> Self {
        Self {
            max_size,
            order: VecDeque::new(),
            set: HashSet::new(),
        }
    }

    fn insert_if_new(&mut self, key: ShareKey) -> bool {
        if self.set.contains(&key) {
            return false;
        }

        if self.order.len() >= self.max_size {
            if let Some(oldest) = self.order.pop_front() {
                self.set.remove(&oldest);
            }
        }

        self.set.insert(key.clone());
        self.order.push_back(key);
        true
    }
}

#[derive(Debug, Clone, Serialize)]
struct ShareEvent {
    ts: u64,
    ts_ms: i64,
    remote: String,
    worker: String,
    job_id: String,
    difficulty: u32,
    accepted: bool,
    reason: Option<String>,
    extranonce2: String,
    ntime: String,
    nonce: String,
    version_bits: String,
    accepted_unvalidated: bool,
}

trait ShareSink: Send + Sync {
    fn submit(&self, event: ShareEvent);
}

struct LogShareSink;

impl ShareSink for LogShareSink {
    fn submit(&self, event: ShareEvent) {
        match serde_json::to_string(&event) {
            Ok(serialized) => println!("SHARE_EVENT {serialized}"),
            Err(error) => eprintln!("SHARE_EVENT serialize_failed error=\"{error}\""),
        }
    }
}

struct CompositeShareSink {
    sinks: Vec<Arc<dyn ShareSink>>,
}

impl CompositeShareSink {
    fn new(sinks: Vec<Arc<dyn ShareSink>>) -> Self {
        Self { sinks }
    }
}

impl ShareSink for CompositeShareSink {
    fn submit(&self, event: ShareEvent) {
        for sink in &self.sinks {
            sink.submit(event.clone());
        }
    }
}

#[derive(Debug, Clone)]
struct HttpShareSinkConfig {
    endpoint: String,
    bearer_token: Option<String>,
    queue_max: usize,
    timeout_ms: u64,
}

struct HttpShareSink {
    sender: mpsc::Sender<ShareEvent>,
    forwarding_counters: Arc<ForwardingCounters>,
}

impl HttpShareSink {
    fn new(config: HttpShareSinkConfig, forwarding_counters: Arc<ForwardingCounters>) -> Self {
        let queue_size = config.queue_max.max(1);
        let (sender, receiver) = mpsc::channel(queue_size);
        let forwarding_for_worker = Arc::clone(&forwarding_counters);

        thread::spawn(move || {
            let runtime = match tokio::runtime::Builder::new_current_thread()
                .enable_io()
                .enable_time()
                .build()
            {
                Ok(runtime) => runtime,
                Err(error) => {
                    eprintln!("SHARE_HTTP runtime_init_failed error=\"{error}\"");
                    return;
                }
            };

            runtime.block_on(run_http_share_sink_worker(
                config,
                forwarding_for_worker,
                receiver,
            ));
        });

        Self {
            sender,
            forwarding_counters,
        }
    }
}

impl ShareSink for HttpShareSink {
    fn submit(&self, event: ShareEvent) {
        match self.sender.try_send(event) {
            Ok(()) => {}
            Err(TrySendError::Full(_)) | Err(TrySendError::Closed(_)) => {
                self.forwarding_counters.inc_drop();
            }
        }
    }
}

async fn run_http_share_sink_worker(
    config: HttpShareSinkConfig,
    forwarding_counters: Arc<ForwardingCounters>,
    mut receiver: mpsc::Receiver<ShareEvent>,
) {
    let client = match Client::builder()
        .timeout(Duration::from_millis(config.timeout_ms))
        .build()
    {
        Ok(client) => client,
        Err(error) => {
            eprintln!("SHARE_HTTP client_init_failed error=\"{error}\"");
            while receiver.recv().await.is_some() {
                forwarding_counters.inc_fwd_fail();
            }
            return;
        }
    };

    while let Some(event) = receiver.recv().await {
        let mut request = client.post(&config.endpoint).json(&event);
        if let Some(token) = config.bearer_token.as_deref() {
            request = request.bearer_auth(token);
        }

        match request.send().await {
            Ok(response) if response.status().is_success() => {
                forwarding_counters.inc_fwd_ok();
            }
            Ok(response) => {
                forwarding_counters.inc_fwd_fail();
                eprintln!(
                    "SHARE_HTTP forward_failed endpoint={} status={}",
                    config.endpoint,
                    response.status().as_u16()
                );
            }
            Err(error) => {
                forwarding_counters.inc_fwd_fail();
                eprintln!(
                    "SHARE_HTTP request_failed endpoint={} error=\"{}\"",
                    config.endpoint, error
                );
            }
        }
    }
}

fn build_share_sink_from_env(forwarding_counters: Arc<ForwardingCounters>) -> Arc<dyn ShareSink> {
    let mode = env::var("AZ_SHARE_SINK")
        .ok()
        .unwrap_or_else(|| DEFAULT_SHARE_SINK.to_string())
        .to_lowercase();

    match mode.as_str() {
        "log" => Arc::new(LogShareSink),
        "http" => Arc::new(HttpShareSink::new(
            http_share_sink_config_from_env(),
            forwarding_counters,
        )),
        "both" => Arc::new(CompositeShareSink::new(vec![
            Arc::new(LogShareSink),
            Arc::new(HttpShareSink::new(
                http_share_sink_config_from_env(),
                forwarding_counters,
            )),
        ])),
        unknown => {
            eprintln!("SHARE_SINK unknown_mode={} defaulting=log", unknown);
            Arc::new(LogShareSink)
        }
    }
}

fn http_share_sink_config_from_env() -> HttpShareSinkConfig {
    let base_url = env::var("AZ_NODE_API_URL")
        .ok()
        .filter(|value| !value.trim().is_empty())
        .unwrap_or_else(|| DEFAULT_NODE_API_URL.to_string());
    let share_path = env::var("AZ_NODE_API_SHARE_PATH")
        .ok()
        .filter(|value| !value.trim().is_empty())
        .unwrap_or_else(|| DEFAULT_NODE_API_SHARE_PATH.to_string());
    let bearer_token = env::var("AZ_NODE_API_TOKEN").ok().and_then(|value| {
        if value.trim().is_empty() {
            None
        } else {
            Some(value)
        }
    });
    let queue_max = env::var("AZ_SHARE_QUEUE_MAX")
        .ok()
        .and_then(|value| value.parse::<usize>().ok())
        .unwrap_or(DEFAULT_SHARE_QUEUE_MAX)
        .max(1);
    let timeout_ms = env::var("AZ_SHARE_HTTP_TIMEOUT_MS")
        .ok()
        .and_then(|value| value.parse::<u64>().ok())
        .unwrap_or(DEFAULT_SHARE_HTTP_TIMEOUT_MS)
        .max(1);

    HttpShareSinkConfig {
        endpoint: join_api_url(&base_url, &share_path),
        bearer_token,
        queue_max,
        timeout_ms,
    }
}

fn join_api_url(base_url: &str, path: &str) -> String {
    let base = base_url.trim_end_matches('/');
    let normalized_path = if path.starts_with('/') {
        path.to_string()
    } else {
        format!("/{path}")
    };
    format!("{base}{normalized_path}")
}

#[derive(Debug)]
struct SessionState {
    session_id: u64,
    diff_sent: bool,
    extranonce1_hex: Option<String>,
}

impl Default for SessionState {
    fn default() -> Self {
        Self {
            session_id: allocate_session_id(),
            diff_sent: false,
            extranonce1_hex: None,
        }
    }
}

impl SessionState {
    fn extranonce1_hex(&mut self) -> &str {
        self.extranonce1_hex
            .get_or_insert_with(generate_extranonce1_hex)
            .as_str()
    }
}

#[derive(Debug)]
struct RpcRequest {
    id: Value,
    method: String,
    params: Value,
}

fn main() -> io::Result<()> {
    let _ = sv2_core::sv2_foundation_ready();
    let version = version::get_version();
    let revision = version::get_revision();

    if env::args()
        .skip(1)
        .any(|arg| arg == "--version" || arg == "-V")
    {
        println!("stratum-gateway version {version} (rev {revision})");
        return Ok(());
    }

    println!("stratum-gateway version {version} (rev {revision})");

    let bind_addr = env::var("GATEWAY_BIND_ADDR").unwrap_or_else(|_| "0.0.0.0:3333".to_string());
    let health_log_interval_secs = env::var("HEALTH_LOG_INTERVAL_SECS")
        .ok()
        .and_then(|value| value.parse::<u64>().ok())
        .unwrap_or(30)
        .max(1);

    let listener = TcpListener::bind(&bind_addr)?;
    println!("LISTENING addr={bind_addr}");

    let started_at = Instant::now();
    let state = Arc::new(GatewayState::from_env());
    let template_state = Arc::new(TemplatePollerState::default());
    spawn_gbt_poller(Arc::clone(&template_state));
    spawn_health_logger(
        Arc::clone(&state),
        Arc::clone(&template_state),
        started_at,
        Duration::from_secs(health_log_interval_secs),
    );

    loop {
        match listener.accept() {
            Ok((stream, remote_addr)) => {
                let state_for_client = Arc::clone(&state);
                let template_state_for_client = Arc::clone(&template_state);
                thread::spawn(move || {
                    handle_client(
                        stream,
                        remote_addr,
                        state_for_client,
                        template_state_for_client,
                    );
                });
            }
            Err(error) => {
                eprintln!("[tcp] accept_failed error=\"{error}\"");
                thread::sleep(Duration::from_secs(1));
            }
        }
    }
}

fn spawn_health_logger(
    state: Arc<GatewayState>,
    template_state: Arc<TemplatePollerState>,
    started_at: Instant,
    interval: Duration,
) {
    thread::spawn(move || loop {
        thread::sleep(interval);
        let uptime_secs = started_at.elapsed().as_secs();
        let sessions = state.sessions.load(Ordering::Relaxed);
        let jobs = template_state.current_job_counter();
        let (shares_ok, shares_rej, shares_dup) = state.share_counters();
        let (shares_fwd_ok, shares_fwd_fail, shares_drop) = state.forwarding_counters();
        println!(
            "HEALTH ok uptime={uptime_secs}s sessions={sessions} jobs={jobs} shares_ok={shares_ok} shares_rej={shares_rej} shares_dup={shares_dup} shares_fwd_ok={shares_fwd_ok} shares_fwd_fail={shares_fwd_fail} shares_drop={shares_drop}"
        );
    });
}

fn spawn_gbt_poller(shared_state: Arc<TemplatePollerState>) {
    thread::spawn(move || {
        let runtime = match tokio::runtime::Builder::new_current_thread()
            .enable_io()
            .enable_time()
            .build()
        {
            Ok(runtime) => runtime,
            Err(error) => {
                eprintln!("TEMPLATE_POLLER runtime_init_failed error=\"{error}\"");
                return;
            }
        };

        if let Err(error) = runtime.block_on(gbt::poller::run_template_poller(shared_state)) {
            eprintln!("TEMPLATE_POLLER exited error=\"{error}\"");
        }
    });
}

fn handle_client(
    stream: TcpStream,
    remote_addr: SocketAddr,
    state: Arc<GatewayState>,
    template_state: Arc<TemplatePollerState>,
) {
    let active_sessions = state.sessions.fetch_add(1, Ordering::SeqCst) + 1;
    println!("[tcp] session_open remote_addr={remote_addr} sessions={active_sessions}");

    if let Err(error) = handle_client_inner(stream, remote_addr, Arc::clone(&state), template_state)
    {
        eprintln!("[tcp] session_error remote_addr={remote_addr} error=\"{error}\"");
    }

    let active_sessions = state.sessions.fetch_sub(1, Ordering::SeqCst) - 1;
    println!("[tcp] session_closed remote_addr={remote_addr} sessions={active_sessions}");
}

fn handle_client_inner(
    stream: TcpStream,
    remote_addr: SocketAddr,
    state: Arc<GatewayState>,
    template_state: Arc<TemplatePollerState>,
) -> io::Result<()> {
    let reader_stream = stream.try_clone()?;
    let mut reader = BufReader::new(reader_stream);
    let mut writer = BufWriter::new(stream);
    let mut session_state = SessionState::default();
    let mut line = String::new();

    loop {
        line.clear();
        let bytes_read = reader.read_line(&mut line)?;
        if bytes_read == 0 {
            break;
        }

        let trimmed = line.trim_end_matches(['\r', '\n']);
        if trimmed.is_empty() {
            continue;
        }
        println!("STRATUM_RX remote={remote_addr} line={trimmed}");

        match parse_rpc_request(trimmed) {
            Ok(request) => {
                let response =
                    build_response(&request, remote_addr, &mut session_state, state.as_ref());
                write_json_line(
                    &mut writer,
                    remote_addr,
                    &session_state,
                    request.method.as_str(),
                    &response,
                )?;

                if request.method == "mining.subscribe" {
                    send_set_difficulty(
                        &mut writer,
                        remote_addr,
                        &mut session_state,
                        STARTING_DIFFICULTY,
                    )?;
                }

                if request.method == "mining.authorize" {
                    if !session_state.diff_sent {
                        send_set_difficulty(
                            &mut writer,
                            remote_addr,
                            &mut session_state,
                            STARTING_DIFFICULTY,
                        )?;
                    }
                    let notify = build_notify_push(template_state.as_ref(), state.as_ref(), false);
                    state.upsert_job_template(notify.job_id.clone(), notify.job_template.clone());
                    write_json_line(
                        &mut writer,
                        remote_addr,
                        &session_state,
                        "mining.notify",
                        &notify.message,
                    )?;
                }
            }
            Err(error) => {
                // Intentionally log only metadata; request bodies can contain passwords.
                eprintln!(
                    "[rpc] invalid_json_or_shape remote_addr={remote_addr} error=\"{error}\""
                );
            }
        }
    }

    writer.flush()?;
    Ok(())
}

fn parse_rpc_request(line: &str) -> Result<RpcRequest, &'static str> {
    let value: Value = serde_json::from_str(line).map_err(|_| "invalid json")?;
    let object = value.as_object().ok_or("request must be a json object")?;

    let id = object.get("id").cloned().ok_or("missing id")?;
    if !id.is_number() && !id.is_string() {
        return Err("id must be a number or string");
    }

    let method = object
        .get("method")
        .and_then(Value::as_str)
        .ok_or("missing or invalid method")?
        .to_string();

    let params = object
        .get("params")
        .cloned()
        .unwrap_or_else(|| Value::Array(vec![]));

    Ok(RpcRequest { id, method, params })
}

fn build_response(
    request: &RpcRequest,
    remote_addr: SocketAddr,
    session_state: &mut SessionState,
    state: &GatewayState,
) -> Value {
    match request.method.as_str() {
        "mining.configure" => {
            let result = match version_rolling_mask_if_requested(&request.params) {
                Some(mask) => json!({
                    "version-rolling": true,
                    "version-rolling.mask": mask
                }),
                None => json!({
                    "version-rolling": false
                }),
            };

            json!({
                "id": request.id,
                "result": result,
                "error": Value::Null
            })
        }
        "mining.subscribe" => json!({
            "id": request.id,
            "result": [
                [
                    ["mining.set_difficulty", SUBSCRIPTION_ID],
                    ["mining.notify", SUBSCRIPTION_ID]
                ],
                session_state.extranonce1_hex(),
                EXTRANONCE_2_SIZE
            ],
            "error": Value::Null
        }),
        "mining.authorize" => {
            let worker = request
                .params
                .as_array()
                .and_then(|params| params.first())
                .and_then(Value::as_str)
                .unwrap_or("<missing>");
            println!("[auth] remote_addr={remote_addr} worker={worker}");
            json!({
                "id": request.id,
                "result": true,
                "error": Value::Null
            })
        }
        "mining.submit" => build_submit_response(request, remote_addr, session_state, state),
        _ => json!({
            "id": request.id,
            "result": Value::Null,
            "error": {
                "code": -32601,
                "message": "Method not found"
            }
        }),
    }
}

fn version_rolling_mask_if_requested(params: &Value) -> Option<String> {
    let params_array = params.as_array()?;
    let requested_extensions = params_array.first()?.as_array()?;
    let requested = requested_extensions.iter().any(|entry| {
        entry
            .as_str()
            .map(|name| name == VERSION_ROLLING_EXTENSION)
            .unwrap_or(false)
    });

    if !requested {
        return None;
    }

    let mask = params_array
        .get(1)
        .and_then(Value::as_object)
        .and_then(|options| options.get("version-rolling.mask"))
        .and_then(Value::as_str)
        .unwrap_or(DEFAULT_VERSION_ROLLING_MASK)
        .to_string();

    Some(mask)
}

#[derive(Debug, Clone)]
struct SubmitShare {
    worker: String,
    job_id: String,
    extranonce2: String,
    ntime: String,
    nonce: String,
    version_bits: String,
}

impl SubmitShare {
    fn duplicate_key(&self) -> ShareKey {
        ShareKey {
            worker: self.worker.clone(),
            job_id: self.job_id.clone(),
            extranonce2: self.extranonce2.clone(),
            ntime: self.ntime.clone(),
            nonce: self.nonce.clone(),
            version_bits: self.version_bits.clone(),
        }
    }
}

#[derive(Debug, Clone)]
enum SubmitValidation {
    Rejected {
        share: SubmitShare,
        reason: String,
        duplicate: bool,
        error: Value,
        share_diff: f64,
    },
    Accepted {
        share: SubmitShare,
        share_diff: f64,
    },
}

fn build_submit_response(
    request: &RpcRequest,
    remote_addr: SocketAddr,
    session_state: &SessionState,
    state: &GatewayState,
) -> Value {
    build_submit_response_with_validator(
        request,
        remote_addr,
        session_state,
        state,
        unix_seconds_now(),
        validate_submit_params,
    )
}

fn build_submit_response_with_validator<V>(
    request: &RpcRequest,
    remote_addr: SocketAddr,
    session_state: &SessionState,
    state: &GatewayState,
    now: u64,
    validator: V,
) -> Value
where
    V: FnOnce(&Value, &SessionState, &GatewayState) -> SubmitValidation,
{
    let validation = validator(&request.params, session_state, state);

    match validation {
        SubmitValidation::Rejected {
            share,
            reason,
            duplicate,
            error,
            share_diff,
        } => {
            state.record_rejected_share(duplicate);
            state.record_worker_share(&share.worker, false, share_diff, now);
            emit_share_event(state, remote_addr, &share, false, Some(reason));
            json!({
                "id": request.id,
                "result": false,
                "error": error
            })
        }
        SubmitValidation::Accepted { share, share_diff } => {
            state.record_accepted_share();
            state.record_worker_share(&share.worker, true, share_diff, now);
            emit_share_event(state, remote_addr, &share, true, None);
            json!({
                "id": request.id,
                "result": true,
                "error": Value::Null
            })
        }
    }
}

fn emit_share_event(
    state: &GatewayState,
    remote_addr: SocketAddr,
    share: &SubmitShare,
    accepted: bool,
    reason: Option<String>,
) {
    let event = ShareEvent {
        ts: unix_seconds_now(),
        ts_ms: unix_millis_now(),
        remote: remote_addr.to_string(),
        worker: share.worker.clone(),
        job_id: share.job_id.clone(),
        difficulty: 1,
        accepted,
        reason,
        extranonce2: share.extranonce2.clone(),
        ntime: share.ntime.clone(),
        nonce: share.nonce.clone(),
        version_bits: share.version_bits.clone(),
        accepted_unvalidated: accepted,
    };
    state.emit_share_event(event);
}

fn validate_submit_params(
    params: &Value,
    session_state: &SessionState,
    state: &GatewayState,
) -> SubmitValidation {
    let Some(param_array) = params.as_array() else {
        return submit_rejected(
            partial_submit_fields(params),
            "invalid_submit_params",
            false,
            submit_error("invalid submit params"),
            0.0,
        );
    };

    if param_array.len() != 5 && param_array.len() != 6 {
        return submit_rejected(
            partial_submit_fields(params),
            "invalid_param_count",
            false,
            submit_error("invalid submit params"),
            0.0,
        );
    }

    let worker = match param_array.first().and_then(Value::as_str) {
        Some(value) => value.to_string(),
        None => {
            return submit_rejected(
                partial_submit_fields(params),
                "invalid_worker_name_type",
                false,
                submit_error("invalid submit params"),
                0.0,
            );
        }
    };
    let job_id = match param_array.get(1).and_then(Value::as_str) {
        Some(value) => value.to_string(),
        None => {
            return submit_rejected(
                partial_submit_fields(params),
                "invalid_job_id_type",
                false,
                submit_error("invalid submit params"),
                0.0,
            );
        }
    };
    let extranonce2 = match param_array.get(2).and_then(Value::as_str) {
        Some(value) => value.to_string(),
        None => {
            return submit_rejected(
                partial_submit_fields(params),
                "invalid_extranonce2_type",
                false,
                submit_error("invalid submit params"),
                0.0,
            );
        }
    };
    let ntime = match param_array.get(3).and_then(Value::as_str) {
        Some(value) => value.to_string(),
        None => {
            return submit_rejected(
                partial_submit_fields(params),
                "invalid_ntime_type",
                false,
                submit_error("invalid submit params"),
                0.0,
            );
        }
    };
    let nonce = match param_array.get(4).and_then(Value::as_str) {
        Some(value) => value.to_string(),
        None => {
            return submit_rejected(
                partial_submit_fields(params),
                "invalid_nonce_type",
                false,
                submit_error("invalid submit params"),
                0.0,
            );
        }
    };
    let version_bits = match param_array.get(5) {
        Some(value) => match value.as_str() {
            Some(bits) => bits.to_string(),
            None => {
                return submit_rejected(
                    partial_submit_fields(params),
                    "invalid_version_bits_type",
                    false,
                    submit_error("invalid submit params"),
                    0.0,
                );
            }
        },
        None => String::new(),
    };

    let share = SubmitShare {
        worker,
        job_id,
        extranonce2,
        ntime,
        nonce,
        version_bits,
    };

    if share.worker.is_empty() || share.worker.len() > 64 {
        return submit_rejected(
            share,
            "invalid_worker",
            false,
            submit_error("invalid submit params"),
            0.0,
        );
    }

    let Some(session_extranonce1) = session_state.extranonce1_hex.as_deref() else {
        return submit_rejected(
            share,
            "missing_extranonce1",
            false,
            submit_error("not subscribed"),
            0.0,
        );
    };

    if !is_decodable_hex_of_len(&share.extranonce2, (EXTRANONCE_2_SIZE as usize) * 2) {
        return submit_rejected(
            share,
            "bad_extranonce2",
            false,
            submit_error("invalid submit params"),
            0.0,
        );
    }

    if !is_decodable_hex_of_len(&share.ntime, 8) {
        return submit_rejected(
            share,
            "bad_ntime",
            false,
            submit_error("invalid submit params"),
            0.0,
        );
    }

    if !is_decodable_hex_of_len(&share.nonce, 8) {
        return submit_rejected(
            share,
            "bad_nonce",
            false,
            submit_error("invalid submit params"),
            0.0,
        );
    }

    if !share.version_bits.is_empty() && !is_decodable_hex_of_len(&share.version_bits, 8) {
        return submit_rejected(
            share,
            "bad_version_bits",
            false,
            submit_error("invalid submit params"),
            0.0,
        );
    }

    if !state.insert_share_key_if_new(share.duplicate_key()) {
        return submit_rejected(
            share,
            "duplicate",
            true,
            submit_error("duplicate share"),
            0.0,
        );
    }

    let Some(job_template) = state.get_job_template(&share.job_id) else {
        return submit_rejected(
            share,
            "unknown_job",
            false,
            submit_error("job not found"),
            0.0,
        );
    };

    let (meets_target, share_diff) =
        match validate_share_pow(&share, session_extranonce1, &job_template) {
            Ok(result) => result,
            Err(_) => {
                return submit_rejected(
                    share,
                    "invalid_job_template",
                    false,
                    submit_error("invalid job data"),
                    0.0,
                );
            }
        };

    if !meets_target {
        return submit_rejected(
            share,
            "low_difficulty_share",
            false,
            submit_error("low difficulty share"),
            share_diff,
        );
    }

    SubmitValidation::Accepted { share, share_diff }
}

fn partial_submit_fields(params: &Value) -> SubmitShare {
    let values = params.as_array();
    let get_string = |idx: usize| {
        values
            .and_then(|items| items.get(idx))
            .and_then(Value::as_str)
            .unwrap_or("")
            .to_string()
    };

    SubmitShare {
        worker: get_string(0),
        job_id: get_string(1),
        extranonce2: get_string(2),
        ntime: get_string(3),
        nonce: get_string(4),
        version_bits: get_string(5),
    }
}

fn is_decodable_hex_of_len(input: &str, expected_len: usize) -> bool {
    if input.len() != expected_len || input.len() % 2 != 0 {
        return false;
    }

    for bytes in input.as_bytes().chunks_exact(2) {
        let pair = match std::str::from_utf8(bytes) {
            Ok(pair) => pair,
            Err(_) => return false,
        };
        if u8::from_str_radix(pair, 16).is_err() {
            return false;
        }
    }

    true
}

fn submit_error(message: &str) -> Value {
    json!([23, message, Value::Null])
}

fn submit_rejected(
    share: SubmitShare,
    reason: &str,
    duplicate: bool,
    error: Value,
    share_diff: f64,
) -> SubmitValidation {
    SubmitValidation::Rejected {
        share,
        reason: reason.to_string(),
        duplicate,
        error,
        share_diff,
    }
}

fn validate_share_pow(
    share: &SubmitShare,
    session_extranonce1: &str,
    job_template: &JobTemplate,
) -> Result<(bool, f64), String> {
    let coinb1 = decode_hex(&job_template.coinb1)?;
    let coinb2 = decode_hex(&job_template.coinb2)?;
    let extranonce1 = decode_hex(session_extranonce1)?;
    let extranonce2 = decode_hex(&share.extranonce2)?;
    let mut coinbase =
        Vec::with_capacity(coinb1.len() + extranonce1.len() + extranonce2.len() + coinb2.len());
    coinbase.extend_from_slice(&coinb1);
    coinbase.extend_from_slice(&extranonce1);
    coinbase.extend_from_slice(&extranonce2);
    coinbase.extend_from_slice(&coinb2);
    let coinbase_hash_be = sha256d_bytes(&coinbase);

    let merkle_root_be = merkle_root_from_branch(coinbase_hash_be, &job_template.merkle_branch)?;

    let version = parse_u32_hex_exact(&job_template.version)?;
    if !share.version_bits.is_empty() {
        let _ = parse_u32_hex_exact(&share.version_bits)?;
    }
    let ntime = parse_u32_hex_exact(&share.ntime)?;
    let nbits = parse_u32_hex_exact(&job_template.nbits)?;
    let nonce = parse_u32_hex_exact(&share.nonce)?;
    let prevhash_be = decode_hex_exact(&job_template.prevhash, 64)?;
    let _job_ntime = parse_u32_hex_exact(&job_template.job_ntime)?;

    let mut prevhash_le = [0u8; 32];
    prevhash_le.copy_from_slice(&prevhash_be);
    prevhash_le.reverse();

    let mut merkle_root_le = merkle_root_be;
    merkle_root_le.reverse();

    let mut header = Vec::with_capacity(80);
    header.extend_from_slice(&version.to_le_bytes());
    header.extend_from_slice(&prevhash_le);
    header.extend_from_slice(&merkle_root_le);
    header.extend_from_slice(&ntime.to_le_bytes());
    header.extend_from_slice(&nbits.to_le_bytes());
    header.extend_from_slice(&nonce.to_le_bytes());

    let hash_be = sha256d_bytes(&header);
    let mut hash_le = hash_be;
    hash_le.reverse();

    let target_le = diff1_target_le();
    let meets_target = cmp_u256_le(&hash_le, &target_le).is_le();
    let share_diff = estimate_difficulty(&hash_le, &target_le);
    Ok((meets_target, share_diff))
}

fn decode_hex_exact(input: &str, expected_len: usize) -> Result<Vec<u8>, String> {
    if input.len() != expected_len {
        return Err("bad_hex_length".to_string());
    }
    decode_hex(input)
}

fn decode_hex(input: &str) -> Result<Vec<u8>, String> {
    if input.len() % 2 != 0 {
        return Err("bad_hex_length".to_string());
    }

    let mut output = Vec::with_capacity(input.len() / 2);
    for bytes in input.as_bytes().chunks_exact(2) {
        let pair = std::str::from_utf8(bytes).map_err(|_| "bad_hex".to_string())?;
        let value = u8::from_str_radix(pair, 16).map_err(|_| "bad_hex".to_string())?;
        output.push(value);
    }

    Ok(output)
}

fn parse_u32_hex_exact(input: &str) -> Result<u32, String> {
    let bytes = decode_hex_exact(input, 8)?;
    let mut array = [0u8; 4];
    array.copy_from_slice(&bytes);
    Ok(u32::from_be_bytes(array))
}

fn merkle_root_from_branch(
    coinbase_hash_be: [u8; 32],
    merkle_branch: &[String],
) -> Result<[u8; 32], String> {
    let mut current = coinbase_hash_be;
    for branch_entry in merkle_branch {
        let branch = decode_hex_exact(branch_entry, 64)?;
        let mut input = Vec::with_capacity(64);
        input.extend_from_slice(&current);
        input.extend_from_slice(&branch);
        current = sha256d_bytes(&input);
    }
    Ok(current)
}

fn sha256d_bytes(input: &[u8]) -> [u8; 32] {
    sha256d::Hash::hash(input).to_byte_array()
}

fn diff1_target_le() -> [u8; 32] {
    let target_be = decode_hex_exact(DIFF1_TARGET_HEX, 64).expect("diff1 target hex must be valid");
    let mut target_le = [0u8; 32];
    target_le.copy_from_slice(&target_be);
    target_le.reverse();
    target_le
}

fn cmp_u256_le(lhs: &[u8; 32], rhs: &[u8; 32]) -> std::cmp::Ordering {
    for idx in (0..32).rev() {
        if lhs[idx] < rhs[idx] {
            return std::cmp::Ordering::Less;
        }
        if lhs[idx] > rhs[idx] {
            return std::cmp::Ordering::Greater;
        }
    }
    std::cmp::Ordering::Equal
}

fn estimate_difficulty(hash_le: &[u8; 32], target_le: &[u8; 32]) -> f64 {
    let hash_f = uint256_le_to_f64(hash_le).max(1.0);
    let target_f = uint256_le_to_f64(target_le);
    target_f / hash_f
}

fn uint256_le_to_f64(value: &[u8; 32]) -> f64 {
    let mut result = 0.0f64;
    for idx in (0..32).rev() {
        result = (result * 256.0) + (value[idx] as f64);
    }
    result
}

fn build_set_difficulty_push(diff: u64) -> Value {
    json!({
        "id": Value::Null,
        "method": "mining.set_difficulty",
        "params": [diff]
    })
}

fn send_set_difficulty(
    writer: &mut BufWriter<TcpStream>,
    remote_addr: SocketAddr,
    session: &mut SessionState,
    diff: u64,
) -> io::Result<()> {
    if session.diff_sent {
        return Ok(());
    }

    let message = build_set_difficulty_push(diff);
    write_json_line(
        writer,
        remote_addr,
        session,
        "mining.set_difficulty",
        &message,
    )?;
    session.diff_sent = true;
    Ok(())
}

struct NotifyPush {
    job_id: String,
    job_template: JobTemplate,
    message: Value,
}

fn build_notify_push(
    template_state: &TemplatePollerState,
    state: &GatewayState,
    force_clean_jobs: bool,
) -> NotifyPush {
    let (work_key, previousblockhash, bits, curtime_hex) = match template_state.latest_template() {
        Some(template) => {
            let work_key = format!(
                "{}:{}:{}",
                template.previousblockhash, template.bits, template.curtime
            );
            (
                work_key,
                template.previousblockhash,
                template.bits,
                format!("{:08x}", template.curtime),
            )
        }
        None => (
            "fallback-no-template".to_string(),
            "0000000000000000000000000000000000000000000000000000000000000000".to_string(),
            "1d00ffff".to_string(),
            "00000000".to_string(),
        ),
    };
    let (job_id, clean_jobs) = state.allocate_or_reuse_job_id(&work_key, force_clean_jobs);
    let job_template = JobTemplate {
        prevhash: previousblockhash.clone(),
        coinb1: String::new(),
        coinb2: String::new(),
        merkle_branch: Vec::new(),
        version: "20000000".to_string(),
        nbits: bits.clone(),
        job_ntime: curtime_hex.clone(),
    };

    let message = json!({
        "id": Value::Null,
        "method": "mining.notify",
        "params": [
            job_id,
            previousblockhash,
            "",
            "",
            [],
            "20000000",
            bits,
            curtime_hex,
            clean_jobs
        ]
    });

    NotifyPush {
        job_id,
        job_template,
        message,
    }
}

fn write_json_line(
    writer: &mut BufWriter<TcpStream>,
    remote_addr: SocketAddr,
    session: &SessionState,
    method_for_log: &str,
    value: &Value,
) -> io::Result<()> {
    let line = value.to_string();
    println!(
        "STRATUM_TX session_id={} remote={} method={} line={line}",
        session.session_id, remote_addr, method_for_log
    );
    writer.write_all(line.as_bytes())?;
    writer.write_all(b"\n")?;
    writer.flush()?;
    Ok(())
}

fn generate_extranonce1_hex() -> String {
    let bytes: [u8; 8] = random();
    bytes_to_lower_hex(&bytes)
}

fn bytes_to_lower_hex(bytes: &[u8]) -> String {
    const HEX: [char; 16] = [
        '0', '1', '2', '3', '4', '5', '6', '7', '8', '9', 'a', 'b', 'c', 'd', 'e', 'f',
    ];

    let mut output = String::with_capacity(bytes.len() * 2);
    for &byte in bytes {
        output.push(HEX[(byte >> 4) as usize]);
        output.push(HEX[(byte & 0x0f) as usize]);
    }
    output
}

fn unix_seconds_now() -> u64 {
    match SystemTime::now().duration_since(UNIX_EPOCH) {
        Ok(duration) => duration.as_secs(),
        Err(_) => 0,
    }
}

fn unix_millis_now() -> i64 {
    match SystemTime::now().duration_since(UNIX_EPOCH) {
        Ok(duration) => duration.as_millis() as i64,
        Err(_) => 0,
    }
}

fn allocate_job_id() -> String {
    NEXT_JOB_ID_COUNTER
        .fetch_add(1, Ordering::Relaxed)
        .to_string()
}

fn allocate_session_id() -> u64 {
    NEXT_SESSION_ID_COUNTER.fetch_add(1, Ordering::Relaxed)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn sample_job_template() -> JobTemplate {
        JobTemplate {
            prevhash: "0000000000000000000000000000000000000000000000000000000000000000"
                .to_string(),
            coinb1: "".to_string(),
            coinb2: "".to_string(),
            merkle_branch: vec![],
            version: "20000000".to_string(),
            nbits: "1d00ffff".to_string(),
            job_ntime: "699dfecc".to_string(),
        }
    }

    #[test]
    fn parse_accepts_string_id_method_and_params_array() {
        let line = r#"{"id":"abc","method":"mining.subscribe","params":["test/0.1"]}"#;
        let request = parse_rpc_request(line).expect("request should parse");
        assert_eq!(request.method, "mining.subscribe");
        assert_eq!(request.id, Value::String("abc".to_string()));
        assert!(request.params.is_array());
        assert_eq!(
            request
                .params
                .as_array()
                .expect("params should be array")
                .len(),
            1
        );
    }

    #[test]
    fn parse_accepts_number_id() {
        let line = r#"{"id":2,"method":"mining.authorize","params":["user.worker","x"]}"#;
        let request = parse_rpc_request(line).expect("request should parse");
        assert!(request.id.is_number());
        assert_eq!(request.method, "mining.authorize");
    }

    #[test]
    fn parse_accepts_non_array_params_for_method_validation() {
        let line = r#"{"id":1,"method":"mining.subscribe","params":{"bad":true}}"#;
        let request = parse_rpc_request(line).expect("request should parse");
        assert!(request.params.is_object());
    }

    #[test]
    fn unknown_method_returns_method_not_found() {
        let request = RpcRequest {
            id: Value::from(99),
            method: "mining.nope".to_string(),
            params: Value::Array(vec![]),
        };
        let mut session_state = SessionState::default();
        let state = GatewayState::new();
        let response = build_response(
            &request,
            "127.0.0.1:12345".parse().expect("valid addr"),
            &mut session_state,
            &state,
        );
        assert_eq!(response["id"], Value::from(99));
        assert_eq!(response["error"]["code"], Value::from(-32601));
        assert_eq!(
            response["error"]["message"],
            Value::from("Method not found")
        );
    }

    #[test]
    fn mining_configure_returns_version_rolling_mask_when_requested() {
        let request = RpcRequest {
            id: Value::from(41),
            method: "mining.configure".to_string(),
            params: json!([
                json!(["version-rolling"]),
                json!({"version-rolling.mask":"1fffe000"}),
            ]),
        };
        let mut session_state = SessionState::default();
        let state = GatewayState::new();
        let response = build_response(
            &request,
            "127.0.0.1:12345".parse().expect("valid addr"),
            &mut session_state,
            &state,
        );
        assert_eq!(response["id"], Value::from(41));
        assert!(response["error"].is_null());
        assert_eq!(response["result"]["version-rolling"], Value::from(true));
        assert_eq!(
            response["result"]["version-rolling.mask"],
            Value::from("1fffe000")
        );
    }

    #[test]
    fn mining_configure_uses_default_mask_when_not_provided() {
        let request = RpcRequest {
            id: Value::from(42),
            method: "mining.configure".to_string(),
            params: json!([["minimum-difficulty", "version-rolling"]]),
        };
        let mut session_state = SessionState::default();
        let state = GatewayState::new();
        let response = build_response(
            &request,
            "127.0.0.1:12345".parse().expect("valid addr"),
            &mut session_state,
            &state,
        );
        assert_eq!(response["id"], Value::from(42));
        assert!(response["error"].is_null());
        assert_eq!(response["result"]["version-rolling"], Value::from(true));
        assert_eq!(
            response["result"]["version-rolling.mask"],
            Value::from("ffffffff")
        );
    }

    #[test]
    fn subscribe_and_authorize_return_success_without_error() {
        let subscribe = RpcRequest {
            id: Value::from(1),
            method: "mining.subscribe".to_string(),
            params: json!(["test/0.1"]),
        };
        let mut session_state = SessionState::default();
        let state = GatewayState::new();
        let subscribe_response = build_response(
            &subscribe,
            "127.0.0.1:12345".parse().expect("valid addr"),
            &mut session_state,
            &state,
        );
        assert_eq!(subscribe_response["id"], Value::from(1));
        assert!(subscribe_response["error"].is_null());
        assert!(subscribe_response["result"].is_array());
        let extranonce1 = subscribe_response["result"][1]
            .as_str()
            .expect("extranonce1 should be a string");
        assert_eq!(extranonce1.len(), 16);
        assert_eq!(subscribe_response["result"][2], Value::from(8));

        let authorize = RpcRequest {
            id: Value::from("auth-1"),
            method: "mining.authorize".to_string(),
            params: json!(["user.worker", "x"]),
        };
        let authorize_response = build_response(
            &authorize,
            "127.0.0.1:12345".parse().expect("valid addr"),
            &mut session_state,
            &state,
        );
        assert_eq!(authorize_response["id"], Value::from("auth-1"));
        assert_eq!(authorize_response["result"], Value::from(true));
        assert!(authorize_response["error"].is_null());
    }

    #[test]
    fn submit_accepts_valid_share_when_job_exists() {
        let request = RpcRequest {
            id: Value::from(10),
            method: "mining.submit".to_string(),
            params: json!([
                "BenC",
                "66",
                "a21d000000000000",
                "699dfecc",
                "19afcd42",
                "00010000"
            ]),
        };
        let mut session_state = SessionState::default();
        let _ = session_state.extranonce1_hex();
        let state = GatewayState::new();
        state.upsert_job_template("66".to_string(), sample_job_template());
        let response = build_response(
            &request,
            "127.0.0.1:12345".parse().expect("valid addr"),
            &mut session_state,
            &state,
        );
        assert_eq!(response["id"], Value::from(10));
        assert_eq!(response["result"], Value::from(false));
        assert_eq!(response["error"][0], Value::from(23));
    }

    #[test]
    fn submit_rejects_when_job_missing() {
        let request = RpcRequest {
            id: Value::from(11),
            method: "mining.submit".to_string(),
            params: json!([
                "BenC",
                "99",
                "a21d000000000000",
                "699dfecc",
                "19afcd42",
                "00010000"
            ]),
        };
        let mut session_state = SessionState::default();
        let _ = session_state.extranonce1_hex();
        let state = GatewayState::new();
        let response = build_response(
            &request,
            "127.0.0.1:12345".parse().expect("valid addr"),
            &mut session_state,
            &state,
        );
        assert_eq!(response["id"], Value::from(11));
        assert_eq!(response["result"], Value::from(false));
        assert_eq!(response["error"][0], Value::from(23));
        assert_eq!(response["error"][1], Value::from("job not found"));
    }

    #[test]
    fn submit_rejects_on_bad_hex_or_length() {
        let request = RpcRequest {
            id: Value::from(12),
            method: "mining.submit".to_string(),
            params: json!(["BenC", "66", "a21d0000", "699dfecc", "zzafcd42", "00010000"]),
        };
        let mut session_state = SessionState::default();
        let _ = session_state.extranonce1_hex();
        let state = GatewayState::new();
        state.upsert_job_template("66".to_string(), sample_job_template());
        let response = build_response(
            &request,
            "127.0.0.1:12345".parse().expect("valid addr"),
            &mut session_state,
            &state,
        );
        assert_eq!(response["id"], Value::from(12));
        assert_eq!(response["result"], Value::from(false));
        assert_eq!(response["error"][0], Value::from(23));
        assert_eq!(response["error"][1], Value::from("invalid submit params"));
    }

    #[test]
    fn submit_rejects_duplicates() {
        let request = RpcRequest {
            id: Value::from(13),
            method: "mining.submit".to_string(),
            params: json!([
                "BenC",
                "66",
                "a21d000000000000",
                "699dfecc",
                "19afcd42",
                "00010000"
            ]),
        };
        let mut session_state = SessionState::default();
        let _ = session_state.extranonce1_hex();
        let state = GatewayState::new();
        state.upsert_job_template("66".to_string(), sample_job_template());

        let first = build_response(
            &request,
            "127.0.0.1:12345".parse().expect("valid addr"),
            &mut session_state,
            &state,
        );
        let second = build_response(
            &request,
            "127.0.0.1:12345".parse().expect("valid addr"),
            &mut session_state,
            &state,
        );

        assert_eq!(first["result"], Value::from(false));
        assert_eq!(second["result"], Value::from(false));
        assert_eq!(second["error"][1], Value::from("duplicate share"));

        let (shares_ok, shares_rej, shares_dup) = state.share_counters();
        assert_eq!(shares_ok, 0);
        assert_eq!(shares_rej, 2);
        assert_eq!(shares_dup, 1);
    }

    #[test]
    fn submit_returns_invalid_params_for_non_array_params() {
        let request = RpcRequest {
            id: Value::from(14),
            method: "mining.submit".to_string(),
            params: json!({"bad":true}),
        };
        let mut session_state = SessionState::default();
        let state = GatewayState::new();
        let response = build_response(
            &request,
            "127.0.0.1:12345".parse().expect("valid addr"),
            &mut session_state,
            &state,
        );
        assert_eq!(response["id"], Value::from(14));
        assert_eq!(response["result"], Value::from(false));
        assert_eq!(response["error"][0], Value::from(23));
        assert_eq!(response["error"][1], Value::from("invalid submit params"));
    }

    #[test]
    fn job_id_is_stable_when_work_does_not_change() {
        let state = GatewayState::new();
        let (job_1, clean_jobs_1) =
            state.allocate_or_reuse_job_id("work:height10:prevhash-a", false);
        let (job_2, clean_jobs_2) =
            state.allocate_or_reuse_job_id("work:height10:prevhash-a", false);

        assert_eq!(job_1, job_2);
        assert!(clean_jobs_1);
        assert!(!clean_jobs_2);
        assert!(state.has_active_job(&job_1));
    }

    #[test]
    fn job_id_changes_when_work_changes_or_clean_jobs_forced() {
        let state = GatewayState::new();
        let (job_1, _) = state.allocate_or_reuse_job_id("work:height10:prevhash-a", false);
        let (job_2, clean_jobs_2) =
            state.allocate_or_reuse_job_id("work:height11:prevhash-b", false);
        let (job_3, clean_jobs_3) =
            state.allocate_or_reuse_job_id("work:height11:prevhash-b", true);

        assert_ne!(job_1, job_2);
        assert_ne!(job_2, job_3);
        assert!(clean_jobs_2);
        assert!(clean_jobs_3);
        assert!(state.has_active_job(&job_2));
        assert!(state.has_active_job(&job_3));
    }

    #[test]
    fn notify_reuses_job_id_and_sets_clean_jobs_false_for_resend() {
        let state = GatewayState::new();
        let template_state = TemplatePollerState::default();

        let first = build_notify_push(&template_state, &state, false);
        let second = build_notify_push(&template_state, &state, false);

        assert_eq!(first.message["params"][0], second.message["params"][0]);
        assert_eq!(first.message["params"][8], Value::from(true));
        assert_eq!(second.message["params"][8], Value::from(false));
    }

    #[test]
    fn notify_allocates_new_job_id_when_clean_jobs_forced() {
        let state = GatewayState::new();
        let template_state = TemplatePollerState::default();

        let first = build_notify_push(&template_state, &state, false);
        let forced = build_notify_push(&template_state, &state, true);

        assert_ne!(first.message["params"][0], forced.message["params"][0]);
        assert_eq!(forced.message["params"][8], Value::from(true));
    }

    #[test]
    fn worker_accounting_tracks_accept_reject_last_seen_and_best_diff() {
        let state = GatewayState::new();
        state.record_worker_share("worker-1", true, 8.0, 100);
        state.record_worker_share("worker-1", false, 3.5, 101);
        state.record_worker_share("worker-1", true, 12.25, 102);

        let guard = state
            .worker_stats
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        let stats = guard.get("worker-1").expect("worker stats should exist");

        assert_eq!(stats.accepted, 2);
        assert_eq!(stats.rejected, 1);
        assert_eq!(stats.last_seen, 102);
        assert_eq!(stats.best_share_diff, 12.25);
    }

    #[test]
    fn worker_accounting_ignores_empty_worker_name() {
        let state = GatewayState::new();
        state.record_worker_share("", true, 100.0, 999);

        let guard = state
            .worker_stats
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        assert!(guard.is_empty());
    }

    #[test]
    fn submit_path_updates_worker_stats_via_handler() {
        let request = RpcRequest {
            id: Value::from(55),
            method: "mining.submit".to_string(),
            params: json!([
                "worker-e2e",
                "job-1",
                "a21d000000000000",
                "699dfecc",
                "19afcd42"
            ]),
        };
        let remote: SocketAddr = "127.0.0.1:12345".parse().expect("valid addr");
        let session_state = SessionState::default();
        let state = GatewayState::new();

        let accepted_share = SubmitShare {
            worker: "worker-e2e".to_string(),
            job_id: "job-1".to_string(),
            extranonce2: "a21d000000000000".to_string(),
            ntime: "699dfecc".to_string(),
            nonce: "19afcd42".to_string(),
            version_bits: String::new(),
        };

        let accepted_response = build_submit_response_with_validator(
            &request,
            remote,
            &session_state,
            &state,
            100,
            |_params, _session, _state| SubmitValidation::Accepted {
                share: accepted_share,
                share_diff: 3.5,
            },
        );
        assert_eq!(accepted_response["result"], Value::from(true));

        let rejected_share = SubmitShare {
            worker: "worker-e2e".to_string(),
            job_id: "job-1".to_string(),
            extranonce2: "a21d000000000000".to_string(),
            ntime: "699dfecc".to_string(),
            nonce: "19afcd42".to_string(),
            version_bits: String::new(),
        };

        let rejected_response = build_submit_response_with_validator(
            &request,
            remote,
            &session_state,
            &state,
            101,
            |_params, _session, _state| SubmitValidation::Rejected {
                share: rejected_share,
                reason: "low_difficulty_share".to_string(),
                duplicate: false,
                error: submit_error("low difficulty share"),
                share_diff: 0.1,
            },
        );
        assert_eq!(rejected_response["result"], Value::from(false));

        let guard = state
            .worker_stats
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        let stats = guard
            .get("worker-e2e")
            .expect("worker stats should exist after submits");
        assert_eq!(stats.accepted, 1);
        assert_eq!(stats.rejected, 1);
        assert_eq!(stats.best_share_diff, 3.5);
        assert_eq!(stats.last_seen, 101);
    }
}
