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
use gbt::poller::{LatestTemplate, TemplatePollerState};

const SUBSCRIPTION_ID: &str = "1";
const EXTRANONCE_1_SIZE: usize = 8;
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
    share_rej_log_counter: AtomicU64,
    notify_real_total: AtomicU64,
    notify_fallback_total: AtomicU64,
    last_fallback_reason: Mutex<String>,
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
            share_rej_log_counter: AtomicU64::new(0),
            notify_real_total: AtomicU64::new(0),
            notify_fallback_total: AtomicU64::new(0),
            last_fallback_reason: Mutex::new("none".to_string()),
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

    fn record_notify_real(&self) {
        self.notify_real_total.fetch_add(1, Ordering::Relaxed);
    }

    fn record_notify_fallback(&self, reason: NotifyFallbackReason) {
        self.notify_fallback_total.fetch_add(1, Ordering::Relaxed);
        let mut guard = self
            .last_fallback_reason
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        *guard = reason.as_str().to_string();
    }

    fn notify_counters(&self) -> (u64, u64, String) {
        let last_reason = self
            .last_fallback_reason
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .clone();
        (
            self.notify_real_total.load(Ordering::Relaxed),
            self.notify_fallback_total.load(Ordering::Relaxed),
            last_reason,
        )
    }

    fn should_log_share_reject(&self) -> bool {
        let current = self.share_rej_log_counter.fetch_add(1, Ordering::Relaxed) + 1;
        current == 1 || current % 100 == 0
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
    duplicate: bool,
    share_diff: f64,
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
        let reason = if event.accepted {
            String::new()
        } else {
            event.reason.clone().unwrap_or_default()
        };
        let duplicate = event.duplicate || reason == "duplicate";
        let share_diff = if event.share_diff.is_finite() {
            event.share_diff
        } else {
            0.0
        };
        let api_payload = json!({
            "ts": event.ts,
            "worker": event.worker,
            "job_id": event.job_id,
            "extranonce2": event.extranonce2,
            "ntime": event.ntime,
            "nonce": event.nonce,
            "accepted": event.accepted,
            "duplicate": duplicate,
            "share_diff": share_diff,
            "reason": reason
        });
        println!("SHARE_HTTP payload={}", api_payload.to_string());
        let mut request = client.post(&config.endpoint).json(&api_payload);
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
                    redact_url_credentials(&config.endpoint),
                    response.status().as_u16()
                );
            }
            Err(error) => {
                forwarding_counters.inc_fwd_fail();
                eprintln!(
                    "SHARE_HTTP request_failed endpoint={} error=\"{}\"",
                    redact_url_credentials(&config.endpoint),
                    error
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
        let (notify_real_total, notify_fallback_total, last_fallback_reason) =
            state.notify_counters();
        let total_decisions = shares_ok + shares_rej;
        let accept_rate = if total_decisions == 0 {
            0.0
        } else {
            shares_ok as f64 / total_decisions as f64
        };
        let validate_shares = gateway_validate_shares_enabled();
        println!(
            "HEALTH ok uptime={uptime_secs}s sessions={sessions} jobs={jobs} shares_ok={shares_ok} shares_rej={shares_rej} shares_dup={shares_dup} accept_rate={accept_rate:.3} validate_shares={validate_shares} notify_real_total={notify_real_total} notify_fallback_total={notify_fallback_total} last_fallback_reason={last_fallback_reason} shares_fwd_ok={shares_fwd_ok} shares_fwd_fail={shares_fwd_fail} shares_drop={shares_drop}"
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
        let logged_line = sanitize_stratum_log_line(trimmed);
        println!("STRATUM_RX remote={remote_addr} line={logged_line}");

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
            let reason_for_event = reason.clone();
            emit_share_event(
                state,
                remote_addr,
                &share,
                false,
                Some(reason_for_event),
                duplicate,
                share_diff,
            );
            if gateway_validate_shares_enabled() && state.should_log_share_reject() {
                eprintln!(
                    "SHARE_REJ reason={} job={} err=\"{}\"",
                    share_reject_reason_token(&reason),
                    share.job_id,
                    compact_for_log(&reason)
                );
            }
            json!({
                "id": request.id,
                "result": false,
                "error": error
            })
        }
        SubmitValidation::Accepted { share, share_diff } => {
            state.record_accepted_share();
            state.record_worker_share(&share.worker, true, share_diff, now);
            emit_share_event(state, remote_addr, &share, true, None, false, share_diff);
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
    duplicate: bool,
    share_diff: f64,
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
        duplicate,
        share_diff,
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
    validate_submit_params_with_mode(
        params,
        session_state,
        state,
        gateway_validate_shares_enabled(),
    )
}

fn validate_submit_params_with_mode(
    params: &Value,
    session_state: &SessionState,
    state: &GatewayState,
    validate_shares: bool,
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

    if !validate_shares {
        return SubmitValidation::Accepted {
            share,
            share_diff: 0.0,
        };
    }

    let (meets_target, share_diff) =
        match validate_share_pow(&share, session_extranonce1, &job_template) {
            Ok(result) => result,
            Err(error) => {
                return submit_rejected(
                    share,
                    error.reason_code(),
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

fn gateway_validate_shares_enabled() -> bool {
    env::var("GATEWAY_VALIDATE_SHARES")
        .ok()
        .map(|value| {
            matches!(
                value.trim().to_ascii_lowercase().as_str(),
                "1" | "true" | "yes" | "on"
            )
        })
        .unwrap_or(false)
}

fn share_reject_reason_token(reason: &str) -> &'static str {
    match reason {
        "bad_ntime" => "BadNtime",
        "bad_nonce" => "BadNonce",
        "unknown_job" => "JobNotFound",
        "coinbase_hash_mismatch" => "CoinbaseHashMismatch",
        "merkle_mismatch" => "MerkleMismatch",
        "low_difficulty_share" => "TargetMismatch",
        _ => "InternalError",
    }
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

#[derive(Debug, Clone)]
enum SharePowError {
    CoinbaseHashMismatch,
    MerkleMismatch,
    InternalError,
}

impl SharePowError {
    fn reason_code(&self) -> &'static str {
        match self {
            SharePowError::CoinbaseHashMismatch => "coinbase_hash_mismatch",
            SharePowError::MerkleMismatch => "merkle_mismatch",
            SharePowError::InternalError => "internal_error",
        }
    }
}

fn validate_share_pow(
    share: &SubmitShare,
    session_extranonce1: &str,
    job_template: &JobTemplate,
) -> Result<(bool, f64), SharePowError> {
    let coinb1 =
        decode_hex(&job_template.coinb1).map_err(|_| SharePowError::CoinbaseHashMismatch)?;
    let coinb2 =
        decode_hex(&job_template.coinb2).map_err(|_| SharePowError::CoinbaseHashMismatch)?;
    let extranonce1 =
        decode_hex(session_extranonce1).map_err(|_| SharePowError::CoinbaseHashMismatch)?;
    let extranonce2 =
        decode_hex(&share.extranonce2).map_err(|_| SharePowError::CoinbaseHashMismatch)?;
    let mut coinbase =
        Vec::with_capacity(coinb1.len() + extranonce1.len() + extranonce2.len() + coinb2.len());
    coinbase.extend_from_slice(&coinb1);
    coinbase.extend_from_slice(&extranonce1);
    coinbase.extend_from_slice(&extranonce2);
    coinbase.extend_from_slice(&coinb2);
    let coinbase_hash_be = sha256d_bytes(&coinbase);

    let merkle_root_be = merkle_root_from_branch(coinbase_hash_be, &job_template.merkle_branch)
        .map_err(|_| SharePowError::MerkleMismatch)?;

    let version =
        parse_u32_hex_exact(&job_template.version).map_err(|_| SharePowError::InternalError)?;
    if !share.version_bits.is_empty() {
        let _ =
            parse_u32_hex_exact(&share.version_bits).map_err(|_| SharePowError::InternalError)?;
    }
    let ntime = parse_u32_hex_exact(&share.ntime).map_err(|_| SharePowError::InternalError)?;
    let nbits =
        parse_u32_hex_exact(&job_template.nbits).map_err(|_| SharePowError::InternalError)?;
    let nonce = parse_u32_hex_exact(&share.nonce).map_err(|_| SharePowError::InternalError)?;
    let prevhash_be =
        decode_hex_exact(&job_template.prevhash, 64).map_err(|_| SharePowError::InternalError)?;
    let _job_ntime =
        parse_u32_hex_exact(&job_template.job_ntime).map_err(|_| SharePowError::InternalError)?;

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

#[derive(Debug, Clone)]
struct NotifyBuildData {
    work_key: String,
    template_height: Option<u64>,
    previousblockhash: String,
    bits: String,
    curtime_hex: String,
    version: String,
    segwit_active: bool,
    has_witness_commitment: bool,
    coinb1: String,
    coinb2: String,
    merkle_branch: Vec<String>,
    tx_count: usize,
}

#[derive(Debug, Clone)]
struct NotifyJobParts {
    coinb1: String,
    coinb2: String,
    merkle_branch: Vec<String>,
    has_witness_commitment: bool,
}

#[derive(Debug, Clone, Copy)]
enum NotifyFallbackReason {
    TemplateUnavailable,
    TemplateMissingTxs,
    CoinbaseNotReady,
    PrevhashMissingOrBad,
    MerkleBuildFailed,
    InternalError,
    TimingRace,
}

impl NotifyFallbackReason {
    fn as_str(self) -> &'static str {
        match self {
            NotifyFallbackReason::TemplateUnavailable => "TemplateUnavailable",
            NotifyFallbackReason::TemplateMissingTxs => "TemplateMissingTxs",
            NotifyFallbackReason::CoinbaseNotReady => "CoinbaseNotReady",
            NotifyFallbackReason::PrevhashMissingOrBad => "PrevhashMissingOrBad",
            NotifyFallbackReason::MerkleBuildFailed => "MerkleBuildFailed",
            NotifyFallbackReason::InternalError => "InternalError",
            NotifyFallbackReason::TimingRace => "TimingRace",
        }
    }
}

#[derive(Debug, Clone)]
struct NotifyBuildError {
    reason: NotifyFallbackReason,
    summary: String,
}

impl NotifyBuildError {
    fn new(reason: NotifyFallbackReason, summary: impl Into<String>) -> Self {
        Self {
            reason,
            summary: summary.into(),
        }
    }
}

fn build_notify_push(
    template_state: &TemplatePollerState,
    state: &GatewayState,
    force_clean_jobs: bool,
) -> NotifyPush {
    let template_seq = template_state.current_job_counter();
    let (notify_data, fallback_error) = match template_state.latest_template() {
        Some(template) => {
            let work_key = build_work_key_from_template(&template);
            match build_notify_data_from_template(&template, work_key.clone()) {
                Ok(data) => (data, None),
                Err(error) => (
                    fallback_notify_data_for_template(&template, work_key),
                    Some(error),
                ),
            }
        }
        None => (
            fallback_notify_data(),
            Some(NotifyBuildError::new(
                NotifyFallbackReason::TemplateUnavailable,
                "no template yet",
            )),
        ),
    };
    let (job_id, clean_jobs) =
        state.allocate_or_reuse_job_id(&notify_data.work_key, force_clean_jobs);
    let height_for_log = notify_data
        .template_height
        .map(|height| height.to_string())
        .unwrap_or_else(|| "?".to_string());
    if let Some(error) = fallback_error {
        state.record_notify_fallback(error.reason);
        eprintln!(
            "NOTIFY_FALLBACK reason={} job={} height={} template_seq={} err=\"{}\"",
            error.reason.as_str(),
            job_id,
            height_for_log,
            template_seq,
            compact_for_log(&error.summary)
        );
    } else {
        state.record_notify_real();
        println!(
            "NOTIFY_REAL job={} height={} template_seq={} txs={} coinb1_len={} coinb2_len={} merkle_branches={} segwit_active={} has_witness_commitment={}",
            job_id,
            height_for_log,
            template_seq,
            notify_data.tx_count,
            notify_data.coinb1.len(),
            notify_data.coinb2.len(),
            notify_data.merkle_branch.len(),
            notify_data.segwit_active,
            notify_data.has_witness_commitment
        );
    }

    let job_template = JobTemplate {
        prevhash: notify_data.previousblockhash.clone(),
        coinb1: notify_data.coinb1.clone(),
        coinb2: notify_data.coinb2.clone(),
        merkle_branch: notify_data.merkle_branch.clone(),
        version: notify_data.version.clone(),
        nbits: notify_data.bits.clone(),
        job_ntime: notify_data.curtime_hex.clone(),
    };

    let message = json!({
        "id": Value::Null,
        "method": "mining.notify",
        "params": [
            job_id,
            notify_data.previousblockhash,
            notify_data.coinb1,
            notify_data.coinb2,
            notify_data.merkle_branch,
            notify_data.version,
            notify_data.bits,
            notify_data.curtime_hex,
            clean_jobs
        ]
    });

    NotifyPush {
        job_id,
        job_template,
        message,
    }
}

fn fallback_notify_data() -> NotifyBuildData {
    NotifyBuildData {
        work_key: "fallback-no-template".to_string(),
        template_height: None,
        previousblockhash: "0000000000000000000000000000000000000000000000000000000000000000"
            .to_string(),
        bits: "1d00ffff".to_string(),
        curtime_hex: "00000000".to_string(),
        version: "20000000".to_string(),
        segwit_active: false,
        has_witness_commitment: false,
        coinb1: String::new(),
        coinb2: String::new(),
        merkle_branch: Vec::new(),
        tx_count: 0,
    }
}

fn fallback_notify_data_for_template(
    template: &LatestTemplate,
    work_key: String,
) -> NotifyBuildData {
    NotifyBuildData {
        work_key,
        template_height: Some(template.height),
        previousblockhash: template.previousblockhash.clone(),
        bits: template.bits.clone(),
        curtime_hex: format!("{:08x}", template.curtime),
        version: template.version.clone(),
        segwit_active: template.segwit_active,
        has_witness_commitment: false,
        coinb1: String::new(),
        coinb2: String::new(),
        merkle_branch: Vec::new(),
        tx_count: template.transaction_count,
    }
}

fn build_notify_data_from_template(
    template: &LatestTemplate,
    work_key: String,
) -> Result<NotifyBuildData, NotifyBuildError> {
    if decode_hex_exact(&template.previousblockhash, 64).is_err() {
        return Err(NotifyBuildError::new(
            NotifyFallbackReason::PrevhashMissingOrBad,
            "previousblockhash must be 32-byte hex",
        ));
    }
    if parse_u32_hex_exact(&template.version).is_err()
        || parse_u32_hex_exact(&template.bits).is_err()
    {
        return Err(NotifyBuildError::new(
            NotifyFallbackReason::InternalError,
            "template version/bits must be 4-byte hex",
        ));
    }

    let parts = build_real_notify_job_parts(template)?;
    Ok(NotifyBuildData {
        work_key,
        template_height: Some(template.height),
        previousblockhash: template.previousblockhash.clone(),
        bits: template.bits.clone(),
        curtime_hex: format!("{:08x}", template.curtime),
        version: template.version.clone(),
        segwit_active: template.segwit_active,
        has_witness_commitment: parts.has_witness_commitment,
        coinb1: parts.coinb1,
        coinb2: parts.coinb2,
        merkle_branch: parts.merkle_branch,
        tx_count: template.transaction_count,
    })
}

fn build_work_key_from_template(template: &LatestTemplate) -> String {
    let mut material = format!(
        "{}:{}:{}:{}:{}:{}",
        template.height,
        template.previousblockhash,
        template.bits,
        template.curtime,
        template.version,
        template.coinbase_value.unwrap_or_default()
    );
    material.push(':');
    material.push_str(if template.segwit_active {
        "segwit-on"
    } else {
        "segwit-off"
    });
    if !template.rules.is_empty() {
        material.push(':');
        material.push_str(&template.rules.join(","));
    }
    if let Some(coinbase_txn_hex) = template.coinbase_txn_hex.as_deref() {
        material.push(':');
        material.push_str(coinbase_txn_hex);
    }
    if let Some(coinbase_aux_flags_hex) = template.coinbase_aux_flags_hex.as_deref() {
        material.push(':');
        material.push_str(coinbase_aux_flags_hex);
    }
    if let Some(default_witness_commitment_hex) = template.default_witness_commitment_hex.as_deref()
    {
        material.push(':');
        material.push_str(default_witness_commitment_hex);
    }
    for tx_hash in &template.transaction_hashes {
        material.push(':');
        material.push_str(tx_hash);
    }

    let work_hash = sha256d_bytes(material.as_bytes());
    format!(
        "{}:{}:{}:{}:{}:{}",
        template.height,
        template.previousblockhash,
        template.bits,
        template.curtime,
        template.version,
        bytes_to_lower_hex(&work_hash)
    )
}

fn build_real_notify_job_parts(
    template: &LatestTemplate,
) -> Result<NotifyJobParts, NotifyBuildError> {
    if template.transaction_hashes.len() != template.transaction_count {
        let reason = if template.transaction_hashes.is_empty() && template.transaction_count > 0 {
            NotifyFallbackReason::TemplateMissingTxs
        } else {
            NotifyFallbackReason::TimingRace
        };
        return Err(NotifyBuildError::new(
            reason,
            format!(
                "tx_hashes={} transaction_count={}",
                template.transaction_hashes.len(),
                template.transaction_count
            ),
        ));
    }

    let coinbase_value = template.coinbase_value.ok_or_else(|| {
        NotifyBuildError::new(
            NotifyFallbackReason::CoinbaseNotReady,
            "template missing coinbasevalue",
        )
    })?;

    let (coinb1, coinb2, has_witness_commitment) =
        build_coinbase_parts_from_template(template, coinbase_value)
            .map_err(|error| NotifyBuildError::new(NotifyFallbackReason::InternalError, error))?;

    let coinbase_hash_be = coinbase_hash_from_notify_parts(&coinb1, &coinb2)
        .map_err(|error| NotifyBuildError::new(NotifyFallbackReason::InternalError, error))?;
    let merkle_branch =
        build_coinbase_merkle_branch(coinbase_hash_be, &template.transaction_hashes).map_err(
            |error| NotifyBuildError::new(NotifyFallbackReason::MerkleBuildFailed, error),
        )?;

    Ok(NotifyJobParts {
        coinb1,
        coinb2,
        merkle_branch,
        has_witness_commitment,
    })
}

fn build_coinbase_parts_from_template(
    template: &LatestTemplate,
    coinbase_value: u64,
) -> Result<(String, String, bool), String> {
    let mut script_sig_prefix = serialize_bip34_height_push(template.height)?;
    if let Some(flags_hex) = template.coinbase_aux_flags_hex.as_deref() {
        if let Ok(flags) = decode_hex(flags_hex) {
            script_sig_prefix.extend_from_slice(&flags);
        }
    }

    let extranonce_len = EXTRANONCE_1_SIZE
        .checked_add(EXTRANONCE_2_SIZE as usize)
        .ok_or_else(|| "coinbase_extranonce_overflow".to_string())?;
    let script_sig_len = script_sig_prefix
        .len()
        .checked_add(extranonce_len)
        .ok_or_else(|| "coinbase_scriptsig_overflow".to_string())?;

    let mut coinb1 = Vec::new();
    coinb1.extend_from_slice(&1u32.to_le_bytes());
    coinb1.extend_from_slice(&encode_varint(1));
    coinb1.extend_from_slice(&[0u8; 32]);
    coinb1.extend_from_slice(&u32::MAX.to_le_bytes());
    coinb1.extend_from_slice(&encode_varint(script_sig_len as u64));
    coinb1.extend_from_slice(&script_sig_prefix);

    let payout_script = default_coinbase_payout_script();
    let witness_commitment_script = if template.segwit_active {
        template
            .default_witness_commitment_hex
            .as_deref()
            .and_then(|hex| decode_hex(hex).ok())
            .filter(|script| !script.is_empty())
    } else {
        None
    };
    let has_witness_commitment = witness_commitment_script.is_some();
    let mut output_count = 1usize;
    if has_witness_commitment {
        output_count += 1;
    }

    let mut coinb2 = Vec::new();
    coinb2.extend_from_slice(&u32::MAX.to_le_bytes());
    coinb2.extend_from_slice(&encode_varint(output_count as u64));
    coinb2.extend_from_slice(&coinbase_value.to_le_bytes());
    coinb2.extend_from_slice(&encode_varint(payout_script.len() as u64));
    coinb2.extend_from_slice(&payout_script);
    if let Some(commitment_script) = witness_commitment_script {
        coinb2.extend_from_slice(&0u64.to_le_bytes());
        coinb2.extend_from_slice(&encode_varint(commitment_script.len() as u64));
        coinb2.extend_from_slice(&commitment_script);
    }
    coinb2.extend_from_slice(&0u32.to_le_bytes());

    Ok((
        bytes_to_lower_hex(&coinb1),
        bytes_to_lower_hex(&coinb2),
        has_witness_commitment,
    ))
}

fn serialize_bip34_height_push(height: u64) -> Result<Vec<u8>, String> {
    let mut encoded = Vec::new();
    let mut value = height;
    while value > 0 {
        encoded.push((value & 0xff) as u8);
        value >>= 8;
    }
    if encoded.is_empty() {
        encoded.push(0);
    }
    if encoded.last().copied().unwrap_or(0) & 0x80 != 0 {
        encoded.push(0);
    }
    if encoded.len() > 75 {
        return Err("coinbase_height_push_too_large".to_string());
    }

    let mut script = Vec::with_capacity(1 + encoded.len());
    script.push(encoded.len() as u8);
    script.extend_from_slice(&encoded);
    Ok(script)
}

fn default_coinbase_payout_script() -> Vec<u8> {
    // Placeholder OP_TRUE script keeps a structurally valid coinbase transaction.
    vec![0x51]
}

fn coinbase_hash_from_notify_parts(coinb1_hex: &str, coinb2_hex: &str) -> Result<[u8; 32], String> {
    let mut coinbase = decode_hex(coinb1_hex)?;
    coinbase.extend(std::iter::repeat(0u8).take(EXTRANONCE_1_SIZE + EXTRANONCE_2_SIZE as usize));
    let coinb2 = decode_hex(coinb2_hex)?;
    coinbase.extend_from_slice(&coinb2);
    Ok(sha256d_bytes(&coinbase))
}

#[allow(dead_code)]
fn split_coinbase_for_extranonce(
    coinbase_tx_hex: &str,
    extranonce1_size: usize,
    extranonce2_size: usize,
) -> Result<(String, String), String> {
    let coinbase_tx = decode_hex(coinbase_tx_hex)?;
    if coinbase_tx.len() < 4 {
        return Err("coinbase_too_short".to_string());
    }

    let mut cursor = 4usize;
    if coinbase_tx.get(cursor) == Some(&0x00) && coinbase_tx.get(cursor + 1) == Some(&0x01) {
        cursor += 2;
    }

    let input_count = read_varint(&coinbase_tx, &mut cursor)?;
    if input_count == 0 {
        return Err("coinbase_missing_inputs".to_string());
    }

    if coinbase_tx.len() < cursor + 36 {
        return Err("coinbase_input_truncated".to_string());
    }
    cursor += 36;

    let script_len_offset = cursor;
    let script_len = read_varint(&coinbase_tx, &mut cursor)? as usize;
    let script_start = cursor;
    let script_end = script_start
        .checked_add(script_len)
        .ok_or_else(|| "coinbase_script_overflow".to_string())?;
    if coinbase_tx.len() < script_end {
        return Err("coinbase_script_truncated".to_string());
    }

    let extranonce_total_size = extranonce1_size
        .checked_add(extranonce2_size)
        .ok_or_else(|| "coinbase_extranonce_overflow".to_string())?;
    let patched_script_len = script_len
        .checked_add(extranonce_total_size)
        .ok_or_else(|| "coinbase_script_len_overflow".to_string())?;

    let mut coinb1 = Vec::with_capacity(script_end + 9);
    coinb1.extend_from_slice(&coinbase_tx[..script_len_offset]);
    coinb1.extend_from_slice(&encode_varint(patched_script_len as u64));
    coinb1.extend_from_slice(&coinbase_tx[script_start..script_end]);

    let coinb2 = coinbase_tx[script_end..].to_vec();

    Ok((bytes_to_lower_hex(&coinb1), bytes_to_lower_hex(&coinb2)))
}

fn read_varint(input: &[u8], cursor: &mut usize) -> Result<u64, String> {
    let prefix = *input
        .get(*cursor)
        .ok_or_else(|| "varint_prefix_missing".to_string())?;
    *cursor += 1;

    match prefix {
        0x00..=0xfc => Ok(prefix as u64),
        0xfd => {
            if input.len() < *cursor + 2 {
                return Err("varint_u16_truncated".to_string());
            }
            let mut bytes = [0u8; 2];
            bytes.copy_from_slice(&input[*cursor..(*cursor + 2)]);
            *cursor += 2;
            Ok(u16::from_le_bytes(bytes) as u64)
        }
        0xfe => {
            if input.len() < *cursor + 4 {
                return Err("varint_u32_truncated".to_string());
            }
            let mut bytes = [0u8; 4];
            bytes.copy_from_slice(&input[*cursor..(*cursor + 4)]);
            *cursor += 4;
            Ok(u32::from_le_bytes(bytes) as u64)
        }
        0xff => {
            if input.len() < *cursor + 8 {
                return Err("varint_u64_truncated".to_string());
            }
            let mut bytes = [0u8; 8];
            bytes.copy_from_slice(&input[*cursor..(*cursor + 8)]);
            *cursor += 8;
            Ok(u64::from_le_bytes(bytes))
        }
    }
}

fn encode_varint(value: u64) -> Vec<u8> {
    match value {
        0x00..=0xfc => vec![value as u8],
        0xfd..=0xffff => {
            let mut encoded = Vec::with_capacity(3);
            encoded.push(0xfd);
            encoded.extend_from_slice(&(value as u16).to_le_bytes());
            encoded
        }
        0x1_0000..=0xffff_ffff => {
            let mut encoded = Vec::with_capacity(5);
            encoded.push(0xfe);
            encoded.extend_from_slice(&(value as u32).to_le_bytes());
            encoded
        }
        _ => {
            let mut encoded = Vec::with_capacity(9);
            encoded.push(0xff);
            encoded.extend_from_slice(&value.to_le_bytes());
            encoded
        }
    }
}

fn decode_hex_u256(input: &str) -> Result<[u8; 32], String> {
    let decoded = decode_hex_exact(input, 64)?;
    let mut bytes = [0u8; 32];
    bytes.copy_from_slice(&decoded);
    Ok(bytes)
}

fn hash_merkle_pair(left: &[u8; 32], right: &[u8; 32]) -> [u8; 32] {
    let mut input = [0u8; 64];
    input[..32].copy_from_slice(left);
    input[32..].copy_from_slice(right);
    sha256d_bytes(&input)
}

fn build_coinbase_merkle_branch(
    coinbase_hash_be: [u8; 32],
    transaction_hashes: &[String],
) -> Result<Vec<String>, String> {
    if transaction_hashes.is_empty() {
        return Ok(Vec::new());
    }

    let mut level = Vec::with_capacity(transaction_hashes.len() + 1);
    level.push(coinbase_hash_be);
    for tx_hash in transaction_hashes {
        level.push(decode_hex_u256(tx_hash)?);
    }

    let mut coinbase_index = 0usize;
    let mut branch = Vec::new();

    while level.len() > 1 {
        let sibling_index = if coinbase_index % 2 == 0 {
            coinbase_index + 1
        } else {
            coinbase_index - 1
        };
        let sibling = if sibling_index < level.len() {
            level[sibling_index]
        } else {
            level[coinbase_index]
        };
        branch.push(bytes_to_lower_hex(&sibling));

        if level.len() % 2 == 1 {
            let last = *level.last().expect("non-empty merkle level");
            level.push(last);
        }

        let mut next_level = Vec::with_capacity(level.len() / 2);
        for pair in level.chunks_exact(2) {
            next_level.push(hash_merkle_pair(&pair[0], &pair[1]));
        }
        level = next_level;
        coinbase_index /= 2;
    }

    Ok(branch)
}

fn write_json_line(
    writer: &mut BufWriter<TcpStream>,
    remote_addr: SocketAddr,
    session: &SessionState,
    method_for_log: &str,
    value: &Value,
) -> io::Result<()> {
    let line = value.to_string();
    let line_for_log = sanitize_stratum_log_line(&line);
    println!(
        "STRATUM_TX session_id={} remote={} method={} line={line_for_log}",
        session.session_id, remote_addr, method_for_log
    );
    writer.write_all(line.as_bytes())?;
    writer.write_all(b"\n")?;
    writer.flush()?;
    Ok(())
}

fn sanitize_stratum_log_line(line: &str) -> String {
    let line_with_redacted_urls = redact_url_credentials(line);
    let mut parsed: Value = match serde_json::from_str(&line_with_redacted_urls) {
        Ok(parsed) => parsed,
        Err(_) => return line_with_redacted_urls,
    };

    redact_sensitive_json_fields(&mut parsed);
    redact_mining_authorize_password(&mut parsed);
    parsed.to_string()
}

fn redact_mining_authorize_password(value: &mut Value) {
    let Some(object) = value.as_object_mut() else {
        return;
    };
    let is_mining_authorize = object
        .get("method")
        .and_then(Value::as_str)
        .map(|method| method == "mining.authorize")
        .unwrap_or(false);
    if !is_mining_authorize {
        return;
    }

    if let Some(params) = object.get_mut("params").and_then(Value::as_array_mut) {
        if let Some(password) = params.get_mut(1) {
            *password = Value::String("****".to_string());
        }
    }
}

fn redact_sensitive_json_fields(value: &mut Value) {
    match value {
        Value::Object(map) => {
            for (key, child_value) in map {
                if is_sensitive_key(key) {
                    *child_value = Value::String("***".to_string());
                } else {
                    redact_sensitive_json_fields(child_value);
                }
            }
        }
        Value::Array(items) => {
            for item in items {
                redact_sensitive_json_fields(item);
            }
        }
        _ => {}
    }
}

fn is_sensitive_key(key: &str) -> bool {
    matches!(
        key.trim().to_ascii_lowercase().as_str(),
        "password" | "pass" | "token" | "authorization" | "bearer"
    )
}

fn redact_url_credentials(input: &str) -> String {
    let mut output = input.to_string();
    for scheme in ["http://", "https://"] {
        let mut search_start = 0usize;
        loop {
            let Some(relative_start) = output[search_start..].find(scheme) else {
                break;
            };

            let authority_start = search_start + relative_start + scheme.len();
            let authority_length = output[authority_start..]
                .find(|ch: char| matches!(ch, '/' | '?' | '#' | '"' | '\'' | ' ' | '\\'))
                .unwrap_or(output.len() - authority_start);
            let authority_end = authority_start + authority_length;
            let authority = &output[authority_start..authority_end];

            let Some(at_index) = authority.rfind('@') else {
                search_start = authority_end;
                continue;
            };
            let userinfo = &authority[..at_index];
            let Some(colon_index) = userinfo.rfind(':') else {
                search_start = authority_end;
                continue;
            };

            let password_start = authority_start + colon_index + 1;
            let password_end = authority_start + at_index;
            output.replace_range(password_start..password_end, "***");
            search_start = password_start + 3;
        }
    }
    output
}

fn compact_for_log(input: &str) -> String {
    let normalized = input.replace(['\n', '\r'], " ");
    let mut compact = String::new();
    for (idx, ch) in normalized.chars().enumerate() {
        if idx >= 240 {
            compact.push_str("...");
            return compact;
        }
        compact.push(ch);
    }
    compact
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

    fn sample_coinbase_tx_hex() -> &'static str {
        "01000000010000000000000000000000000000000000000000000000000000000000000000ffffffff03010203ffffffff0100000000000000000000000000"
    }

    fn sample_latest_template() -> LatestTemplate {
        LatestTemplate {
            height: 700001,
            previousblockhash: "0000000000000000000000000000000000000000000000000000000000000001"
                .to_string(),
            bits: "1d00ffff".to_string(),
            curtime: 1710000000,
            version: "20000000".to_string(),
            rules: vec!["csv".to_string(), "segwit".to_string()],
            segwit_active: true,
            coinbase_txn_hex: Some(sample_coinbase_tx_hex().to_string()),
            coinbase_value: Some(5_000_000_000),
            coinbase_aux_flags_hex: Some("062f503253482f".to_string()),
            default_witness_commitment_hex: None,
            transaction_hashes: vec!["11".repeat(32), "22".repeat(32)],
            transaction_count: 2,
        }
    }

    fn sample_latest_template_without_coinbasetxn() -> LatestTemplate {
        LatestTemplate {
            height: 700001,
            previousblockhash: "0000000000000000000000000000000000000000000000000000000000000001"
                .to_string(),
            bits: "1d00ffff".to_string(),
            curtime: 1710000000,
            version: "20000000".to_string(),
            rules: vec!["csv".to_string(), "segwit".to_string()],
            segwit_active: true,
            coinbase_txn_hex: None,
            coinbase_value: Some(5_000_000_000),
            coinbase_aux_flags_hex: Some("062f503253482f".to_string()),
            default_witness_commitment_hex: None,
            transaction_hashes: vec![],
            transaction_count: 0,
        }
    }

    #[test]
    fn redaction_helpers_mask_urls_and_sensitive_json_keys() {
        let masked_url = redact_url_credentials("http://user:pass@example.com:8332");
        assert_eq!(masked_url, "http://user:***@example.com:8332");

        let mut payload = json!({
            "password": "secret",
            "pass": "abc",
            "token": "tkn",
            "authorization": "Bearer top-secret",
            "bearer": "raw-token",
            "nested": {
                "password": "hidden"
            }
        });
        redact_sensitive_json_fields(&mut payload);
        assert_eq!(payload["password"], Value::from("***"));
        assert_eq!(payload["pass"], Value::from("***"));
        assert_eq!(payload["token"], Value::from("***"));
        assert_eq!(payload["authorization"], Value::from("***"));
        assert_eq!(payload["bearer"], Value::from("***"));
        assert_eq!(payload["nested"]["password"], Value::from("***"));
    }

    #[test]
    fn stratum_line_redaction_masks_authorize_password() {
        let line =
            r#"{"id":2,"method":"mining.authorize","params":["worker-a","my-password-value"]}"#;
        let redacted = sanitize_stratum_log_line(line);
        assert!(redacted.contains("\"worker-a\""));
        assert!(redacted.contains("\"****\""));
        assert!(!redacted.contains("my-password-value"));
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
        assert_eq!(response["result"], Value::from(true));
        assert!(response["error"].is_null());
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

        assert_eq!(first["result"], Value::from(true));
        assert_eq!(second["result"], Value::from(false));
        assert_eq!(second["error"][1], Value::from("duplicate share"));

        let (shares_ok, shares_rej, shares_dup) = state.share_counters();
        assert_eq!(shares_ok, 1);
        assert_eq!(shares_rej, 1);
        assert_eq!(shares_dup, 1);
    }

    #[test]
    fn submit_validation_toggle_controls_pow_path() {
        let request = RpcRequest {
            id: Value::from(77),
            method: "mining.submit".to_string(),
            params: json!([
                "toggle-worker",
                "job-toggle",
                "a21d000000000000",
                "699dfecc",
                "19afcd42"
            ]),
        };
        let remote: SocketAddr = "127.0.0.1:12345".parse().expect("valid addr");
        let mut session_state = SessionState::default();
        let _ = session_state.extranonce1_hex();
        let state_toggle_off = GatewayState::new();
        state_toggle_off.upsert_job_template(
            "job-toggle".to_string(),
            JobTemplate {
                prevhash: "0000000000000000000000000000000000000000000000000000000000000000"
                    .to_string(),
                coinb1: "".to_string(),
                coinb2: "".to_string(),
                merkle_branch: vec![],
                version: "20000000".to_string(),
                nbits: "zzzzzzzz".to_string(),
                job_ntime: "699dfecc".to_string(),
            },
        );

        let accepted_with_toggle_off = build_submit_response_with_validator(
            &request,
            remote,
            &session_state,
            &state_toggle_off,
            200,
            |params, session, gateway_state| {
                validate_submit_params_with_mode(params, session, gateway_state, false)
            },
        );
        assert_eq!(accepted_with_toggle_off["result"], Value::from(true));
        assert!(accepted_with_toggle_off["error"].is_null());

        let state_toggle_on = GatewayState::new();
        state_toggle_on.upsert_job_template(
            "job-toggle".to_string(),
            JobTemplate {
                prevhash: "0000000000000000000000000000000000000000000000000000000000000000"
                    .to_string(),
                coinb1: "".to_string(),
                coinb2: "".to_string(),
                merkle_branch: vec![],
                version: "20000000".to_string(),
                nbits: "zzzzzzzz".to_string(),
                job_ntime: "699dfecc".to_string(),
            },
        );
        let rejected_with_toggle_on = build_submit_response_with_validator(
            &request,
            remote,
            &session_state,
            &state_toggle_on,
            201,
            |params, session, gateway_state| {
                validate_submit_params_with_mode(params, session, gateway_state, true)
            },
        );
        assert_eq!(rejected_with_toggle_on["result"], Value::from(false));
        assert_eq!(
            rejected_with_toggle_on["error"][1],
            Value::from("invalid job data")
        );
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
    fn notify_with_template_builds_real_job_fields_and_store_matches_notify() {
        let state = GatewayState::new();
        let template_state = TemplatePollerState::default();
        template_state.set_latest_template_for_test(sample_latest_template());

        let notify = build_notify_push(&template_state, &state, false);
        let coinb1 = notify.message["params"][2]
            .as_str()
            .expect("coinb1 should be string");
        let coinb2 = notify.message["params"][3]
            .as_str()
            .expect("coinb2 should be string");
        let branch = notify.message["params"][4]
            .as_array()
            .expect("merkle branch should be array");

        assert!(!coinb1.is_empty());
        assert!(!coinb2.is_empty());
        assert!(!branch.is_empty());

        state.upsert_job_template(notify.job_id.clone(), notify.job_template.clone());
        let stored = state
            .get_job_template(&notify.job_id)
            .expect("stored job template should exist");
        let branch_from_notify: Vec<String> = branch
            .iter()
            .map(|entry| {
                entry
                    .as_str()
                    .expect("branch entry should be string")
                    .to_string()
            })
            .collect();

        assert_eq!(
            stored.prevhash,
            notify.message["params"][1]
                .as_str()
                .expect("notify prevhash should be string")
        );
        assert_eq!(stored.coinb1, coinb1);
        assert_eq!(stored.coinb2, coinb2);
        assert_eq!(stored.merkle_branch, branch_from_notify);
        assert_eq!(
            stored.version,
            notify.message["params"][5]
                .as_str()
                .expect("notify version should be string")
        );
        assert_eq!(
            stored.nbits,
            notify.message["params"][6]
                .as_str()
                .expect("notify nbits should be string")
        );
        assert_eq!(
            stored.job_ntime,
            notify.message["params"][7]
                .as_str()
                .expect("notify ntime should be string")
        );
    }

    #[test]
    fn coinbase_builder_produces_non_empty_parts_without_coinbasetxn() {
        let template = sample_latest_template_without_coinbasetxn();
        let parts = build_real_notify_job_parts(&template).expect("coinbase parts should build");
        assert!(!parts.coinb1.is_empty());
        assert!(!parts.coinb2.is_empty());

        let mut coinbase = decode_hex(&parts.coinb1).expect("coinb1 should decode");
        coinbase.extend_from_slice(&[0x11; EXTRANONCE_1_SIZE]);
        coinbase.extend_from_slice(&[0x22; EXTRANONCE_2_SIZE as usize]);
        coinbase.extend_from_slice(&decode_hex(&parts.coinb2).expect("coinb2 should decode"));
        let serialized_hex = bytes_to_lower_hex(&coinbase);
        assert!(!serialized_hex.is_empty());
        assert_eq!(serialized_hex.len() % 2, 0);
    }

    #[test]
    fn coinbase_builder_skips_witness_commitment_when_segwit_inactive() {
        let mut template = sample_latest_template_without_coinbasetxn();
        template.rules = vec!["csv".to_string(), "!segwit".to_string()];
        template.segwit_active = false;
        template.default_witness_commitment_hex = Some(
            "6a24aa21a9ed1111111111111111111111111111111111111111111111111111111111111111"
                .to_string(),
        );

        let parts = build_real_notify_job_parts(&template).expect("coinbase parts should build");
        assert!(!parts.has_witness_commitment);

        let coinb2 = decode_hex(&parts.coinb2).expect("coinb2 should decode");
        let mut cursor = 4usize; // sequence
        let output_count = read_varint(&coinb2, &mut cursor).expect("output count should parse");
        assert_eq!(output_count, 1);
    }

    #[test]
    fn coinbase_builder_includes_witness_commitment_when_segwit_active() {
        let mut template = sample_latest_template_without_coinbasetxn();
        template.rules = vec!["csv".to_string(), "segwit".to_string()];
        template.segwit_active = true;
        template.default_witness_commitment_hex = Some(
            "6a24aa21a9ed2222222222222222222222222222222222222222222222222222222222222222"
                .to_string(),
        );

        let parts = build_real_notify_job_parts(&template).expect("coinbase parts should build");
        assert!(parts.has_witness_commitment);

        let coinb2 = decode_hex(&parts.coinb2).expect("coinb2 should decode");
        let mut cursor = 4usize; // sequence
        let output_count = read_varint(&coinb2, &mut cursor).expect("output count should parse");
        assert_eq!(output_count, 2);
    }

    #[test]
    fn merkle_branch_is_empty_when_template_has_zero_transactions() {
        let branch = build_coinbase_merkle_branch([0u8; 32], &[])
            .expect("merkle branch should build for empty tx list");
        assert!(branch.is_empty());
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
