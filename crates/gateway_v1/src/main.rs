use rand::random;
use serde_json::{json, Value};
use std::env;
use std::io::{self, BufRead, BufReader, BufWriter, Write};
use std::net::{SocketAddr, TcpListener, TcpStream};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::thread;
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

mod gbt;
mod version;
use gbt::poller::TemplatePollerState;

const SUBSCRIPTION_ID: &str = "1";
const EXTRANONCE_2_SIZE: u64 = 8;
const STARTING_DIFFICULTY: u64 = 1;

struct GatewayState {
    sessions: AtomicU64,
}

impl GatewayState {
    fn new() -> Self {
        Self {
            sessions: AtomicU64::new(0),
        }
    }
}

#[derive(Debug, Default)]
struct SessionState {
    extranonce1_hex: Option<String>,
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
    params: Vec<Value>,
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
    let state = Arc::new(GatewayState::new());
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
        println!("HEALTH ok uptime={uptime_secs}s sessions={sessions} jobs={jobs}");
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

    if let Err(error) = handle_client_inner(stream, remote_addr, template_state) {
        eprintln!("[tcp] session_error remote_addr={remote_addr} error=\"{error}\"");
    }

    let active_sessions = state.sessions.fetch_sub(1, Ordering::SeqCst) - 1;
    println!("[tcp] session_closed remote_addr={remote_addr} sessions={active_sessions}");
}

fn handle_client_inner(
    stream: TcpStream,
    remote_addr: SocketAddr,
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
                let response = build_response(&request, remote_addr, &mut session_state);
                write_json_line(&mut writer, remote_addr, &response)?;

                if request.method == "mining.authorize" {
                    write_json_line(&mut writer, remote_addr, &build_set_difficulty_push())?;
                    write_json_line(
                        &mut writer,
                        remote_addr,
                        &build_notify_push(template_state.as_ref()),
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
        .and_then(Value::as_array)
        .ok_or("missing or invalid params")?
        .clone();

    Ok(RpcRequest { id, method, params })
}

fn build_response(
    request: &RpcRequest,
    remote_addr: SocketAddr,
    session_state: &mut SessionState,
) -> Value {
    match request.method.as_str() {
        "mining.configure" => json!({
            "id": request.id,
            "result": {
                "version-rolling": false
            },
            "error": Value::Null
        }),
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
                .first()
                .and_then(Value::as_str)
                .unwrap_or("<missing>");
            println!("[auth] remote_addr={remote_addr} worker={worker}");
            json!({
                "id": request.id,
                "result": true,
                "error": Value::Null
            })
        }
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

fn build_set_difficulty_push() -> Value {
    json!({
        "id": Value::Null,
        "method": "mining.set_difficulty",
        "params": [STARTING_DIFFICULTY]
    })
}

fn build_notify_push(template_state: &TemplatePollerState) -> Value {
    let fallback_time = match SystemTime::now().duration_since(UNIX_EPOCH) {
        Ok(duration) => duration.as_secs(),
        Err(_) => 0,
    };

    let (job_id, previousblockhash, bits, curtime_hex) = match template_state.latest_template() {
        Some(template) => (
            template.job_id.to_string(),
            template.previousblockhash,
            template.bits,
            format!("{:08x}", template.curtime),
        ),
        None => (
            template_state.current_job_counter().to_string(),
            "0000000000000000000000000000000000000000000000000000000000000000".to_string(),
            "1d00ffff".to_string(),
            format!("{fallback_time:08x}"),
        ),
    };

    json!({
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
            true
        ]
    })
}

fn write_json_line(
    writer: &mut BufWriter<TcpStream>,
    remote_addr: SocketAddr,
    value: &Value,
) -> io::Result<()> {
    let line = value.to_string();
    println!("STRATUM_TX remote={remote_addr} line={line}");
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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parse_accepts_string_id_method_and_params_array() {
        let line = r#"{"id":"abc","method":"mining.subscribe","params":["test/0.1"]}"#;
        let request = parse_rpc_request(line).expect("request should parse");
        assert_eq!(request.method, "mining.subscribe");
        assert_eq!(request.id, Value::String("abc".to_string()));
        assert_eq!(request.params.len(), 1);
    }

    #[test]
    fn parse_accepts_number_id() {
        let line = r#"{"id":2,"method":"mining.authorize","params":["user.worker","x"]}"#;
        let request = parse_rpc_request(line).expect("request should parse");
        assert!(request.id.is_number());
        assert_eq!(request.method, "mining.authorize");
    }

    #[test]
    fn parse_rejects_non_array_params() {
        let line = r#"{"id":1,"method":"mining.subscribe","params":{"bad":true}}"#;
        assert!(parse_rpc_request(line).is_err());
    }

    #[test]
    fn unknown_method_returns_method_not_found() {
        let request = RpcRequest {
            id: Value::from(99),
            method: "mining.nope".to_string(),
            params: vec![],
        };
        let mut session_state = SessionState::default();
        let response = build_response(
            &request,
            "127.0.0.1:12345".parse().expect("valid addr"),
            &mut session_state,
        );
        assert_eq!(response["id"], Value::from(99));
        assert_eq!(response["error"]["code"], Value::from(-32601));
        assert_eq!(
            response["error"]["message"],
            Value::from("Method not found")
        );
    }

    #[test]
    fn mining_configure_returns_version_rolling_false() {
        let request = RpcRequest {
            id: Value::from(41),
            method: "mining.configure".to_string(),
            params: vec![
                json!(["version-rolling"]),
                json!({"version-rolling.mask":"ffffffff"}),
            ],
        };
        let mut session_state = SessionState::default();
        let response = build_response(
            &request,
            "127.0.0.1:12345".parse().expect("valid addr"),
            &mut session_state,
        );
        assert_eq!(response["id"], Value::from(41));
        assert!(response["error"].is_null());
        assert_eq!(response["result"]["version-rolling"], Value::from(false));
    }

    #[test]
    fn subscribe_and_authorize_return_success_without_error() {
        let subscribe = RpcRequest {
            id: Value::from(1),
            method: "mining.subscribe".to_string(),
            params: vec![Value::from("test/0.1")],
        };
        let mut session_state = SessionState::default();
        let subscribe_response = build_response(
            &subscribe,
            "127.0.0.1:12345".parse().expect("valid addr"),
            &mut session_state,
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
            params: vec![Value::from("user.worker"), Value::from("x")],
        };
        let authorize_response = build_response(
            &authorize,
            "127.0.0.1:12345".parse().expect("valid addr"),
            &mut session_state,
        );
        assert_eq!(authorize_response["id"], Value::from("auth-1"));
        assert_eq!(authorize_response["result"], Value::from(true));
        assert!(authorize_response["error"].is_null());
    }
}
