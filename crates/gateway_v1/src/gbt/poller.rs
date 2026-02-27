use anyhow::{anyhow, Context};
use reqwest::Client;
use serde::Deserialize;
use serde_json::{json, Value};
use std::env;
use std::fmt;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, RwLock};
use std::time::Duration;
use sv2_core::stratum_core::bitcoin::hashes::{sha256d, Hash};

const DEFAULT_POLL_SECS: u64 = 3;
const HTTP_TIMEOUT_SECS: u64 = 15;
const AZ_RPC_ERR_MISSING_SEGWIT_RULES: i64 = -8;
const DEFAULT_TEMPLATE_VERSION: u32 = 0x2000_0000;
const GBT_CAPABILITIES: [&str; 5] = [
    "coinbasetxn",
    "workid",
    "coinbase/append",
    "longpoll",
    "proposal",
];

#[derive(Debug, Default)]
pub struct TemplatePollerState {
    job_counter: AtomicU64,
    latest_template: RwLock<Option<LatestTemplate>>,
}

impl TemplatePollerState {
    pub fn current_job_counter(&self) -> u64 {
        self.job_counter.load(Ordering::Relaxed)
    }

    pub fn latest_template(&self) -> Option<LatestTemplate> {
        match self.latest_template.read() {
            Ok(guard) => guard.clone(),
            Err(poisoned) => poisoned.into_inner().clone(),
        }
    }

    fn next_job_counter(&self) -> u64 {
        self.job_counter.fetch_add(1, Ordering::Relaxed) + 1
    }

    fn set_latest_template(&self, latest_template: LatestTemplate) {
        match self.latest_template.write() {
            Ok(mut guard) => {
                *guard = Some(latest_template);
            }
            Err(poisoned) => {
                *poisoned.into_inner() = Some(latest_template);
            }
        }
    }

    #[cfg(test)]
    pub fn set_latest_template_for_test(&self, latest_template: LatestTemplate) {
        self.set_latest_template(latest_template);
    }
}

#[derive(Debug, Clone)]
pub struct LatestTemplate {
    pub height: u64,
    pub previousblockhash: String,
    pub bits: String,
    pub curtime: u64,
    pub version: String,
    pub coinbase_txn_hex: Option<String>,
    pub coinbase_value: Option<u64>,
    pub coinbase_aux_flags_hex: Option<String>,
    pub default_witness_commitment_hex: Option<String>,
    pub transaction_hashes: Vec<String>,
    pub transaction_count: usize,
}

impl LatestTemplate {
    fn from_snapshot(snapshot: &TemplateSnapshot) -> Self {
        let transaction_hashes = snapshot
            .transactions
            .iter()
            .filter_map(TemplateTransaction::tx_hash_hex)
            .collect();
        Self {
            height: snapshot.height,
            previousblockhash: snapshot.previousblockhash.clone(),
            bits: snapshot.bits.clone(),
            curtime: snapshot.curtime,
            version: format!("{:08x}", snapshot.version),
            coinbase_txn_hex: snapshot.coinbase_txn_hex(),
            coinbase_value: snapshot.coinbasevalue,
            coinbase_aux_flags_hex: snapshot
                .coinbaseaux
                .as_ref()
                .and_then(|aux| aux.flags.clone()),
            default_witness_commitment_hex: snapshot.default_witness_commitment.clone(),
            transaction_hashes,
            transaction_count: snapshot.transactions.len(),
        }
    }
}

#[derive(Debug, Clone, Deserialize)]
struct TemplateSnapshot {
    height: u64,
    previousblockhash: String,
    bits: String,
    curtime: u64,
    target: String,
    #[serde(default = "default_template_version")]
    version: u32,
    #[serde(default)]
    coinbasetxn: Option<TemplateCoinbaseTxn>,
    #[serde(default)]
    coinbase_payload: Option<String>,
    #[serde(default)]
    coinbasevalue: Option<u64>,
    #[serde(default)]
    coinbaseaux: Option<TemplateCoinbaseAux>,
    #[serde(default)]
    default_witness_commitment: Option<String>,
    #[serde(default)]
    transactions: Vec<TemplateTransaction>,
}

impl TemplateSnapshot {
    fn coinbase_txn_hex(&self) -> Option<String> {
        self.coinbasetxn
            .as_ref()
            .and_then(TemplateCoinbaseTxn::as_hex)
            .or_else(|| self.coinbase_payload.clone())
    }
}

#[derive(Debug, Clone, Deserialize)]
#[serde(untagged)]
enum TemplateCoinbaseTxn {
    DataObject { data: String },
    HexString(String),
}

impl TemplateCoinbaseTxn {
    fn as_hex(&self) -> Option<String> {
        match self {
            TemplateCoinbaseTxn::DataObject { data } => Some(data.clone()),
            TemplateCoinbaseTxn::HexString(hex) => Some(hex.clone()),
        }
    }
}

#[derive(Debug, Clone, Deserialize)]
struct TemplateCoinbaseAux {
    #[serde(default)]
    flags: Option<String>,
}

#[derive(Debug, Clone, Deserialize)]
struct TemplateTransaction {
    #[serde(default)]
    txid: Option<String>,
    #[serde(default)]
    hash: Option<String>,
    #[serde(default)]
    data: Option<String>,
}

impl TemplateTransaction {
    fn tx_hash_hex(&self) -> Option<String> {
        if let Some(txid) = self.txid.as_deref() {
            return Some(txid.to_string());
        }
        if let Some(hash) = self.hash.as_deref() {
            return Some(hash.to_string());
        }
        self.data
            .as_deref()
            .and_then(txid_hex_from_transaction_data_hex)
    }
}

fn txid_hex_from_transaction_data_hex(data_hex: &str) -> Option<String> {
    let raw = decode_hex_for_poller(data_hex)?;
    let txid = sha256d::Hash::hash(&raw).to_byte_array();
    Some(bytes_to_lower_hex(&txid))
}

fn decode_hex_for_poller(input: &str) -> Option<Vec<u8>> {
    if input.len() % 2 != 0 {
        return None;
    }

    let mut output = Vec::with_capacity(input.len() / 2);
    for pair in input.as_bytes().chunks_exact(2) {
        let pair_str = std::str::from_utf8(pair).ok()?;
        let value = u8::from_str_radix(pair_str, 16).ok()?;
        output.push(value);
    }
    Some(output)
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

fn default_template_version() -> u32 {
    DEFAULT_TEMPLATE_VERSION
}

#[derive(Debug, Deserialize)]
struct JsonRpcResponse<T> {
    result: Option<T>,
    error: Option<JsonRpcError>,
}

#[derive(Debug, Deserialize)]
struct JsonRpcError {
    code: i64,
    message: String,
}

#[derive(Debug)]
struct AzRpcError {
    code: i64,
    message: String,
}

impl fmt::Display for AzRpcError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "AZ RPC error code={} message={}",
            self.code, self.message
        )
    }
}

impl std::error::Error for AzRpcError {}

pub async fn run_template_poller(shared_state: Arc<TemplatePollerState>) -> anyhow::Result<()> {
    let rpc_url = env::var("AZ_RPC_URL").context("AZ_RPC_URL is required for GBT poller")?;
    let rpc_user = env::var("AZ_RPC_USER").context("AZ_RPC_USER is required for GBT poller")?;
    let rpc_password =
        env::var("AZ_RPC_PASSWORD").context("AZ_RPC_PASSWORD is required for GBT poller")?;
    let poll_interval = poll_interval_from_env();
    let client = Client::builder()
        .timeout(Duration::from_secs(HTTP_TIMEOUT_SECS))
        .build()
        .context("failed to build AZ RPC HTTP client")?;

    println!(
        "TEMPLATE_POLLER started rpc_url={} poll_secs={}",
        rpc_url_for_log(&rpc_url),
        poll_interval.as_secs()
    );

    let mut last_identity: Option<(u64, String)> = None;

    loop {
        match fetch_template(&client, &rpc_url, &rpc_user, &rpc_password).await {
            Ok(template) if template_changed(&last_identity, &template) => {
                let template_seq = shared_state.next_job_counter();
                shared_state.set_latest_template(LatestTemplate::from_snapshot(&template));
                let coinbasevalue_for_log = template
                    .coinbasevalue
                    .map(|value| value.to_string())
                    .unwrap_or_else(|| "missing".to_string());
                let has_coinbasetxn = template.coinbase_txn_hex().is_some();
                println!(
                    "TEMPLATE job={} height={} prevhash={} bits={} curtime={} target={} txs={} coinbasevalue={} has_coinbasetxn={}",
                    template_seq,
                    template.height,
                    template.previousblockhash,
                    template.bits,
                    template.curtime,
                    template.target,
                    template.transactions.len(),
                    coinbasevalue_for_log,
                    has_coinbasetxn
                );
                last_identity = Some((template.height, template.previousblockhash));
            }
            Ok(_) => {}
            Err(error) => {
                if let Some(rpc_error) = error.downcast_ref::<AzRpcError>() {
                    if rpc_error.code == AZ_RPC_ERR_MISSING_SEGWIT_RULES {
                        eprintln!(
                            "TEMPLATE_POLLER poll_failed rpc_error_code={} rpc_error_message=\"{}\" hint=\"missing segwit rules; call getblocktemplate with params=[{{\\\"rules\\\":[\\\"segwit\\\"]}}]\"",
                            rpc_error.code,
                            compact_for_log(&rpc_error.message)
                        );
                    } else {
                        eprintln!(
                            "TEMPLATE_POLLER poll_failed rpc_error_code={} rpc_error_message=\"{}\"",
                            rpc_error.code,
                            compact_for_log(&rpc_error.message)
                        );
                    }
                } else {
                    eprintln!("TEMPLATE_POLLER poll_failed error=\"{error}\"");
                }
            }
        }

        tokio::time::sleep(poll_interval).await;
    }
}

fn poll_interval_from_env() -> Duration {
    let interval = env::var("AZ_GBT_POLL_SECS")
        .ok()
        .and_then(|value| value.parse::<u64>().ok())
        .unwrap_or(DEFAULT_POLL_SECS)
        .max(1);
    Duration::from_secs(interval)
}

async fn fetch_template(
    client: &Client,
    rpc_url: &str,
    rpc_user: &str,
    rpc_password: &str,
) -> anyhow::Result<TemplateSnapshot> {
    let request_body = build_getblocktemplate_request_body();

    let response = client
        .post(rpc_url)
        .basic_auth(rpc_user, Some(rpc_password))
        .json(&request_body)
        .send()
        .await
        .context("AZ RPC request failed")?;
    let status = response.status();
    let body = response
        .text()
        .await
        .context("failed to read AZ RPC body")?;

    if !status.is_success() {
        return Err(anyhow!(
            "AZ RPC http_status={} body={}",
            status.as_u16(),
            compact_for_log(&body)
        ));
    }

    parse_template_response(&body)
}

fn parse_template_response(body: &str) -> anyhow::Result<TemplateSnapshot> {
    let rpc: JsonRpcResponse<TemplateSnapshot> =
        serde_json::from_str(body).context("failed to decode AZ RPC JSON response")?;

    if let Some(error) = rpc.error {
        return Err(anyhow!(AzRpcError {
            code: error.code,
            message: error.message
        }));
    }

    rpc.result
        .context("AZ RPC response missing result for getblocktemplate")
}

fn build_getblocktemplate_request_body() -> Value {
    json!({
        "jsonrpc": "1.0",
        "id": "gateway_v1-gbt-poller",
        "method": "getblocktemplate",
        "params": [{
            "rules": ["segwit"],
            "capabilities": GBT_CAPABILITIES
        }]
    })
}

fn template_changed(last_identity: &Option<(u64, String)>, template: &TemplateSnapshot) -> bool {
    match last_identity {
        Some((last_height, last_prevhash)) => {
            *last_height != template.height || *last_prevhash != template.previousblockhash
        }
        None => true,
    }
}

fn rpc_url_for_log(rpc_url: &str) -> String {
    let Ok(mut parsed) = reqwest::Url::parse(rpc_url) else {
        return "<invalid_az_rpc_url>".to_string();
    };

    if parsed.password().is_some() {
        let _ = parsed.set_password(Some("***"));
    }

    parsed.to_string()
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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parse_template_response_extracts_required_fields() {
        let body = r#"{
            "id":"gateway_v1-gbt-poller",
            "result":{
                "height": 100,
                "previousblockhash":"abc123",
                "bits":"1d00ffff",
                "curtime": 1710000000,
                "target":"00000000ffffffffffffffffffffffffffffffffffffffffffffffffffffffff",
                "coinbasevalue": 5000000000,
                "coinbaseaux": { "flags": "062f503253482f" },
                "default_witness_commitment": "6a24aa21a9ed1111111111111111111111111111111111111111111111111111111111111111",
                "transactions": [{
                    "data": "01000000000100"
                }]
            },
            "error":null
        }"#;

        let template = parse_template_response(body).expect("response should parse");
        assert_eq!(template.height, 100);
        assert_eq!(template.previousblockhash, "abc123");
        assert_eq!(template.bits, "1d00ffff");
        assert_eq!(template.curtime, 1710000000);
        assert_eq!(
            template.target,
            "00000000ffffffffffffffffffffffffffffffffffffffffffffffffffffffff"
        );
        assert_eq!(template.coinbasevalue, Some(5_000_000_000));
        assert_eq!(
            template
                .coinbaseaux
                .as_ref()
                .and_then(|aux| aux.flags.as_deref()),
            Some("062f503253482f")
        );
        assert!(template.default_witness_commitment.is_some());
        assert_eq!(template.transactions.len(), 1);
    }

    #[test]
    fn latest_template_includes_coinbase_related_fields() {
        let body = r#"{
            "id":"gateway_v1-gbt-poller",
            "result":{
                "height": 101,
                "previousblockhash":"00aa",
                "bits":"1d00ffff",
                "curtime": 1710000001,
                "target":"00000000ffffffffffffffffffffffffffffffffffffffffffffffffffffffff",
                "coinbasevalue": 123,
                "coinbaseaux": { "flags": "51" },
                "default_witness_commitment": "6a24aa21a9ed2222222222222222222222222222222222222222222222222222222222222222",
                "coinbase_payload": "0100000000",
                "transactions": []
            },
            "error":null
        }"#;

        let snapshot = parse_template_response(body).expect("response should parse");
        let latest = LatestTemplate::from_snapshot(&snapshot);
        assert_eq!(latest.height, 101);
        assert_eq!(latest.coinbase_value, Some(123));
        assert_eq!(latest.coinbase_aux_flags_hex.as_deref(), Some("51"));
        assert!(latest.default_witness_commitment_hex.is_some());
        assert_eq!(latest.coinbase_txn_hex.as_deref(), Some("0100000000"));
    }

    #[test]
    fn template_changed_detects_height_or_prevhash_change() {
        let template = TemplateSnapshot {
            height: 10,
            previousblockhash: "aaa".to_string(),
            bits: "1d00ffff".to_string(),
            curtime: 1,
            target: "ffff".to_string(),
            version: default_template_version(),
            coinbasetxn: None,
            coinbase_payload: None,
            coinbasevalue: None,
            coinbaseaux: None,
            default_witness_commitment: None,
            transactions: Vec::new(),
        };
        assert!(template_changed(&None, &template));
        assert!(!template_changed(&Some((10, "aaa".to_string())), &template));
        assert!(template_changed(&Some((11, "aaa".to_string())), &template));
        assert!(template_changed(&Some((10, "bbb".to_string())), &template));
    }

    #[test]
    fn parse_template_response_reports_missing_segwit_rules_error() {
        let body = r#"{
            "id":"gateway_v1-gbt-poller",
            "result":null,
            "error":{
                "code":-8,
                "message":"getblocktemplate must be called with the segwit rule set (call with {\"rules\": [\"segwit\"]})"
            }
        }"#;

        let error = parse_template_response(body).expect_err("response should surface rpc error");
        let rpc_error = error
            .downcast_ref::<AzRpcError>()
            .expect("error should be typed rpc error");
        assert_eq!(rpc_error.code, -8);
        assert!(rpc_error.message.contains("segwit rule set"));
    }

    #[test]
    fn getblocktemplate_request_includes_segwit_rules_param() {
        let request = build_getblocktemplate_request_body();
        assert_eq!(request["method"], Value::from("getblocktemplate"));
        assert_eq!(
            request["params"],
            json!([{
                "rules": ["segwit"],
                "capabilities": [
                    "coinbasetxn",
                    "workid",
                    "coinbase/append",
                    "longpoll",
                    "proposal"
                ]
            }])
        );
    }

    #[test]
    fn rpc_url_for_log_redacts_password() {
        let logged = rpc_url_for_log("http://user:supersecret@127.0.0.1:8332");
        assert!(logged.contains("user:***@"));
        assert!(!logged.contains("supersecret"));
    }
}
