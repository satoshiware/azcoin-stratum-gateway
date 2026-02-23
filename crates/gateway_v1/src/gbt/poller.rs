use anyhow::{anyhow, Context};
use reqwest::Client;
use serde::Deserialize;
use serde_json::json;
use std::env;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::Duration;

const DEFAULT_POLL_SECS: u64 = 3;
const HTTP_TIMEOUT_SECS: u64 = 15;

#[derive(Debug, Default)]
pub struct TemplatePollerState {
    job_counter: AtomicU64,
}

impl TemplatePollerState {
    pub fn current_job_counter(&self) -> u64 {
        self.job_counter.load(Ordering::Relaxed)
    }

    fn next_job_counter(&self) -> u64 {
        self.job_counter.fetch_add(1, Ordering::Relaxed) + 1
    }
}

#[derive(Debug, Clone, Deserialize)]
struct TemplateSnapshot {
    height: u64,
    previousblockhash: String,
    bits: String,
    curtime: u64,
    target: String,
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
                let job = shared_state.next_job_counter();
                println!(
                    "TEMPLATE job={} height={} prevhash={} bits={} curtime={} target={}",
                    job,
                    template.height,
                    template.previousblockhash,
                    template.bits,
                    template.curtime,
                    template.target
                );
                last_identity = Some((template.height, template.previousblockhash));
            }
            Ok(_) => {}
            Err(error) => {
                eprintln!("TEMPLATE_POLLER poll_failed error=\"{error}\"");
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
    let request_body = json!({
        "jsonrpc": "1.0",
        "id": "gateway_v1-gbt-poller",
        "method": "getblocktemplate",
        "params": [{}]
    });

    let response = client
        .post(rpc_url)
        .basic_auth(rpc_user, Some(rpc_password))
        .json(&request_body)
        .send()
        .await
        .context("AZ RPC request failed")?;
    let status = response.status();
    let body = response.text().await.context("failed to read AZ RPC body")?;

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
        return Err(anyhow!(
            "AZ RPC error code={} message={}",
            error.code,
            error.message
        ));
    }

    rpc.result
        .context("AZ RPC response missing result for getblocktemplate")
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

    if !parsed.username().is_empty() || parsed.password().is_some() {
        let _ = parsed.set_username("redacted");
        let _ = parsed.set_password(None);
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
                "target":"00000000ffffffffffffffffffffffffffffffffffffffffffffffffffffffff"
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
    }

    #[test]
    fn template_changed_detects_height_or_prevhash_change() {
        let template = TemplateSnapshot {
            height: 10,
            previousblockhash: "aaa".to_string(),
            bits: "1d00ffff".to_string(),
            curtime: 1,
            target: "ffff".to_string(),
        };
        assert!(template_changed(&None, &template));
        assert!(!template_changed(&Some((10, "aaa".to_string())), &template));
        assert!(template_changed(&Some((11, "aaa".to_string())), &template));
        assert!(template_changed(&Some((10, "bbb".to_string())), &template));
    }

    #[test]
    fn rpc_url_for_log_redacts_password() {
        let logged = rpc_url_for_log("http://user:supersecret@127.0.0.1:8332");
        assert!(logged.contains("redacted"));
        assert!(!logged.contains("supersecret"));
    }
}
