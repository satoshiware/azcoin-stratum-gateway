use std::env;
use std::fs;

const EMBEDDED_VERSION: &str = include_str!("../../../VERSION");

pub fn get_version() -> String {
    read_runtime_version_file()
        .or_else(|| parse_version(EMBEDDED_VERSION))
        .unwrap_or_else(|| env!("CARGO_PKG_VERSION").to_string())
}

pub fn get_revision() -> String {
    let full = env::var("GATEWAY_VCS_REF")
        .or_else(|_| env::var("VCS_REF"))
        .unwrap_or_else(|_| "unknown".to_string());
    short_sha(&full)
}

fn read_runtime_version_file() -> Option<String> {
    let path = env::var("GATEWAY_VERSION_FILE").unwrap_or_else(|_| "VERSION".to_string());
    fs::read_to_string(path)
        .ok()
        .and_then(|raw| parse_version(&raw))
}

fn parse_version(raw: &str) -> Option<String> {
    let version = raw.trim();
    if version.is_empty() {
        None
    } else {
        Some(version.to_string())
    }
}

fn short_sha(revision: &str) -> String {
    let clean = revision.trim();
    if clean.is_empty() {
        return "unknown".to_string();
    }

    clean.chars().take(7).collect()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn get_version_returns_release_version() {
        assert_eq!(get_version(), "0.1.2");
    }
}
