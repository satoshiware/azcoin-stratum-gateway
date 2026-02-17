//! Foundation crate for future Stratum V2 integration.
//! This intentionally only compiles and re-exports upstream SV2 primitives.

pub use stratum_core;

pub fn sv2_foundation_ready() -> bool {
    true
}
