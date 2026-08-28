// ============================================================================
// common/src/metrics.rs
//
// WHAT THIS FILE DOES
// --------------------
// Sets up Prometheus metrics for a service and gives it a /metrics endpoint
// to expose them on. This lives in `common` (not in `ingestion` or
// `processor` individually) because BOTH services need to do the exact
// same setup step — no reason to duplicate it.
//
// THE TWO CRATES INVOLVED
// ------------------------
// - `metrics`: a FACADE crate. It defines macros like `metrics::counter!`,
//   `metrics::histogram!`, `metrics::gauge!` that your business logic calls
//   directly, anywhere in the codebase, without importing anything
//   Prometheus-specific. Think of it like the `log` crate: your code calls
//   `log::info!(...)` without caring whether the output goes to stdout, a
//   file, or a remote log collector — that's decided once, at startup, by
//   whichever "recorder" you install.
// - `metrics-exporter-prometheus`: the actual BACKEND. Installing its
//   recorder is what makes those `metrics::counter!` calls actually get
//   captured and rendered in Prometheus's text format. Swap this crate out
//   later (e.g. for a different observability backend) and none of your
//   `metrics::counter!` call sites need to change.
//
// METRIC TYPES YOU'LL USE
// -------------------------
// - counter: a number that only goes up (e.g. "total transactions
//   processed"). Prometheus computes rates from these itself
//   (`rate(my_counter[5m])`), so you never need to reset it yourself.
// - histogram: records a distribution of values (e.g. "how long did this
//   ClickHouse insert take") so you can later query percentiles (p50, p99)
//   instead of just an average, which hides outliers.
// - gauge: a number that goes up AND down (e.g. "current in-flight
//   requests"). Less common in this project, but good to know exists.
// ============================================================================

use axum::{Router, routing::get};
use metrics_exporter_prometheus::{PrometheusBuilder, PrometheusHandle};

/// Installs the Prometheus recorder as the global metrics backend for this
/// process. Call this ONCE, at the very start of `main()`, before any code
/// that might call `metrics::counter!` etc. runs.
///
/// Returns a `PrometheusHandle`, which is the thing that can actually
/// RENDER the current metric values as text — you'll pass this into the
/// `/metrics` route handler below.
pub fn install_recorder() -> PrometheusHandle {
    // .install_recorder() sets this as the process-wide default recorder —
    // it's what makes `metrics::counter!("x").increment(1)` calls anywhere
    // else in the codebase "just work" without passing a handle around
    // everywhere. .expect() is reasonable here: if this fails, it means
    // something ELSE already installed a global recorder, which would be a
    // startup-order bug worth crashing loudly on rather than silently
    // limping along with no metrics.
    PrometheusBuilder::new()
        .install_recorder()
        .expect("failed to install Prometheus metrics recorder")
}

/// Builds a tiny Axum router containing just the `/metrics` route.
///
/// WHY A SEPARATE ROUTER FUNCTION (rather than just documenting "add this
/// route to your router"): `ingestion` already has its own Axum router for
/// `/transactions`. `processor` has NO existing HTTP server at all — it's
/// purely a Kafka consumer loop. This function works for both cases: in
/// ingestion you can `.merge()` it into the existing router; in processor
/// you'll serve this router standalone on its own port (see integration
/// notes below).
pub fn metrics_router(handle: PrometheusHandle) -> Router {
    Router::new().route(
        "/metrics",
        // `move` captures `handle` into the closure. PrometheusHandle is
        // cheap to clone internally (it's Arc-backed), so this works fine
        // even though the closure runs on every scrape request.
        get(move || {
            let handle = handle.clone();
            async move { handle.render() }
        }),
    )
}
