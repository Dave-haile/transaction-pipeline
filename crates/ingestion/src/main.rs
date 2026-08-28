// -----------------------------------------------------------------------
// ingestion service — the front door of the pipeline.
// Accepts a transaction over HTTP POST, builds it into our shared
// `common::Transaction` type, serializes it to JSON, and publishes it
// onto the Kafka topic "transactions.raw" for the processor to consume.
// -----------------------------------------------------------------------

use axum::{
    Json, // extractor/response wrapper for JSON bodies
    Router,
    extract::State, // pulls shared state (our Kafka producer) into a handler
    routing::post,  // registers a route for HTTP POST
};
use common::Transaction;
use rdkafka::{
    ClientConfig,
    producer::{FutureProducer, FutureRecord},
};
use serde::Deserialize;
use std::{sync::Arc, time::Duration};

// This is what we expect the CLIENT to send us — deliberately NOT the
// same as Transaction. The client shouldn't be able to set `id` or
// `timestamp` themselves; those are generated server-side inside
// Transaction::new(). Keeping the "wire" shape separate from the
// "domain" shape is a pattern worth internalizing early.
#[derive(Debug, Deserialize)]
struct CreateTransactionRequest {
    account_id: String,
    amount: f64,
    currency: String,
    merchant: String,
}

// Shared state every request handler can access. We wrap it in Arc
// (atomic reference counted pointer) so multiple concurrent requests
// can each hold a cheap, thread-safe reference to the SAME producer,
// instead of each request needing its own Kafka connection.
struct AppState {
    producer: FutureProducer,
}


#[tokio::main] // macro that sets up the async runtime and lets main() be async
async fn main() {
    dotenvy::dotenv().ok();
    // Wire up logging so we can see what's happening when requests come in.
    tracing_subscriber::fmt::init();

    let prometheus_handle = common::metrics::install_recorder();


    let kafka_brokers = std::env::var("KAFKA_BOOTSTRAP_SERVERS")
    .expect("KAFKA_BOOTSTRAP_SERVERS must be set — check .env for local runs, or docker-compose environment: for containers");

    // Build the Kafka producer. ClientConfig is a builder: each .set()
    // call configures one option, and .create() finalizes it into a
    // real FutureProducer connected (lazily) to the broker.
    let producer: FutureProducer = ClientConfig::new()
        // where to find the broker — matches what we set up when we
        // installed Redpanda natively on the VM (default port 9092)
        .set("bootstrap.servers", &kafka_brokers)
        // "all" = wait for every in-sync replica to acknowledge the
        // write before we consider it successful. Slower, but means we
        // never silently lose a transaction if a broker restarts.
        // (With --replicas 1 this is currently just 1 broker anyway —
        // matters more once we have a multi-broker cluster.)
        .set("message.timeout.ms", "5000")
        .set("acks", "all")
        .create()
        .expect("failed to create Kafka producer"); // .expect() panics with
    // this message if producer creation fails — acceptable at startup,
    // since there's no point running a server that can't reach Kafka.

    // Bundle the producer into our shared state, wrapped in Arc so it
    // can be cloned cheaply and shared across concurrent request handlers.
    let state = Arc::new(AppState { producer });

    // Build the router: one route, POST /transactions, handled by
    // create_transaction below. .with_state(state) makes `state`
    // available to any handler that asks for it via the State extractor.
    let app = Router::new()
        .route("/transactions", post(create_transaction))
        .with_state(state)
        .merge(common::metrics::metrics_router(prometheus_handle)); // add /metrics endpoint for Prometheus

    // Bind a TCP listener on port 3000, on all interfaces (0.0.0.0) so
    // it's reachable from outside the VM too, not just localhost.
    let listener = tokio::net::TcpListener::bind("0.0.0.0:3001")
        .await
        .expect("failed to bind to port 3001");

    tracing::info!("ingestion service listening on 0.0.0.0:3001");

    // Start serving requests. This runs forever (until the process is
    // killed), which is why it's the last line of main().
    axum::serve(listener, app).await.expect("server error");
}

// The actual request handler. Axum figures out how to call this based
// on the TYPES of its parameters — this is "extractor" magic:
//   State(state)  -> pulls our AppState out of what we passed to with_state
//   Json(payload) -> parses the request body as JSON into CreateTransactionRequest,
//                    and returns a 400 error automatically if it doesn't parse
async fn create_transaction(
    State(state): State<Arc<AppState>>,
    Json(payload): Json<CreateTransactionRequest>,
) -> Result<Json<Transaction>, (axum::http::StatusCode, String)> {
        metrics::counter!("http_requests_total", "route" => "transactions").increment(1); // ADD

    // Turn the untrusted wire request into our real domain type.
    // This is where id + timestamp get generated (inside Transaction::new).
    let transaction = Transaction::new(
        payload.account_id,
        payload.amount,
        payload.currency,
        payload.merchant,
    );

    // Serialize the transaction to a JSON string — this is the actual
    // bytes that will travel through Kafka.
    let payload_json = serde_json::to_string(&transaction)
        .map_err(|e| (axum::http::StatusCode::INTERNAL_SERVER_ERROR, e.to_string()))?;
    // `?` here: if serialization fails, immediately return the Err
    // variant, converted via map_err into our (StatusCode, String)
    // error type, instead of continuing execution.

    // Build the Kafka message. FutureRecord borrows its key/payload, so
    // we pass references (&transaction.account_id, &payload_json).
    // The KEY matters: Kafka guarantees all messages with the same key
    // land on the same partition, IN ORDER. We use account_id as the
    // key so all of one account's transactions are ordered relative to
    // each other — required for rules like "3 transactions in 60s".
    let record = FutureRecord::to("transactions.raw")
        .key(&transaction.account_id)
        .payload(&payload_json);

    // .send() returns a Future — we .await it, with a timeout of 0
    // meaning "use the producer's own configured timeout" (the
    // message.timeout.ms we set to 5000 above) rather than an
    // additional one here.
    state
        .producer
        .send(record, Duration::from_secs(0))
        .await
        .map_err(|(kafka_err, _owned_record)| {
            // rdkafka's send() error is a tuple: (the actual error, the
            // record we tried to send, handed back so you could retry
            // it). We only care about the error here, so we discard the
            // record with `_owned_record`.
            metrics::counter!("kafka_publish_errors_total").increment(1); // ADD
            (
                axum::http::StatusCode::INTERNAL_SERVER_ERROR,
                format!("failed to publish to kafka: {kafka_err}"),
            )
        })?;
    metrics::counter!("transactions_published_total").increment(1); // ADD

    // Success — echo the full transaction (including generated id and
    // timestamp) back to the caller as JSON, with axum's default 200 OK.
    Ok(Json(transaction))
}
