// -----------------------------------------------------------------------
// processor service — the middle of the pipeline.
// Consumes transactions from "transactions.raw", runs fraud/anomaly
// rules against each one, and republishes anything suspicious onto
// "transactions.flagged". Everything (suspicious or not) will later
// also get written to ClickHouse — we'll add that in the next step.
// -----------------------------------------------------------------------
use clickhouse::Row; // derive macro that makes a struct insertable/queryable
use common::{FlaggedTransaction, Transaction};
use futures::StreamExt; // gives us .next() on the Kafka message stream
use rdkafka::{
    ClientConfig,
    Message, // Message trait lets us call .payload() on a Kafka message
    consumer::{Consumer, StreamConsumer},
    producer::{FutureProducer, FutureRecord},
};
// use std::time::Duration;
use uuid::Uuid;
mod velocity_tracker;
use velocity_tracker::VelocityTracker; // ADD THIS IMPORT

// -----------------------------------------------------------------------
// TransactionRow — the shape we write into ClickHouse. Deliberately a
// SEPARATE type from Transaction/FlaggedTransaction (not reused), because
// this one is FLAT (ClickHouse tables don't nest structs the way JSON
// does) and combines "was this flagged" into the same row rather than
// two separate tables. Row (from the clickhouse crate) + Serialize let
// this struct be written straight into the "transactions" table.
// -----------------------------------------------------------------------
#[derive(Row, serde::Serialize)]
struct TransactionRow {
    #[serde(with = "clickhouse::serde::uuid")]
    id: Uuid,
    account_id: String,
    amount: f64,
    currency: String,
    merchant: String,
    // DateTime64(3, 'UTC') in the table = millisecond precision, so we
    // use the matching ::millis helper. clickhouse::serde::* modules are
    // how the crate knows to convert chrono's DateTime<Utc> into
    // ClickHouse's on-wire DateTime64 representation — plain serde
    // can't do this conversion on its own.
    #[serde(with = "clickhouse::serde::chrono::datetime64::millis")]
    timestamp: chrono::DateTime<chrono::Utc>,
    flagged_reasons: Vec<String>,
    // Nullable(DateTime64) on the table side = Option<DateTime<Utc>> on
    // the Rust side. The `::option` suffix on the helper module is what
    // makes it accept an Option instead of a bare DateTime.
    #[serde(with = "clickhouse::serde::chrono::datetime64::millis::option")]
    flagged_at: Option<chrono::DateTime<chrono::Utc>>,
}

#[tokio::main]
async fn main() {
    dotenvy::dotenv().ok();
    tracing_subscriber::fmt::init();

    let prometheus_handle = common::metrics::install_recorder();

    tokio::spawn(async move {
        let app = common::metrics::metrics_router(prometheus_handle);
        let listener = tokio::net::TcpListener::bind("0.0.0.0:9091")
            .await
            .expect("failed to bind metrics port 9091");
        axum::serve(listener, app)
            .await
            .expect("metrics server crashed");
    });
    let kafka_brokers = std::env::var("KAFKA_BOOTSTRAP_SERVERS")
    .expect("KAFKA_BOOTSTRAP_SERVERS must be set — check .env for local runs, or docker-compose environment: for containers");
    let tracker = VelocityTracker::new();
    // --- consumer setup ---
    let consumer: StreamConsumer = ClientConfig::new()
        .set("bootstrap.servers", &kafka_brokers)
        // group.id identifies this consumer as part of a "consumer
        // group". Kafka tracks read progress (offsets) PER GROUP, so if
        // we ever run multiple processor instances with the SAME
        // group.id, Kafka automatically splits the topic's partitions
        // between them instead of both reading everything.
        .set("group.id", "processor-group")
        // "earliest" = if this group has never consumed before, start
        // from the beginning of the topic instead of only new messages.
        // Useful while developing, since we can re-run the processor
        // and reprocess transactions we already sent via curl.
        .set("auto.offset.reset", "earliest")
        .create()
        .expect("failed to create Kafka consumer");

    // Tell the consumer which topic(s) to read from. It's a slice
    // (&[...]) because you can subscribe to multiple topics at once —
    // we only need one for now.
    consumer
        .subscribe(&["transactions.raw"])
        .expect("failed to subscribe to transactions.raw");

    // --- producer setup (for re-publishing flagged transactions) ---
    let producer: FutureProducer = ClientConfig::new()
        .set("bootstrap.servers", &kafka_brokers)
        .set("message.timeout.ms", "5000")
        .create()
        .expect("failed to create Kafka producer");

    // Client::default() + with_url/with_database is a builder, same
    // pattern as ClientConfig for Kafka above. Note this talks to
    // ClickHouse over HTTP (port 8123), not the native 9000 port —
    // that's what clickhouse-client (the CLI) uses, not this crate.
    let clickhouse_url = std::env::var("CLICKHOUSE_URL")
        .expect("CLICKHOUSE_URL must be set — check .env for local runs, or docker-compose environment: for containers");

    let ch_client = clickhouse::Client::default()
        .with_url(&clickhouse_url)
        .with_database("transaction_pipeline")
        // Force synchronous inserts. Some ClickHouse server versions default
        // to async_insert = 1, which buffers writes server-side and responds
        // differently than the clickhouse crate's plain Insert expects —
        // that mismatch is what caused the CANNOT_READ_ALL_DATA error.
        // Explicitly setting both to "0" makes every insert() call through
        // this client behave the simple, predictable way.
        .with_compression(clickhouse::Compression::None)
        .with_option("async_insert", "0")
        .with_option("wait_for_async_insert", "0");

    tracing::info!("processor started, consuming transactions.raw");

    // stream() gives us an async stream of incoming messages.
    // .next().await pulls the next one, or None if the stream ends
    // (won't happen in practice — Kafka streams run forever).
    let mut message_stream = consumer.stream();

    while let Some(message_result) = message_stream.next().await {
        // Each item is a Result — Ok(message) if we successfully
        // received one, Err if something went wrong at the Kafka
        // client level (network blip, etc). We handle both rather
        // than using `?` or `.unwrap()`, because one bad message
        // should never crash the whole processor.
        match message_result {
            Ok(message) => {
                // message.payload() returns Option<&[u8]> — the raw
                // bytes, or None if the message had no payload at all
                // (an empty/tombstone message).
                if let Some(payload_bytes) = message.payload() {
                    // pass the clickhouse client and tracker through now
                    handle_message(payload_bytes, &producer, &ch_client, &tracker).await;
                }
            }
            Err(e) => {
                tracing::warn!("error receiving message from kafka: {e}");
                // We just log and continue the loop — no return/break,
                // so a transient error doesn't kill the whole consumer.
            }
        }
    }
}

// Pulled out into its own function to keep the main loop readable.
// Takes the raw bytes off Kafka and the producer to publish flagged
// results back with.
async fn handle_message(
    payload_bytes: &[u8],
    producer: &FutureProducer,
    ch_client: &clickhouse::Client,
    tracker: &VelocityTracker, // ADD THIS PARAMETER
) {
    // Try to parse the bytes as JSON into our Transaction type.
    // serde_json::from_slice works directly on &[u8], no need to
    // convert to a String first.
    let transaction: Transaction = match serde_json::from_slice(payload_bytes) {
        Ok(t) => t,
        Err(e) => {
            // A message that doesn't parse as a Transaction is bad
            // data — log it and move on. (Later, this is exactly what
            // the transactions.dlq "dead letter queue" topic is for,
            // instead of silently dropping it like we do here for now.)
            tracing::warn!("failed to deserialize transaction: {e}");
            return;
        }
    };

    // Run every rule against this transaction and collect the reasons
    // for any that matched. evaluate_rules returns an empty Vec if
    // nothing tripped.
    let mut reasons = evaluate_rules(&transaction);

    // ADD THE VELOCITY CHECK HERE - inside the message handling, not in the main loop
    if tracker.record_and_check(&transaction.account_id, transaction.timestamp, 60, 5) {
        reasons.push("velocity: >5 transactions in 60s".to_string());
    }

    let is_flagged = !reasons.is_empty();

    let row = TransactionRow {
        id: transaction.id,
        account_id: transaction.account_id.clone(),
        amount: transaction.amount,
        currency: transaction.currency.clone(),
        merchant: transaction.merchant.clone(),
        timestamp: transaction.timestamp,
        flagged_reasons: reasons.clone(),
        flagged_at: if is_flagged {
            Some(chrono::Utc::now())
        } else {
            None
        },
    };

    // Write to ClickHouse for ALL transactions (flagged or not)
    match ch_client.insert("transactions") {
        Ok(mut insert) => {
            if let Err(e) = insert.write(&row).await {
                tracing::warn!("failed to write row to clickhouse: {e}");
            } else if let Err(e) = insert.end().await {
                tracing::warn!("failed to finalize clickhouse insert: {e}");
            }
        }
        Err(e) => tracing::warn!("failed to open clickhouse insert: {e}"),
    }

    if !is_flagged {
        metrics::counter!("transactions_processed_total", "flagged" => "false").increment(1); // ADD
        tracing::info!("transaction {} clean", transaction.id);
        return;
    }
    metrics::counter!("transactions_processed_total", "flagged" => "true").increment(1); // ADD
    tracing::warn!("transaction {} flagged: {:?}", transaction.id, reasons);

    let flagged = FlaggedTransaction {
        transaction: transaction.clone(),
        reasons,
        flagged_at: chrono::Utc::now(),
    };

    // Serialize and publish, same pattern as the ingestion service.
    let payload_json = match serde_json::to_string(&flagged) {
        Ok(j) => j,
        Err(e) => {
            tracing::warn!("failed to serialize flagged transaction: {e}");
            return;
        }
    };

    let record = FutureRecord::to("transactions.flagged")
        .key(&transaction.account_id) // same key strategy as ingestion —
        // keeps one account's flagged transactions ordered too
        .payload(&payload_json);

    if let Err((kafka_err, _)) = producer
        .send(record, std::time::Duration::from_secs(0))
        .await
    {
        tracing::warn!("failed to publish flagged transaction: {kafka_err}");
    }
}

// Our rule engine, v1: two simple, STATELESS rules — each one only
// needs the single transaction in front of it, no memory of past
// transactions. That's deliberate for this first pass; the "N
// transactions in 60 seconds" rule we'll add later is stateful and
// needs the processor to track recent history per account, which is
// more involved.
fn evaluate_rules(transaction: &Transaction) -> Vec<String> {
    let mut reasons = Vec::new();

    // Rule 1: large amount.
    const LARGE_AMOUNT_THRESHOLD: f64 = 10_000.0;
    if transaction.amount > LARGE_AMOUNT_THRESHOLD {
        reasons.push(format!(
            "amount {} exceeds threshold {}",
            transaction.amount, LARGE_AMOUNT_THRESHOLD
        ));
    }

    // Rule 2: odd-hour transaction (1am-4am UTC), a classic simple
    // fraud heuristic — most legitimate purchases don't happen at 2am.
    use chrono::Timelike; // brings .hour() into scope for DateTime
    let hour = transaction.timestamp.hour();
    if (1..4).contains(&hour) {
        reasons.push(format!(
            "transaction occurred at unusual hour ({hour}:00 UTC)"
        ));
    }

    reasons
}
