// -----------------------------------------------------------------------
// common crate — shared types used by both `ingestion` and `processor`.
// This is a *library* crate (no main.rs), so its only job is to define
// things other crates import. Keeping Transaction here means ingestion
// (which builds one from an HTTP request) and processor (which reads one
// back off Kafka) are always working with the exact same shape of data —
// no risk of the two sides drifting apart.
// -----------------------------------------------------------------------

// Bring external types into scope, same idea as imports in any language.
// Without these we'd have to write the fully-qualified path everywhere,
// e.g. chrono::DateTime<chrono::Utc> instead of just DateTime<Utc>.
use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use uuid::Uuid;

// #[derive(...)] is an attribute that auto-generates code for the struct
// directly below it, instead of us hand-writing it:
//   Debug        -> lets us print the struct with {:?} for logging
//   Clone        -> lets us copy a Transaction with .clone() (useful once
//                   we hand one to the Kafka producer but still want to
//                   log or return a copy)
//   Serialize    -> serde trait: struct -> JSON
//   Deserialize  -> serde trait: JSON -> struct
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Transaction {
    // Unique ID per transaction, generated server-side (never trusted
    // from client input). Lets us trace one transaction through every
    // stage of the pipeline: raw topic -> flagged topic -> ClickHouse row.
    pub id: Uuid,

    // Whose account this belongs to. This is the field we'll partition
    // the Kafka topic on later, so all of one account's transactions
    // land on the same partition IN ORDER — required for rules like
    // "3 transactions from this account within 60 seconds".
    pub account_id: String,

    // The transaction amount. f64 (floating point) is fine for a
    // learning project; real money-handling systems usually store
    // integer cents instead, to avoid float rounding errors. Worth
    // knowing, not a blocker here.
    pub amount: f64,

    // e.g. "USD". Plain String for now — could become an enum later
    // once we know the fixed set of currencies we actually support.
    pub currency: String,

    // Who the transaction was with.
    pub merchant: String,

    // When the transaction happened. Always stored in UTC so there's
    // no ambiguity once ingestion, processor, and ClickHouse are all
    // potentially running in different places/timezones.
    pub timestamp: DateTime<Utc>,
    // pub on every field above makes them visible outside this crate.
    // `pub` on the struct itself (below) does the same for the type —
    // without it, `ingestion` and `processor` couldn't import Transaction
    // at all.
}

// -----------------------------------------------------------------------
// FlaggedTransaction — wraps a Transaction that a fraud rule flagged as
// suspicious, plus WHY it was flagged. This is what the processor
// produces onto "transactions.flagged", and eventually what ClickHouse
// stores in its "flagged" table for the dashboard to query.
// -----------------------------------------------------------------------
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct FlaggedTransaction {
    // We keep the ORIGINAL transaction nested inside, rather than
    // flattening its fields in here. That way if Transaction ever grows
    // new fields, FlaggedTransaction automatically has them too — no
    // duplicate field list to keep in sync.
    pub transaction: Transaction,

    // Human-readable reason, e.g. "amount exceeds $10,000 threshold".
    // A Vec because a transaction could trip more than one rule at once
    // — we don't want to pick just one and discard the rest.
    pub reasons: Vec<String>,

    // When the processor evaluated and flagged it — separate from
    // transaction.timestamp (when the transaction itself happened),
    // since there's always some processing lag between the two.
    pub flagged_at: DateTime<Utc>,
}

// impl blocks attach methods/functions to a struct, separate from the
// struct's field definitions. This is different from most OOP languages,
// where data and behavior are usually declared together in one class body.
impl Transaction {
    // Constructor. `new` is just a naming convention, not a keyword —
    // Rust doesn't have special "constructor" syntax.
    // `Self` means "whatever type this impl block belongs to", i.e.
    // Transaction — writing Self instead of Transaction everywhere means
    // if we ever rename the struct, this function doesn't need edits.
    pub fn new(account_id: String, amount: f64, currency: String, merchant: String) -> Self {
        Self {
            // id and timestamp are generated INSIDE the constructor,
            // not taken as parameters. That's deliberate: the caller
            // only supplies the "business" fields, and nobody outside
            // this function can fabricate a transaction ID or backdate
            // a timestamp.
            id: Uuid::new_v4(), // v4 = randomly generated UUID
            account_id,         // shorthand for `account_id: account_id`
            amount,
            currency,
            merchant,
            timestamp: Utc::now(),
        }
    }
}
pub mod metrics;