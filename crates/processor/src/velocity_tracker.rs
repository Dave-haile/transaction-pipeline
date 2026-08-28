// ============================================================================
// velocity_tracker.rs
//
// WHAT THIS FILE DOES
// --------------------
// Implements a *stateful* fraud rule: "flag an account if it has made more
// than THRESHOLD transactions within WINDOW_SECS seconds."
//
// This is different from your existing rules (amount > $10k, hour 1-4am UTC)
// because those only need to look at the ONE transaction currently being
// processed. This rule needs to remember what happened *recently* for a
// given account, across many separate messages coming off Kafka. That means
// we need state that:
//   1. Lives longer than a single function call (persists across messages)
//   2. Is keyed per-account (account A's history shouldn't affect account B)
//   3. Is safe to touch from wherever your Kafka consumer loop calls it
//
// WHY A HashMap<String, VecDeque<DateTime<Utc>>> BEHIND A MUTEX
// ---------------------------------------------------------------
// - HashMap<account_id, ...>  -> O(1) lookup of "this account's history"
// - VecDeque<DateTime<Utc>>   -> a double-ended queue. We push new
//     timestamps onto the back and pop expired ones off the front. A
//     VecDeque is the right tool here because both of those operations are
//     O(1), whereas doing the same with a Vec (push_back is fine, but
//     removing from the front is O(n) because everything has to shift).
// - Mutex<...>                -> your Kafka consumer processes messages one
//     at a time in this design, but we wrap it in a Mutex anyway because (a)
//     it's cheap, (b) it makes the type Send + Sync so it can live in Arc
//     and be shared if you ever add concurrent consumers, and (c) it's the
//     same pattern you already know from the Axum exercise.
// - Arc<...>                  -> lets you clone a handle to the SAME
//     underlying map and pass it into your processing function / task
//     without copying the data. Arc = "Atomically Reference Counted" shared
//     ownership.
// ============================================================================

use chrono::{DateTime, Utc};
use std::collections::{HashMap, VecDeque};
use std::sync::{Arc, Mutex};

/// Shared, cloneable handle to the velocity-tracking state.
///
/// We wrap the Mutex<HashMap<...>> in its own struct (rather than passing
/// the raw Arc<Mutex<HashMap<...>>> type around everywhere) for two reasons:
///   1. It lets us attach methods (record_and_check) directly to it, so call
///      sites read as `tracker.record_and_check(...)` instead of manually
///      locking a mutex inline everywhere — that's easy to get wrong.
///   2. It keeps the locking logic in ONE place, so if you ever change the
///      data structure or add metrics/logging around lock acquisition, you
///      only touch this file.
#[derive(Clone)]
pub struct VelocityTracker {
    // Arc so cloning VelocityTracker (e.g. to move into a consumer task)
    // shares the same underlying map rather than making a fresh empty one.
    history: Arc<Mutex<HashMap<String, VecDeque<DateTime<Utc>>>>>,
}

impl VelocityTracker {
    /// Create a new, empty tracker. Call this ONCE at processor startup —
    /// not once per message — and clone the handle into wherever needs it.
    pub fn new() -> Self {
        VelocityTracker {
            history: Arc::new(Mutex::new(HashMap::new())),
        }
    }

    /// Records a transaction for `account_id` at `timestamp`, then checks
    /// whether that account has exceeded `threshold` transactions within
    /// the trailing `window_secs` seconds (counting the one we just added).
    ///
    /// Returns `true` if the account should be flagged.
    ///
    /// Why does this one method both RECORD and CHECK, instead of two
    /// separate methods? Because they need to happen under the same mutex
    /// lock to be correct — if we unlocked between "record" and "check",
    /// another thread could sneak in a change and we'd check stale data.
    /// Doing both while holding one lock keeps it atomic.
    pub fn record_and_check(
        &self,
        account_id: &str,
        timestamp: DateTime<Utc>,
        window_secs: i64,
        threshold: usize,
    ) -> bool {
        // .lock() returns a MutexGuard, which derefs to the HashMap.
        // .unwrap() is fine here (not a real error path): it only fails if
        // another thread PANICKED while holding the lock, which would mean
        // something else already went badly wrong.
        let mut history = self.history.lock().unwrap();

        // entry(...).or_insert_with(...) is the idiomatic Rust way to say
        // "get the VecDeque for this account, or create an empty one if
        // this is the account's first transaction we've seen."
        let timestamps = history
            .entry(account_id.to_string())
            .or_insert_with(VecDeque::new);

        // Step 1: record the new transaction's timestamp.
        timestamps.push_back(timestamp);

        // Step 2: evict anything now outside the trailing window.
        // We compute the cutoff instant, then pop from the FRONT of the
        // deque (the oldest entries) as long as they're older than the
        // cutoff. This is why VecDeque matters: pop_front is O(1).
        let cutoff = timestamp - chrono::Duration::seconds(window_secs);
        while let Some(&oldest) = timestamps.front() {
            if oldest < cutoff {
                timestamps.pop_front();
            } else {
                // Timestamps arrive in order, so once we hit one that's
                // still inside the window, everything after it is too —
                // safe to stop early instead of scanning the whole deque.
                break;
            }
        }

        // Step 3: whatever's left in the deque is "transactions from this
        // account within the last window_secs seconds," including the one
        // we just added. Compare against the threshold.
        timestamps.len() > threshold

        // NOTE: the MutexGuard (`history`) is dropped here at the end of
        // the function, which releases the lock automatically. You don't
        // need to call anything explicitly — that's RAII, one of the big
        // ideas Rust leans on instead of manual lock/unlock calls.
    }
}

// ============================================================================
// A note on memory growth: this map never removes an account_id entry, only
// old timestamps within an entry. For a portfolio project this is fine, but
// worth knowing: in a long-running production system you'd eventually want
// to periodically sweep out accounts with empty deques, or use a crate like
// `dashmap` with a TTL, so memory doesn't grow unboundedly with every unique
// account_id you've ever seen. Flagging this now so it's a conscious
// tradeoff, not a surprise later.
// ============================================================================

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn flags_when_over_threshold_within_window() {
        let tracker = VelocityTracker::new();
        let now = Utc::now();

        // threshold = 3, so the 4th transaction within 60s should flag.
        assert!(!tracker.record_and_check("acct-1", now, 60, 3));
        assert!(!tracker.record_and_check("acct-1", now, 60, 3));
        assert!(!tracker.record_and_check("acct-1", now, 60, 3));
        assert!(tracker.record_and_check("acct-1", now, 60, 3));
    }

    #[test]
    fn does_not_flag_once_old_transactions_expire_out_of_window() {
        let tracker = VelocityTracker::new();
        let t0 = Utc::now();

        // 3 transactions right at t0.
        tracker.record_and_check("acct-2", t0, 60, 2);
        tracker.record_and_check("acct-2", t0, 60, 2);

        // A 4th transaction 61 seconds later — outside the 60s window, so
        // the earlier ones should have been evicted and this should NOT flag.
        let t1 = t0 + chrono::Duration::seconds(61);
        assert!(!tracker.record_and_check("acct-2", t1, 60, 2));
    }

    #[test]
    fn tracks_accounts_independently() {
        let tracker = VelocityTracker::new();
        let now = Utc::now();

        tracker.record_and_check("acct-a", now, 60, 1);
        // acct-b's first transaction should not be affected by acct-a's history.
        assert!(!tracker.record_and_check("acct-b", now, 60, 1));
    }
}
