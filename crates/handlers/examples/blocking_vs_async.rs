//! cargo run -p engine-handlers --example blocking_vs_async
//!
//! Prints heartbeat lag on a 2-worker runtime with 4 tasks that sleep 500 ms
//! either with tokio::time::sleep (good) or std::thread::sleep (bad).

use std::time::Duration;

use engine_handlers::diagnostics::{SleepKind, heartbeat_lag_experiment};

fn main() {
    let (workers, load, sleep, tick) =
        (2, 4, Duration::from_millis(500), Duration::from_millis(10));
    println!(
        "runtime worker threads = {workers}, load tasks = {load}, each sleeps {sleep:?}, heartbeat every {tick:?}\n"
    );
    for kind in [SleepKind::Async, SleepKind::Blocking] {
        let r = heartbeat_lag_experiment(kind, workers, load, sleep, tick);
        println!(
            "{:<9} heartbeat ticks={:<4} max lag={:>9.1?} mean lag={:>9.1?} load finished after {:>9.1?}",
            format!("{:?}", r.kind),
            r.ticks,
            r.max_lag,
            r.mean_lag,
            r.total
        );
    }
    println!("\nBlocking sleep parks runtime threads: timers, sockets and other tasks stall.");
}
