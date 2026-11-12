//! Intentionally bad vs good: `std::thread::sleep` vs `tokio::time::sleep`.

use std::time::Duration;

use engine_handlers::diagnostics::{SleepKind, heartbeat_lag_experiment};

#[test]
fn blocking_sleep_starves_the_runtime_but_async_sleep_does_not() {
    let workers = 2;
    let load = 4; // more blocking tasks than runtime worker threads
    let sleep = Duration::from_millis(300);
    let tick = Duration::from_millis(10);

    let good = heartbeat_lag_experiment(SleepKind::Async, workers, load, sleep, tick);
    let bad = heartbeat_lag_experiment(SleepKind::Blocking, workers, load, sleep, tick);

    println!(
        "async    : max lag {:?}, mean {:?}, total {:?}",
        good.max_lag, good.mean_lag, good.total
    );
    println!(
        "blocking : max lag {:?}, mean {:?}, total {:?}",
        bad.max_lag, bad.mean_lag, bad.total
    );

    // With 4 blocking tasks on 2 workers the heartbeat is stalled for at least
    // one full sleep period (300 ms); with async sleep it stays near 0.
    assert!(
        bad.max_lag >= Duration::from_millis(200),
        "blocking max lag {:?}",
        bad.max_lag
    );
    assert!(
        good.max_lag < Duration::from_millis(100),
        "async max lag {:?}",
        good.max_lag
    );
    // Blocking also serializes the load: 4 tasks / 2 threads = 2 rounds.
    assert!(bad.total >= sleep * 2);
}
