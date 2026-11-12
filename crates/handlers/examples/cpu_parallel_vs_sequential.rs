//! cargo run --release -p engine-handlers --example cpu_parallel_vs_sequential -- [to] [threads...]
//!
//! Measures prime counting in [0, to) sequentially and on dedicated Rayon
//! pools of several sizes. Numbers are *measured on your machine*; nothing is
//! assumed. Run in --release: debug builds are 10-50x slower.

use std::time::Instant;

use engine_handlers::cpu::{count_primes_parallel, count_primes_sequential};
use tokio_util::sync::CancellationToken;

fn main() {
    let args: Vec<String> = std::env::args().skip(1).collect();
    let to: u64 = args
        .first()
        .and_then(|s| s.parse().ok())
        .unwrap_or(10_000_000);
    let logical = std::thread::available_parallelism()
        .map(|n| n.get())
        .unwrap_or(1);
    let thread_counts: Vec<usize> = if args.len() > 1 {
        args[1..].iter().filter_map(|s| s.parse().ok()).collect()
    } else {
        let mut v = vec![1, 2, 4, 8, logical];
        v.sort_unstable();
        v.dedup();
        v
    };
    println!("available_parallelism (logical CPUs visible to this process) = {logical}");
    println!("range = [0, {to})\n");

    let s = Instant::now();
    let seq = count_primes_sequential(0, to);
    let seq_ms = s.elapsed().as_secs_f64() * 1000.0;
    println!("sequential           : {seq} primes in {seq_ms:>9.1} ms");

    let token = CancellationToken::new();
    for threads in thread_counts {
        let pool = rayon::ThreadPoolBuilder::new()
            .num_threads(threads)
            .build()
            .unwrap();
        let s = Instant::now();
        let (count, used, chunks) = pool
            .install(|| count_primes_parallel(0, to, (to / 256).max(1000), &token))
            .unwrap();
        let ms = s.elapsed().as_secs_f64() * 1000.0;
        assert_eq!(count, seq);
        println!(
            "rayon threads = {threads:>3}  : {count} primes in {ms:>9.1} ms  speedup x{:.2}  ({} chunks, {} threads did work)",
            seq_ms / ms,
            chunks,
            used.len()
        );
    }
}
