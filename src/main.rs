use crate::config::ThreadAmount;
use crate::scheduler::{Scheduler, WorkerError, DIRTY_ITER, WORKER_AVERAGE, WORKER_STATE};
use core::hint::black_box;
use core::sync::atomic::AtomicU64;
use core::sync::atomic::Ordering::{Acquire, Release};
use std::thread::sleep;
use std::time::{Duration, Instant};

mod scheduler;
mod builder;
mod worker;
mod task;
mod config;
mod idx_cache;

struct Post;
struct Fetch;

fn test_task2(x: &'static AtomicU64) -> impl FnMut() + Send + 'static {
    move || {
        x.fetch_add(1, Release);
    }
}
fn test_task1(x: &'static AtomicU64) -> impl FnMut() + Send + 'static {
    move || {
        x.fetch_add(1, Release);
    }
}

fn empty_task1() -> impl FnMut() + Send + 'static {
    move || {

    }
}
fn empty_task2() -> impl FnMut() + Send + 'static {
    move || {

    }
}



fn main() {
    let counter = Box::leak(Box::new(AtomicU64::new(0)));

    let mut sh = Scheduler::new(config::Config::default())
        .add_scheduler::<Post>(ThreadAmount::Default)
        .add_scheduler::<Fetch>(ThreadAmount::Default)
        .register_task(&test_task1(counter))
        .register_task(&test_task2(counter))
        .apply();



    let now = Instant::now();
    for _ in 0..5_000_0000 {
        let t = sh.any_task::<_, Fetch>(test_task1(counter));
        black_box(t);

        let y = sh.any_task::<_, Post>(test_task2(counter));
        black_box(y);

    }
    let elapsed = now.elapsed();
    println!("Elapsed: {:.2?}", elapsed);

    println!("counter: {:?}", counter);
    
    println!("iters: {:?}", DIRTY_ITER.load(Acquire));
    println!("workers: {:?}", WORKER_AVERAGE.load(Acquire) / 1_000_000);
    black_box(sh);
}
