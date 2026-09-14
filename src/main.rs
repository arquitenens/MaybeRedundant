use crate::config::ThreadAmount;
use crate::scheduler::{Scheduler, TaskWrapper, WorkerError, DIRTY_ITER, WORKER_AVERAGE, WORKER_STATE};
use core::hint::black_box;
use core::sync::atomic::AtomicU64;
use core::sync::atomic::Ordering::{Acquire, Release};
use std::ops::Deref;
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

fn test_task2(x: &mut u64) -> impl FnMut() {
    move || {
        *x += 1;
    }
}
fn test_task1(x: &mut u64) -> impl FnMut() {
    move || {
        *x += 1;
    }
}

fn test_task3(x: &AtomicU64) -> impl FnMut() {
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

    let counter1 = Box::leak(Box::new(0u64));
    let counter2 = Box::leak(Box::new(0u64));
    let atomic_counter = Box::leak(Box::new(AtomicU64::new(0)));
    let mut sh = Scheduler::new(config::Config::default())
        .add_scheduler::<Post>(ThreadAmount::Default)
        .add_scheduler::<Fetch>(ThreadAmount::Default)
        .apply();
    //    Scheduler::create_task(|| test_task1, counter1)
    let now = Instant::now();
    let mut counter = 0;
    while now.elapsed() <= Duration::from_secs(1) {
        let create1 = create_task!(empty_task2());
        let create2 = create_task!(empty_task1());

        let y = sh.any_task::<_, Post>(create1);
        //sh.block_until::<_, Post>(y);
        let t = sh.any_task::<_, Fetch>(create2);
        //sh.block_until::<_, Fetch>(t)
        counter += 1;
    }
    println!("counter: {}", counter);

    let elapsed = now.elapsed();
    println!("counter1: {}", *counter1 + *counter2);
    println!("atomic_counter: {}", atomic_counter.load(Acquire));
    println!("Elapsed: {:.2?}", elapsed);

    
    println!("iters: {:?}", DIRTY_ITER.load(Acquire));
    println!("workers: {:?}", WORKER_AVERAGE.load(Acquire) / 1_000_000);
    black_box(sh);
}
