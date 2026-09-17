use crate::config::ThreadAmount;
use crate::scheduler::{Scheduler, TaskWrapper, WorkerError, IS_DIFFERENT, WORKER_STATE};
use core::hint::black_box;

use core::sync::atomic::AtomicU64;
use core::sync::atomic::Ordering::{Acquire, Release};
use core::ops::Deref;
use std::thread::{available_parallelism, sleep};
use std::time::{Duration, Instant};
use crate::idx_cache::IdxCache;

mod scheduler;
mod builder;
mod worker;
mod task;
mod config;
mod idx_cache;

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


fn empty_task1() -> impl FnMut() + Send {
    move || {

    }
}
fn empty_task2() -> impl FnMut() + Send {
    move || {

    }
}

struct Post;
struct Fetch;

fn main() {
    let counter1 = Box::leak(Box::new(0u64));
    let counter2 = &mut 0u64;
    let atomic_counter = Box::leak(Box::new(AtomicU64::new(0)));
    let mut sh = Scheduler::new(config::Config::default())
        .add_scheduler::<Post>(ThreadAmount::Overwrite(2))
        .add_scheduler::<Fetch>(ThreadAmount::Default)
        .apply();
    let now = Instant::now();
    let mut counter = 0;


     unsafe {
         for _ in 0..5_000_000 {
             let create1 = create_task!(test_task3(atomic_counter));
             let create2 = create_task!(test_task3(atomic_counter));
             let x1 = sh.any_task_lockless::<_, Post>(create1);
             sh.block_until_arrival::<_, Post>(x1);
             let x2 = sh.any_task_lockless::<_, Fetch>(create2);
             sh.block_until_arrival::<_, Fetch>(x2);
             counter += 1;
         }
     }

    println!("counter {:?}", counter);
    println!("atomic counter {:?}", atomic_counter.load(Acquire));
    println!("is different : {}", IS_DIFFERENT.load(Acquire));


    black_box(sh);
}
