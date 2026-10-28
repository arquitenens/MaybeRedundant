use crate::config::ThreadAmount;
use crate::scheduler::{Scheduler, TaskWrapper, WorkerError, MAIL_BOX, WORKER_STATE};
use core::hint::black_box;

use core::sync::atomic::AtomicU64;
use core::sync::atomic::Ordering::{Acquire, Release};
use core::ops::Deref;
use std::any::TypeId;
use std::sync::atomic::AtomicBool;
use std::thread::{available_parallelism, sleep};
use std::time::{Duration, Instant};
use crate::idx_cache::{IdxCache, SchedCache};

mod scheduler;
mod builder;
mod worker;
mod task;
mod config;
mod idx_cache;

fn test_task2(mut x: u64) -> impl FnMut() {
    move || {
        x += 1;
        //println!("task2: {}", x);
    }
}
fn test_task1(x: &mut u64) -> impl FnMut() {
    || {
        *x += 1;
    }
}


fn test_task3(x: &AtomicU64) -> impl FnMut() {
    || {
        x.fetch_add(1, Release);
    }
}
fn task_finished(flag: &AtomicBool) -> impl FnMut() {
    || {
        flag.store(true, Release);
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

fn make() -> impl FnMut() {
    let mut buf = Vec::new();
    move || {
        buf.push(1);
    }
}

struct Post;
struct Fetch;

fn main() {
    let counter1 = Box::leak(Box::new(0u64));
    let counter2 = &mut 0u64;
    let atomic_counter = Box::leak(Box::new(AtomicU64::new(0)));
    let task_completion = Box::leak(Box::new(AtomicBool::new(false)));
    let mut sh = Scheduler::<0>::new(config::Config::default())
        .add_scheduler::<Post>(ThreadAmount::Default)
        .add_scheduler::<Fetch>(ThreadAmount::Default)
        .apply();
    let now = Instant::now();
    let mut counter: u64 = 0;


     unsafe {
         for i in 0..10_000_000{
             let create1 = create_task_with_callback!(empty_task1(), task_finished(task_completion));
             let create2 = create_task!(empty_task1());
             let x1 = sh.any_task_locking::<_, Post>(create1);
             sh.block_until(x1, || {
                 return task_completion.load(Acquire);
             });
             let x2 = sh.any_task_locking::<_, Fetch>(create2);
             sh.block_until_arrival(x2);
             counter += 1;
         }
     }

    println!("elapsed {:?}", now.elapsed());
    println!("counter {:?}", counter);
    println!("counter2 {:?}", counter2);
    println!("atomic counter {:?}", atomic_counter.load(Acquire));
    

    black_box(sh);
}
