use crate::config::ThreadAmount;
use crate::scheduler::{Scheduler, DIRTY_ITER};
use core::hint::black_box;
use core::sync::atomic::AtomicU64;
use core::sync::atomic::Ordering::{Acquire, Release};
use std::time::Instant;

mod scheduler;
mod builder;
mod worker;
mod task;
mod config;
mod IdxCache;

struct Post;
struct Fetch;

fn counter_task1(x: &'static AtomicU64) -> impl FnMut() + Send + 'static {
    move || {
        x.fetch_add(1, Release);
    }
}
fn test_task1(x: &'static AtomicU64) -> impl FnMut() + Send + 'static {
    move || {
        x.fetch_add(1, Release);
    }
}

#[inline(never)]
pub fn test(sh: &mut Scheduler, counter: &'static AtomicU64) {
    let t = sh.any_task::<_, Fetch>(
        counter_task1(counter),
    );

    std::hint::black_box(t);
}

fn main() {
    let counter = &*Box::leak(Box::new(AtomicU64::new(0)));

    let mut sh = Scheduler::new(config::Config::default())
        .add_scheduler::<Post>(ThreadAmount::Default)
        .add_scheduler::<Fetch>(ThreadAmount::Default)
        .register_task(test_task1(counter))
        .register_task(counter_task1(counter))
        .apply();

    test(&mut sh, &counter);

    let now = Instant::now();
    for _ in 0..5_000_000 {
        let t = sh.any_task::<_, Fetch>(counter_task1(counter));
        let _ = black_box(t);
        let y = sh.any_task::<_, Post>(test_task1(counter));
        let _ = black_box(y);

    }
    let elapsed = now.elapsed();
    println!("Elapsed: {:.2?}", elapsed);

    println!("counter: {:?}", counter);
    
    println!("iters: {:?}", DIRTY_ITER.load(Acquire));
    black_box(sh);
}
