use std::hint::black_box;
use std::thread::JoinHandle;
use std::time::Instant;
use crate::config::ThreadAmount;
use crate::scheduler::{Scheduler, SubScheduler, TASK_SLOTS};
use crate::task::Task;

mod scheduler;
mod builder;
mod worker;
mod task;
mod config;

struct Post;
struct Fetch;

fn counter_task1() -> impl FnMut() + Send + 'static {
    move || {
        let mut x: i64 = black_box(67);
        for i in 0..200 {
            x = x.wrapping_mul(0x9E3779B97F4A7C1).wrapping_add(i);

            x ^= x >> 27;
            x = x.rotate_left(17);

            x = x.wrapping_mul(x | 1);
        }

    }
}
fn test_task1() -> impl FnMut() + Send + 'static {
    move || {
        //println!("hi");
    }
}

fn main() {
    let mut sh = Scheduler::new(config::Config::default())
        .add_scheduler::<Post>(ThreadAmount::Default)
        .add_scheduler::<Fetch>(ThreadAmount::Default)
        .register_task(test_task1())
        .register_task(counter_task1())
        .apply();

    let now = Instant::now();
    for i in 0..5_000_000{
        let x = sh.any_task::<_, Fetch>(test_task1(), false);
        let y = sh.any_task::<_, Post>(test_task1(), false);
        black_box(x);
        black_box(y);
    }
    let elapsed = now.elapsed();
    println!("Elapsed: {:.2?}", elapsed);

    black_box(sh);
}
