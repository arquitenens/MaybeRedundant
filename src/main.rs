use std::thread::JoinHandle;
use crate::scheduler::{SubScheduler, TASK_SLOTS};
use crate::task::Task;

mod scheduler;
mod builder;
mod worker;
mod task;
mod config;

fn main() {
    unsafe { println!("jh = {:?}", size_of::<Task>()); }
    unsafe { println!("jh = {:?}", size_of_val(&*&raw const TASK_SLOTS)); }
    println!("jh = {:?}", size_of::<Option<JoinHandle<()>>>())
}
