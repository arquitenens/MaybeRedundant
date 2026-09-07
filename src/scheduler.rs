use std::mem::MaybeUninit;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::thread::JoinHandle;
use crate::builder::SchedulerBuilder;
use crate::config::Config;
use crate::task::Task;


//CONSTS
const MAX_WORKERS_PER_SCHED: usize = 64;
const MAX_SCHEDULERS: usize = 64;



//global task slots
pub static mut TASK_SLOTS: [Task; 128] = [const { Task::const_default() }; 128];

//Each Worker gets their own reference to this, must be accessed atomically
static WORKER_STATE: AtomicU64 = AtomicU64::new(0);

#[repr(align(64))]
#[derive(Clone, Copy)]
pub(crate) struct Padded<T>(pub(crate) T);
impl<T> Padded<T> {
    #[inline(always)]
    fn get(&mut self) -> &mut T {
        &mut self.0
    }
}

pub(crate) struct Scheduler {
    generic_schedulers: [SubScheduler; MAX_SCHEDULERS],
}
impl Scheduler {
    fn new(config: Config) -> SchedulerBuilder {
        SchedulerBuilder{
            config,
            incomplete: MaybeUninit::uninit(),
            registrations: 0,
        }
    }
}

#[repr(align(64))]
//dont reorder evil compiler grrr
#[repr(C)]
//128bytes
pub(crate) struct SubScheduler{
    //amount of workers
    workers: usize,
    //offset within the global "WORKER_STATE" mask
    offset: usize,

    //its rarely used, the pointer indirection shouldn't matter
    handles: Box<[Option<JoinHandle<()>>; MAX_WORKERS_PER_SCHED]>,

    //every worker has their own UNIQUE index into it and terminates once it's set to true
    //since its mostly only reads they can be in the same cache-line
    worker_terminate: Padded<[AtomicBool; MAX_WORKERS_PER_SCHED]>,

}
impl SubScheduler {
    fn new(idx: usize, workers: usize, offset: usize) -> Self {

        let mut handles: [Option<JoinHandle<()>>; MAX_WORKERS_PER_SCHED] = [const { None }; MAX_WORKERS_PER_SCHED];

        for w in 0..workers{
            let h = std::thread::spawn(move || {

            });
            handles[w].replace(h);
        }

        let terminate = Padded([const { AtomicBool::new(false) }; MAX_WORKERS_PER_SCHED]);

        Self{
            workers,
            offset,
            handles: Box::new(handles),
            worker_terminate: terminate,
        }
    }
}


impl Drop for SubScheduler {
    //this might drop a non-finished task, it's on you to make sure the scheduler lives long enough
    //for every task to complete if you care
    fn drop(&mut self) {
            #[cfg(debug_assertions)]
            eprintln!("sub-scheduler = {:p} has been dropped", self);
            for (idx, h) in self.handles.iter_mut().enumerate(){
            self.worker_terminate.get()[idx].store(true, Ordering::Release);
            let _ = h.take().unwrap().join();
        }
    }
}
