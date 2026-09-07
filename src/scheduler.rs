use std::cell::UnsafeCell;
use std::mem::{transmute, MaybeUninit};
use std::ptr::{addr_of_mut, null, null_mut};
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicPtr, AtomicU64, Ordering};
use std::thread::JoinHandle;
use crate::builder::{SchedulerBuilder, TypesIdx};
use crate::config::{Config, MAX_SUB_SCHEDULERS, MAX_WORKERS_PER_SCHED};
use crate::task::Task;
use crate::worker::Worker;




//global task slots
pub static mut TASK_SLOTS: [Task; 128] = [const { Task::const_default() }; 128];

//Each Worker gets their own reference to this, must be accessed atomically
static WORKER_STATE: AtomicU64 = AtomicU64::new(0);

#[repr(align(64))]
#[derive(Clone, Copy)]
///64 byte aligned T
pub(crate) struct Padded<T>(pub(crate) T);
impl<T> Padded<T> {
    #[inline(always)]
    fn get(&mut self) -> &mut T {
        &mut self.0
    }
}

pub(crate) struct Scheduler {
    generic_schedulers: [SubScheduler; MAX_SUB_SCHEDULERS],
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
    //TODO but also there is not really a point in having it be on the heap?
    handles: Box<[Option<JoinHandle<()>>; MAX_WORKERS_PER_SCHED]>,

    //every worker has their own UNIQUE index into it and terminates once it's set to true
    //since its mostly only reads they can be in the same cache-line
    worker_terminate: Padded<[AtomicBool; MAX_WORKERS_PER_SCHED]>,

}

static mut SUB_SCHEDULERS:
[MaybeUninit<SubScheduler>; MAX_SUB_SCHEDULERS] =
    [const { MaybeUninit::uninit() }; MAX_SUB_SCHEDULERS];
impl SubScheduler {
    fn new<T: TypesIdx>(workers: usize, offset: usize) -> *mut Self {

        let handles: Box<[Option<JoinHandle<()>>; MAX_WORKERS_PER_SCHED]> = Box::new([const { None }; MAX_WORKERS_PER_SCHED]);

        let terminate = Padded([const { AtomicBool::new(false) }; MAX_WORKERS_PER_SCHED]);

        //TODO is this sound?
        let incomplete_schedulers: *mut SubScheduler = unsafe {(&raw mut SUB_SCHEDULERS[T::get_or_register_tid()]).cast::<SubScheduler>()};

        unsafe {
            std::ptr::write(&raw mut (*incomplete_schedulers).worker_terminate, terminate);
            std::ptr::write(&raw mut (*incomplete_schedulers).handles, handles);
            std::ptr::write(&raw mut (*incomplete_schedulers).workers, workers);
            std::ptr::write(&raw mut (*incomplete_schedulers).offset, offset);

            for w in 0..workers {
                //let term = &raw mut (*incomplete_schedulers).worker_terminate.get()[w];
                let worker = Worker::new(w, 0,
                                         &(*incomplete_schedulers).worker_terminate.get()[w],
                                         AtomicPtr::new(&raw mut TASK_SLOTS[w])
                );
                let h = std::thread::spawn(move || {
                    worker.run()
                });
                (*incomplete_schedulers).handles[w].replace(h);
            }
            return incomplete_schedulers.cast::<SubScheduler>();
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
