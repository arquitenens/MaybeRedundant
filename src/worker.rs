use crate::scheduler::WORKER_STATE;
use crate::task::Task;
use core::sync::atomic::{AtomicBool, AtomicPtr, Ordering};
use std::sync::atomic::Ordering::Release;
use std::thread::sleep;
use std::time::Duration;

struct UnsafePtr<T>(*mut T);
unsafe impl<T> Send for UnsafePtr<T> {}

pub(crate) struct Worker{
    pub(crate) idx: usize,

    //signal to stop execution of the worker, usually when the parent is being dropped
    pub(crate) signal: &'static AtomicBool,

    //the worker doesn't care if the task came from cache or from the queue
    //does not need to be atomically loaded/swapped technically
    pub(crate) current_task: AtomicPtr<Task>,

    //occasionally request a heartbeat to check if a given worker crashed or stalled
    //or otherwise takes a long time to check itself "unbusy"
    has_heartbeat: &'static AtomicBool,
}


impl Worker {
    pub(crate) fn new(idx: usize, slot: usize, has_heartbeat: &'static AtomicBool, signal: &'static AtomicBool, current_task: AtomicPtr<Task>) -> Self {
        Self{
            idx,
            has_heartbeat,
            signal,
            current_task,
        }
    }
    pub(crate) fn run(self) {
        unsafe {
            //TODO very dirty and unsafe, will cleanup so no reason to document just yet
            loop {
                if !self.has_heartbeat.load(Ordering::Relaxed){
                    //self.has_heartbeat.store(true, Ordering::Relaxed);
                }


                if self.signal.load(Ordering::Acquire) {
                    //dbg!("Dropping Worker {}", self.idx);
                    let old = self.current_task.load(Ordering::Acquire).replace(Task::empty());
                    if !old.data.is_null() { (old.dropper)(old.data); }

                    //TODO Maybe send acknowledge signal so the worker doesn't drop a tad bit too early
                    self.signal;

                    break;
                }

                let current_task = self.current_task.load(Ordering::Acquire);

                if current_task.is_null() {
                    continue;
                }

                if (*current_task).data.is_null(){
                    continue;
                }

                (*current_task).execute();

                //let ptr = WORKER_STATE[self.idx].get().as_ptr();
                current_task.replace(Task::empty());

                //ptr.write_volatile(*ptr | 1 << self.idx);
                WORKER_STATE[self.idx].get().fetch_or(1 << self.idx, Release);

            }
        }


    }
}