use crate::scheduler::WORKER_STATE;
use crate::task::Task;
use core::sync::atomic::{AtomicBool, AtomicPtr, Ordering};
use core::sync::atomic::Ordering::Release;


pub(crate) struct Worker{
    pub(crate) idx: usize,

    pub(crate) tid: usize,
    //signal to stop execution of the worker, usually when the parent is being dropped
    pub(crate) signal: &'static AtomicBool,

    //the worker doesn't care if the task came from cache or from the queue
    //does not need to be atomically loaded/swapped technically
    pub(crate) current_task: AtomicPtr<Task>,

    //occasionally request a heartbeat to check if a given worker crashed or stalled
    //or otherwise takes a long time to check itself "unbusy"
    has_heartbeat: &'static AtomicBool,
}

unsafe fn noop_call(_: *const ()) {}
static EMPTY_TASK: Task = Task::empty();



impl Worker {
    pub(crate) fn new(idx: usize, slot: usize, has_heartbeat: &'static AtomicBool, signal: &'static AtomicBool, current_task: AtomicPtr<Task>) -> Self {
        Self{
            idx,
            tid: slot,
            has_heartbeat,
            signal,
            current_task,
        }
    }
    pub(crate) fn run(self) {
        unsafe {
            //TODO very dirty and unsafe, will cleanup so no reason to document just yet
            //i dont know what to do with this, no pause or yield so idk
            loop {
                // if !self.has_heartbeat.load(Ordering::Relaxed){
                //     //self.has_heartbeat.store(true, Ordering::Relaxed);
                // }


                if self.signal.load(Ordering::Acquire) {
                    //dbg!("Dropping Worker {}", self.idx);
                    let old = self.current_task.load(Ordering::Acquire).replace(Task::empty());
                    if !old.data.is_null() { (old.dropper)(old.data); }

                    //TODO Maybe send acknowledge signal so the worker doesn't drop a tad bit too early
                    self.signal;

                    break;
                }


                let current_task = self.current_task.load(Ordering::Acquire);


                if (*current_task).data.is_null(){
                    continue;
                }

                (*current_task).execute();
                let _ = current_task.replace(Task::empty());
                WORKER_STATE[self.tid].get().fetch_or(1 << self.idx, Release);

            }
        }


    }
}