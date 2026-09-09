use std::ptr::{null_mut, NonNull};
use std::sync::atomic::{AtomicBool, AtomicPtr, AtomicU64, Ordering};
use std::sync::atomic::Ordering::{Acquire, Release};
use crate::scheduler::WORKER_STATE;
use crate::task::Task;


struct UnsafePtr<T>(*mut T);
unsafe impl<T> Send for UnsafePtr<T> {}

pub(crate) struct Worker{
    pub(crate) idx: usize,

    //signal to stop execution of the worker, usually when the parent is being dropped
    pub(crate) signal: &'static AtomicBool,

    //the worker doesn't care if the task came from cache or from the queue
    //does not need to be atomically loaded/swapped technically
    pub(crate) current_task: AtomicPtr<Task>
}


impl Worker {
    pub(crate) fn new(idx: usize, slot: usize, signal: &'static AtomicBool, current_task: AtomicPtr<Task>) -> Self {
        Self{
            idx,
            signal,
            current_task,
        }
    }
    pub(crate) fn run(self) {
        unsafe {
            //TODO very dirty and unsafe, will cleanup so no reason to document just yet
            loop {
                if self.signal.load(Ordering::Acquire) {
                    //dbg!("Dropping Worker {}", self.idx);
                    let old = self.current_task.load(Ordering::Acquire).replace(Task::empty());
                    if !old.data.is_null() { (old.dropper)(old.data); }

                    //TODO Maybe send acknowledge signal so the worker doesn't drop a tad bit too early
                    self.signal;

                    break;
                }

                let current_task = self.current_task.load(Ordering::Acquire);


                //TODO just a general function of "task.is_empty()"
                 if current_task.is_null(){
                     continue;
                 }
                 if (*current_task).data.is_null(){
                     continue;
                 }

                (*current_task).execute();


                let ptr = WORKER_STATE[self.idx].get().as_ptr();
                current_task.write_volatile(Task::empty());

                ptr.write_volatile(*ptr | 1 << self.idx);
                //WORKER_STATE[self.idx].get().fetch_or(1 << self.idx, Release);


            }
        }


    }
}