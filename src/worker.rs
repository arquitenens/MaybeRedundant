use std::ptr::NonNull;
use std::sync::atomic::{AtomicBool, AtomicPtr, AtomicU64, Ordering};
use std::sync::atomic::Ordering::{Acquire, Release};
use crate::scheduler::WORKER_STATE;
use crate::task::Task;


struct UnsafePtr<T>(*mut T);
unsafe impl<T> Send for UnsafePtr<T> {}

pub(crate) struct Worker{
    pub(crate) idx: usize,

    pub(crate) offset: usize,


    pub(crate) signal: &'static AtomicBool,

    //the worker doesn't care if the task came from cache or from the queue
    //does not need to be atomically loaded/swapped technically
    pub(crate) current_task: AtomicPtr<Task>
}


impl Worker {
    pub(crate) fn new(idx: usize, offset: usize, signal: &'static AtomicBool, current_task: AtomicPtr<Task>) -> Self {
        Self{
            idx,
            offset,
            signal,
            current_task,
        }
    }
    pub(crate) fn run(self) {
        unsafe {
            loop {
                if self.signal.load(Ordering::Acquire) {
                    dbg!("Dropping Worker {}", self.idx);
                    let t = self.current_task.load(Ordering::Acquire).replace(Task::const_default());
                    (t.dropper)(t.data);
                    break;
                }

                let current_task = self.current_task.load(Ordering::Acquire);

                if current_task.is_null(){
                    continue;
                }
                //((*current_task).callable)((*current_task).data);

                let ptr = WORKER_STATE[self.idx].get_imutable().as_ptr();
                ptr.write_volatile(*ptr | 1 << (self.idx * self.offset))
                //WORKER_STATE[self.idx].get_imutable().fetch_or(1 << self.idx, Release);


            }
        }


    }
}