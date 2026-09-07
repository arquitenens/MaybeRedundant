use std::ptr::NonNull;
use std::sync::atomic::{AtomicBool, AtomicPtr, AtomicU64, Ordering};
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
                
            }
        }


    }
}