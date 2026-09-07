use std::mem::MaybeUninit;
use std::sync::atomic::{AtomicUsize, Ordering};
use crate::config::Config;
use crate::scheduler::Scheduler;
use crate::task::Task;

pub trait TypesIdx {
    fn get_or_register_tid() -> usize {
        static COUNTER: AtomicUsize = AtomicUsize::new(0);
        generic_static_cache::generic_static! {
            static ID: &usize = &COUNTER.fetch_add(1, Ordering::Relaxed);
        }
        *ID
    }
}

pub trait FID {
    fn get_or_register_fid(&self) -> usize {
        static COUNTER: AtomicUsize = AtomicUsize::new(0);
        generic_static_cache::generic_static! {
            static ID: &usize = &COUNTER.fetch_add(1, Ordering::Relaxed);
        }
        *ID
    }
}

pub struct SchedulerBuilder{
    pub(crate) config: Config,
    pub(crate) incomplete: MaybeUninit<Scheduler>,
    //Since every registration increments this it means a type that's not implemented it
    //will give an index greater than this registration
    pub(crate) registrations: usize
}
impl SchedulerBuilder {
    pub fn add_scheduler<T: TypesIdx>(mut self) -> Self{
        self.registrations += 1;
        return self
    }
    pub fn apply(mut self) -> Scheduler{
        unsafe {self.incomplete.assume_init()}
    }
}