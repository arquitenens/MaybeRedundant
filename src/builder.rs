use std::mem::MaybeUninit;
use std::ptr;
use std::sync::atomic::{AtomicUsize, Ordering};
use crate::config::{Config, ThreadAmount};
use crate::scheduler::{Scheduler, SubScheduler, TASK_SLOTS};
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

impl<T> TypesIdx for T {}

pub trait FID {
    fn get_or_register_fid(&self) -> usize {
        static COUNTER: AtomicUsize = AtomicUsize::new(0);
        generic_static_cache::generic_static! {
            static ID: &usize = &COUNTER.fetch_add(1, Ordering::Relaxed);
        }
        *ID
    }
}

impl<T> FID for T {}

pub struct SchedulerBuilder{
    pub(crate) config: Config,
    pub(crate) incomplete: MaybeUninit<Scheduler>,
    //Since every registration increments this it means a type that's not implemented it
    //will give an index greater than this registration
    pub(crate) registrations: usize,
    pub(crate) total: usize
}
impl SchedulerBuilder {
    pub fn add_scheduler<T: TypesIdx>(mut self, thread_overwrite: ThreadAmount) -> Self{
        self.registrations += 1;

        let mut amount = self.config.threads_per_sub_sched;
        let workers = match thread_overwrite {
            ThreadAmount::Overwrite(x) => {
                amount = x;
                self.config.threads_per_sub_sched + x
            },
            ThreadAmount::Default => self.config.threads_per_sub_sched,
        };

        let offset = self.total;
        self.total += workers;

        
        let naive_offset = 8 * self.registrations;
        
        let sh = SubScheduler::new::<T>(self.registrations, 8, naive_offset);
        unsafe {self.incomplete.assume_init_mut().generic_schedulers[T::get_or_register_tid()] = sh};
        return self
    }
    pub fn register_task<F: FID + FnMut()>(self, exec: F) -> Self{
        let raw_task: *mut F = ptr::from_ref(&exec) as *mut _;
        let task = Task::new(raw_task);
        unsafe {TASK_SLOTS[exec.get_or_register_fid()].replace(task)};
        return self
    }

    pub fn apply(self) -> Scheduler{
        unsafe {self.incomplete.assume_init()}
    }
}