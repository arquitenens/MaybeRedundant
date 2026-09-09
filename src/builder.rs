use core::any::type_name;
use core::mem::MaybeUninit;
use core::ptr;
use core::sync::atomic::{AtomicUsize, Ordering};
use crate::config::{Config, ThreadAmount};
use crate::scheduler::{Scheduler, SubScheduler, TASK_SLOTS};
use crate::task::Task;

pub(crate) trait TypesIdx {
    ///Cant be called outside of this crate so its fine
    fn get_or_register_tid() -> usize {
        static COUNTER: AtomicUsize = AtomicUsize::new(0);
        generic_static_cache::generic_static! {
            static ID: &usize = &COUNTER.fetch_add(1, Ordering::Relaxed);
        }
        *ID
    }
}

impl<T> TypesIdx for T {}

pub(crate) trait FID {
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
    //not yet completed scheduler
    pub(crate) incomplete: Scheduler,
    //Since every registration increments this it means a type that's not implemented it
    //will give an index greater than this registration
    pub(crate) registrations: usize,

    pub(crate) total_workers: usize
}
impl SchedulerBuilder {
    #[inline(never)]
    pub fn add_scheduler<T: TypesIdx>(mut self, thread_overwrite: ThreadAmount) -> Self{
        self.registrations += 1;

        #[cfg(debug_assertions)]
        println!("added scheduler Name: {}, Idx: {}", type_name::<T>(), T::get_or_register_tid());

        let workers = match thread_overwrite {
            ThreadAmount::Default => self.config.threads_per_sub_sched,
            ThreadAmount::Overwrite(n) => n,
        };

        let offset = self.total_workers;
        let sh = SubScheduler::new::<T>(offset, workers);
        self.total_workers += workers;

        #[cfg(debug_assertions)]
        println!("sh: {:p}", sh);


        unsafe {self.incomplete.generic_schedulers[T::get_or_register_tid()] = sh};
        return self
    }

    //TODO this function is bound to be replaced with an attribute macro and is not there to stay
    pub fn register_task<F: FID + FnMut()>(self, exec: F) -> Self{
        let raw_task: *mut F = ptr::from_ref(&exec) as *mut _;
        let task = Task::new(raw_task);
        unsafe {TASK_SLOTS[exec.get_or_register_fid()].replace(task)};
        //println!("TASK_SLOTS {:?}", unsafe {&*&raw mut TASK_SLOTS});
        return self
    }

    pub fn apply(self) -> Scheduler{
        self.incomplete
    }
}