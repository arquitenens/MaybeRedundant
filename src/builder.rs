use core::any::type_name;
use core::mem::MaybeUninit;
use core::ptr;
use core::sync::atomic::{AtomicUsize, Ordering};
use crate::config::{Config, ThreadAmount};
use crate::idx_cache::{FIDCache, IdxCache};
use crate::scheduler::{Padded, Scheduler, SubScheduler, TASK_SLOTS};
use crate::task::Task;



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
    pub fn add_scheduler<T: IdxCache>(mut self, thread_overwrite: ThreadAmount) -> Self{
        self.registrations += 1;

        #[cfg(debug_assertions)]
        println!("added scheduler Name: {}, Idx: {}", type_name::<T>(), T::empty::<T>().get_tid());

        let workers = match thread_overwrite {
            ThreadAmount::Default => self.config.threads_per_sub_sched,
            ThreadAmount::Overwrite(n) => n,
        };

        let offset = self.total_workers;
        let sh = SubScheduler::new::<T>(offset, workers);
        self.total_workers += workers;

        #[cfg(debug_assertions)]
        println!("sh: {:p}", sh);
        
        let x = T::empty::<T>().get_tid();

        unsafe {self.incomplete.generic_schedulers[T::empty::<T>().get_tid()] = Padded(sh)};
        return self
    }

    //TODO this function is bound to be replaced with an attribute macro and is not there to stay
    pub fn register_task<F: FIDCache + FnMut()>(self, exec: &F) -> Self{
        let raw_task: *mut F = exec as *const F as *mut _;
        let task = Task::new(raw_task);
        unsafe {TASK_SLOTS[exec.get_fid()].replace(task)};
        return self
    }

    pub fn apply(self) -> Scheduler{
        self.incomplete
    }
}