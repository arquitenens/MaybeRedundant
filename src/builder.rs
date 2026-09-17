use crate::config::{Config, ThreadAmount};
use crate::idx_cache::IdxCache;
use crate::scheduler::{Padded, Scheduler, SubScheduler};
use core::any::type_name;


pub struct SchedulerBuilder{
    pub(crate) config: Config,
    //not yet completed scheduler
    pub(crate) incomplete: Scheduler,

    pub(crate) registrations: usize,

    pub(crate) total_workers: usize
}
impl SchedulerBuilder {
    #[inline(never)]
    pub fn add_scheduler<T: IdxCache>(mut self, thread_overwrite: ThreadAmount) -> Self{
        self.registrations += 1;

        #[cfg(debug_assertions)]
        println!("added scheduler Name: {}, Idx: {}", type_name::<T>(), unsafe {T::empty::<T>().get_tid()});

        let workers = match thread_overwrite {
            ThreadAmount::Default => self.config.threads_per_sub_sched,
            ThreadAmount::Overwrite(n) => n,
        };

        let offset = self.total_workers;
        let sh = SubScheduler::new::<T>(offset, workers);
        self.total_workers += workers;

        #[cfg(debug_assertions)]
        println!("sh: {:p}", sh);

        self.incomplete.worker_state_copy[unsafe {T::empty::<T>().get_tid()}] = (1 << workers) - 1;
        unsafe {self.incomplete.generic_schedulers[T::empty::<T>().get_tid()] = Padded(sh)};
        return self
    }
    

    pub fn apply(self) -> Scheduler{
        self.incomplete
    }
}