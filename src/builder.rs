use crate::config::{Config, ThreadAmount};
use crate::idx_cache::{IdxCache, SchedCache};
use crate::scheduler::{Padded, Scheduler, SubScheduler, WORKER_STATE};
use core::any::type_name;
use std::sync::atomic::{AtomicU64, AtomicUsize};


pub struct SchedulerBuilder{
    pub(crate) config: Config,
    //not yet completed scheduler
    pub(crate) incomplete: Scheduler<0>,

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
        let sh = SubScheduler::new::<T>(&self.incomplete, offset, workers);
        self.total_workers += workers;

        #[cfg(debug_assertions)]
        println!("sh: {:p}", sh);

        self.incomplete.worker_state_copy[unsafe {T::empty::<T>().get_tid()}] = (1 << workers) - 1;
        //Dude im so sorry
        unsafe {*&raw mut WORKER_STATE[self.incomplete.get_sid()][T::empty::<T>().get_tid()] = Padded(AtomicU64::new((1 << workers) - 1))};
        unsafe {self.incomplete.generic_schedulers[T::empty::<T>().get_tid()] = Padded(sh)};
        return self
    }
    

    pub fn apply(self) -> Scheduler<0>{
        self.incomplete
    }
}