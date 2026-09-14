//CONSTS
pub const MAX_WORKERS_PER_SCHED: usize = 64;

pub const MAX_SUB_SCHEDULERS: usize = 64;


//TODO custom thread amounts
pub(crate) enum ThreadAmount{
    Default,
    Overwrite(usize),
}

pub struct Config{
    pub(crate) threads_per_sub_sched: usize,
}
impl Config{
    ///Be careful, if you set the workers to 16, but you only have 16 threads then the 17th thread
    ///will be an OS thread and massively impact your performance (up to 5x ive measured)
    pub fn new(threads_per_sub_sched: usize) -> Self{
        assert!(threads_per_sub_sched > 0, "threads_per_worker must be > 0");
        assert!(threads_per_sub_sched <= 64, "threads_per_worker must be < 64");
        Config{threads_per_sub_sched }
    }
}

impl Default for Config{
    fn default() -> Self{
        Config{threads_per_sub_sched: 7}
    }
}