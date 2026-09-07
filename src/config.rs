pub struct Config{
    pub(crate) max_threads: usize,
    pub(crate) threads_per_sub_sched: usize,
}
impl Config{
    pub fn new(max_threads: usize, threads_per_sub_sched: usize) -> Self{
        assert!(max_threads > 0, "max_threads must be > 0");
        assert!(threads_per_sub_sched > 0, "threads_per_worker must be > 0");
        assert!(max_threads >= threads_per_sub_sched, "cant have more threads than max");
        assert!(threads_per_sub_sched <= 64, "threads_per_worker must be < 64");
        Config{max_threads, threads_per_sub_sched }
    }
}

impl Default for Config{
    fn default() -> Self{
        Config{max_threads: 8, threads_per_sub_sched: 4}
    }
}