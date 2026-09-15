use crate::builder::{SchedulerBuilder};
use crate::config::{Config, MAX_SUB_SCHEDULERS, MAX_WORKERS_PER_SCHED};
use crate::task::Task;
use crate::worker::Worker;
use core::hint::black_box;
use core::mem::{ManuallyDrop, MaybeUninit};
use core::{ptr};
use core::arch::asm;
use std::ptr::null_mut;
use core::sync::atomic::Ordering::Release;
use core::sync::atomic::{AtomicBool, AtomicPtr, AtomicU64, Ordering};
use std::mem::{transmute, transmute_copy};
use std::sync::atomic::AtomicUsize;
use std::thread::{sleep, JoinHandle};
use crate::builder;
use core::cell::RefCell;
use std::sync::atomic::Ordering::Acquire;
use std::time::Duration;
use crate::idx_cache::{FIDCache, IdxCache};



#[macro_export]
macro_rules! create_task {
    ($task:ident($($arg:expr),*)) => {
        Scheduler::create_task(|| $task($($arg),*))
    };
}

#[derive(Debug)]
#[repr(transparent)]
pub struct TaskWrapper<F: FnMut()>(F);
impl<F: FnMut()> TaskWrapper<F> {
    fn into_inner(self) -> F {

        self.0
    }
}

//global task slots
//due to how FID works duplicates are impossible meaning 128 unique Tasks can be stored in a program

pub static mut TASK_SLOTS: [Task; 128] = [const { Task::empty() }; 128];

//Each Worker gets their own reference to this
//READ ONLY FROM SCHEDULER
pub(crate) static WORKER_STATE: [Padded<AtomicU64>; MAX_SUB_SCHEDULERS] = [const { Padded(AtomicU64::new(0)) }; MAX_SUB_SCHEDULERS];

#[repr(align(64))]
#[derive(Clone, Copy, Debug)]
///64 byte aligned T
pub(crate) struct Padded<T>(pub(crate) T);
impl<T> Padded<T> {
    #[inline(always)]
    fn get_mut(&mut self) -> &mut T {
        &mut self.0
    }
    #[inline(always)]
    pub(crate) fn get(&self) -> &T {
        &self.0
    }
}

#[derive(Debug)]
pub(crate) enum WorkerError<F: FnMut()>{
    Busy(TaskWrapper<F>),
    Misc,
}

pub(crate) struct Scheduler {
    pub(crate) generic_schedulers: [Padded<*mut SubScheduler>; MAX_SUB_SCHEDULERS],
    worker_state_copy: [u64; MAX_WORKERS_PER_SCHED],
    iterations: u64,
}

pub(crate) static WORKER_AVERAGE: AtomicUsize = AtomicUsize::new(0);
pub(crate) static DIRTY_ITER: AtomicU64 = AtomicU64::new(0);
impl Scheduler {
    pub(crate) fn new(config: Config) -> SchedulerBuilder {
        SchedulerBuilder{
            config,
            incomplete: Scheduler {
                generic_schedulers: [Padded(ptr::null_mut()); MAX_SUB_SCHEDULERS],
                worker_state_copy: [0; MAX_WORKERS_PER_SCHED],
                iterations: 0,
            },
            registrations: 0,
            total_workers: 0,
        }
    }

    #[inline]
    pub fn any_task<F, T>(&mut self, exec: TaskWrapper<F>) -> Result<(), WorkerError<F>>
    where F: FIDCache + FnMut(),
          T: IdxCache,
    {

        let tid = T::empty::<T>().get_tid();


        if tid >= MAX_SUB_SCHEDULERS {
            core::hint::cold_path();
            panic!("You tried to input types that have not been registered yet into either the register or this function")
        }

        //synchronize copies
        let available_workers = self.worker_state_copy[tid];

        let available_idx = available_workers.trailing_zeros() as usize;

        //sleep(Duration::from_nanos(80));
        if available_idx == 0 {
            self.worker_state_copy[tid] = WORKER_STATE[tid].get().load(Acquire);

            WORKER_STATE[tid].get().store(0, Release);
            return Err(WorkerError::Busy(exec));
        }
        self.worker_state_copy[tid] &= !(1 << available_idx);
        let offset = unsafe { (*self.generic_schedulers[tid].0).offset };
        //the reference should be fine since the function providing the closure is global and "static"
        let raw = ptr::from_ref(&(exec)) as *mut F;
        let slot: *mut Task = unsafe { &raw mut TASK_SLOTS[offset + available_idx]};
        unsafe {slot.write_volatile(Task::new(raw))};


        return Ok(())
    }

    ///block until a worker received the task and NOT until the task has finished executing
    pub fn block_until_arrival<F: FnMut(), For: IdxCache>(&mut self, busy: Result<(), WorkerError<F>>){
        match busy {
            Ok(()) => {},
            Err(WorkerError::Busy(task)) => {
                let mut temp = task;
                while let Err(WorkerError::Busy(t)) = self.any_task::<_, For>(temp) {
                    temp = t;
                }
            }
            Err(WorkerError::Misc) => {}
        }
    }
    #[inline]
    ///this just exists for if you want to be explicit you should generally use the macro
    pub fn create_task<T: 'static + FnOnce() -> F, F>(task: T) -> TaskWrapper<F> where F: FnMut() {
        TaskWrapper(task())
    }

    //partially ignores borrowing rules
    ///you should generally just go through the create_task macro of function
    pub unsafe fn unchecked_task_wrapper<F: FnMut()>(task: F) -> TaskWrapper<F> {
        return transmute_copy(&task)
    }
}

#[repr(align(64))]
//dont reorder evil compiler grrr
#[repr(C)]
//128bytes
#[derive(Debug)]
pub(crate) struct SubScheduler{
    //amount of workers
    workers: usize,
    //offset within the global "WORKER_STATE" mask
    offset: usize,

    //its rarely used, the pointer indirection shouldn't matter
    //TODO but also there is not really a point in having it be on the heap?
    handles: Box<[Option<JoinHandle<()>>; MAX_WORKERS_PER_SCHED]>,

    heartbeat_test: [Padded<AtomicBool>; MAX_WORKERS_PER_SCHED],
    //every worker has their own UNIQUE index into it and terminates once it's set to true
    //since its mostly only reads they can be in the same cache-line
    worker_terminate: Padded<[AtomicBool; MAX_WORKERS_PER_SCHED]>,

}

static mut SUB_SCHEDULERS:
[MaybeUninit<SubScheduler>; MAX_SUB_SCHEDULERS] =
    [const { MaybeUninit::uninit() }; MAX_SUB_SCHEDULERS];
impl SubScheduler {

    pub(crate) fn new<T: IdxCache>(offset: usize, workers: usize) -> *mut Self {
        let tid = T::empty::<T>().get_tid();


        assert!(tid < MAX_SUB_SCHEDULERS, "TID greater than MAX_SUB_SCHEDULERS");
        assert!(offset + workers <= unsafe {(*&raw mut TASK_SLOTS).len()}, "not enough global task slots for this sub_scheduler");


        //set uo the masks
        let mut mask = 0u64;
        mask |= (1 << workers) - 1;

        //println!("mask {:064b}", mask);

        //init the states for a given sub_scheduler
        WORKER_STATE[tid].get().store(mask, Release);
        let handles: Box<[Option<JoinHandle<()>>; MAX_WORKERS_PER_SCHED]> = Box::new([const { None }; MAX_WORKERS_PER_SCHED]);
        let heartbeats = [const { Padded(AtomicBool::new(false)) }; MAX_WORKERS_PER_SCHED];
        let terminate = Padded([const { AtomicBool::new(false) }; MAX_WORKERS_PER_SCHED]);

        //This should be sound since im not creating any mutable references, hence "raw mut"
        let incomplete_schedulers: *mut SubScheduler = unsafe {(&raw mut SUB_SCHEDULERS[tid]).cast::<SubScheduler>()};

        unsafe {
            core::ptr::write_volatile(&raw mut (*incomplete_schedulers).workers, workers);
            //println!("workers {}", (*incomplete_schedulers).workers);
            core::ptr::write_volatile(&raw mut (*incomplete_schedulers).offset, offset);
            //println!("offset {}", (*incomplete_schedulers).offset);
            core::ptr::write_volatile(&raw mut (*incomplete_schedulers).worker_terminate, terminate);
            core::ptr::write_volatile(&raw mut (*incomplete_schedulers).handles, handles);
            core::ptr::write_volatile(&raw mut (*incomplete_schedulers).heartbeat_test, heartbeats);

            for w in 0..workers {
                let global_slot = offset + w;
                let worker = Worker::new(w,
                                         tid,
                                         (*incomplete_schedulers).heartbeat_test[global_slot].get(),
                                         &(*incomplete_schedulers).worker_terminate.0[w],
                                         AtomicPtr::new(&raw mut TASK_SLOTS[global_slot] as *mut Task)
                );
                let h = std::thread::spawn(move || {
                    worker.run()
                });
                (*incomplete_schedulers).handles[w].replace(h);
            }
            return incomplete_schedulers.cast::<SubScheduler>();
        }
    }
}


impl Drop for SubScheduler {
    //this might drop a non-finished task, it's on you to make sure the scheduler lives long enough
    //for every task to complete if you care
    fn drop(&mut self) {
            #[cfg(debug_assertions)]
            eprintln!("sub-scheduler = {:p} has been dropped", self);
            for (idx, h) in self.handles[0..self.workers].iter_mut().enumerate(){
                self.worker_terminate.get()[idx].store(true, Ordering::Release);
                let _ = h.take().unwrap().join();
        }
    }
}


//Shouldn't be accessible and stay private
struct DropRange;
impl Drop for Scheduler {
    fn drop(&mut self) {
        //in order to only drop registered scheduler and save a bit of performance
        //you can just register a new item which will then have the biggest index, thus tell you how many items are registered
        let x = DropRange.get_tid();
        for sh in self.generic_schedulers[0..DropRange.get_tid()].into_iter(){
            unsafe {sh.0.drop_in_place()};
        }
    }
}
