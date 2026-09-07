use std::cell::UnsafeCell;
use std::mem::{transmute, MaybeUninit};
use std::ptr;
use std::ptr::{addr_of_mut, null, null_mut};
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicPtr, AtomicU64, Ordering};
use std::sync::atomic::Ordering::{Acquire, Release};
use std::thread::JoinHandle;
use crate::builder::{SchedulerBuilder, TypesIdx, FID};
use crate::config::{Config, MAX_SUB_SCHEDULERS, MAX_WORKERS_PER_SCHED};
use crate::task::Task;
use crate::worker::Worker;




//global task slots
pub static mut TASK_SLOTS: [Option<Task>; 128] = [const { None }; 128];

//Each Worker gets their own reference to this, must be accessed atomically
pub(crate) static WORKER_STATE: [Padded<AtomicU64>; MAX_SUB_SCHEDULERS] = [const { Padded(AtomicU64::new(u64::MAX)) }; MAX_SUB_SCHEDULERS];

#[repr(align(64))]
#[derive(Clone, Copy)]
///64 byte aligned T
pub(crate) struct Padded<T>(pub(crate) T);
impl<T> Padded<T> {
    #[inline(always)]
    fn get(&mut self) -> &mut T {
        &mut self.0
    }
    #[inline(always)]
    pub(crate) fn get_imutable(&self) -> &T {
        &self.0
    }
}

#[derive(Debug)]
pub(crate) enum WorkerError{
    NoWorkers,
}

pub(crate) struct Scheduler {
    pub(crate) generic_schedulers: [*mut SubScheduler; MAX_SUB_SCHEDULERS],
}
impl Scheduler {
    pub(crate) fn new(config: Config) -> SchedulerBuilder {
        SchedulerBuilder{
            config,
            incomplete: MaybeUninit::uninit(),
            registrations: 0,
            total: 0,
        }
    }
    pub(crate) fn any_task<F, T>(&mut self, exec: F, unique_arg: bool) -> Result<(), WorkerError>
    where F: FID + FnMut(),
          T: TypesIdx,
    {
        let tid = T::get_or_register_tid();
        let fid = exec.get_or_register_fid();

        if tid >= MAX_SUB_SCHEDULERS || fid >= MAX_WORKERS_PER_SCHED {
            std::hint::cold_path();
            panic!("You tried to input types that have not been registered yet into either the register or this function")
        }

        //println!("Tid: {:?}", tid);

        let available_workers = WORKER_STATE[tid].get_imutable().load(Ordering::Acquire);

        //println!("available {:064b}", available_workers);

        let available_idx = available_workers.trailing_zeros() as usize;


        //dbg!(available_idx);

        if available_idx == 0 {
            return Err(WorkerError::NoWorkers)
        }
        if available_idx == 64{
            return Err(WorkerError::NoWorkers)
        }

        //SAFETY this is not a fetch_and due to the fact that the Scheduler is single threaded
        //And the dependency should prevent the cpu from reordering
        unsafe {WORKER_STATE[tid].get_imutable().as_ptr().write_volatile(available_workers & (!(1u64 << available_idx)))};
        //WORKER_STATE[tid].get_imutable().fetch_and(!(1u64 << available_idx), Release);

        let slot: *mut Task = unsafe {ptr::from_ref(&TASK_SLOTS[fid]) as *mut _};


        if unique_arg {
            let raw = ptr::from_ref(&exec) as *mut F;
            unsafe {slot.replace(Task::new(raw))};
        }


        return Ok(())
    }
}

#[repr(align(64))]
//dont reorder evil compiler grrr
#[repr(C)]
//128bytes
pub(crate) struct SubScheduler{
    //amount of workers
    workers: usize,
    //offset within the global "WORKER_STATE" mask
    offset: usize,

    //its rarely used, the pointer indirection shouldn't matter
    //TODO but also there is not really a point in having it be on the heap?
    handles: Box<[Option<JoinHandle<()>>; MAX_WORKERS_PER_SCHED]>,

    //every worker has their own UNIQUE index into it and terminates once it's set to true
    //since its mostly only reads they can be in the same cache-line
    worker_terminate: Padded<[AtomicBool; MAX_WORKERS_PER_SCHED]>,

}

static mut SUB_SCHEDULERS:
[MaybeUninit<SubScheduler>; MAX_SUB_SCHEDULERS] =
    [const { MaybeUninit::uninit() }; MAX_SUB_SCHEDULERS];
impl SubScheduler {
    pub(crate) fn new<T: TypesIdx>(registrations: usize, workers: usize, offset: usize) -> *mut Self {

        assert!(T::get_or_register_tid() <= registrations);
        assert!(offset + workers <= 64);

        let mut mask = 0u64;
        mask |= ((1 << 8) - 1) << offset - 8;

        //println!("mask {:064b}", mask);

        WORKER_STATE[T::get_or_register_tid()].get_imutable().store(mask, Release);
        //println!("WORKER_STATE {:?}", WORKER_STATE);

        let handles: Box<[Option<JoinHandle<()>>; MAX_WORKERS_PER_SCHED]> = Box::new([const { None }; MAX_WORKERS_PER_SCHED]);

        let terminate = Padded([const { AtomicBool::new(false) }; MAX_WORKERS_PER_SCHED]);

        //TODO is this sound?
        let incomplete_schedulers: *mut SubScheduler = unsafe {(&raw mut SUB_SCHEDULERS[T::get_or_register_tid()]).cast::<SubScheduler>()};

        unsafe {
            std::ptr::write(&raw mut (*incomplete_schedulers).worker_terminate, terminate);
            std::ptr::write(&raw mut (*incomplete_schedulers).handles, handles);
            std::ptr::write(&raw mut (*incomplete_schedulers).workers, workers);
            std::ptr::write(&raw mut (*incomplete_schedulers).offset, offset);

            for w in 0..workers {
                //let term = &raw mut (*incomplete_schedulers).worker_terminate.get()[w];
                let worker = Worker::new(w, offset,
                                         &(*incomplete_schedulers).worker_terminate.get()[w],
                                         AtomicPtr::new(ptr::from_ref(&TASK_SLOTS[w]) as *mut _)
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
            for (idx, h) in self.handles.iter_mut().enumerate(){
                self.worker_terminate.get()[idx].store(true, Ordering::Release);
            let _ = h.take().unwrap().join();
        }
    }
}
