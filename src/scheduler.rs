use crate::builder::SchedulerBuilder;
use crate::config::{Config, MAX_SUB_SCHEDULERS, MAX_WORKERS_PER_SCHED};
use crate::idx_cache::{FIDCache, IdxCache};
use crate::task::Task;
use crate::worker::Worker;
use core::arch::asm;
use core::cell::OnceCell;
use core::mem::transmute_copy;
use core::mem::MaybeUninit;
use core::sync::atomic::Ordering::Acquire;
use core::sync::atomic::Ordering::Release;
use core::sync::atomic::{AtomicBool, AtomicPtr, AtomicU64, Ordering};
use core::ptr;
use std::thread::JoinHandle;



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

//The task every worker gets
pub static mut MAIL_BOX: [Task; 64] = [const { Task::empty() }; 64];

pub static mut TASK_SLOTS: [u8; 4096] = [0; 4096];

pub static mut OCCUPIED_BYTES: usize = 0;

static STALE_ATOMIC: [Padded<AtomicU64>; MAX_SUB_SCHEDULERS] = [const {Padded(AtomicU64::new(0))}; MAX_SUB_SCHEDULERS];

//due to how FID works duplicates are impossible meaning 128 unique Tasks can be stored in a program
pub static mut TASK_POSITION_LOOKUP: [OnceCell<usize>; 128] = [const {OnceCell::new()}; 128];

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
enum LockVariant{
    Weak,
    Strong,
    None
}

pub static IS_DIFFERENT: AtomicU64 = AtomicU64::new(0);

#[derive(Debug)]
pub(crate) enum WorkerError<F: FnMut()>{
    Busy(TaskWrapper<F>, LockVariant),
    Misc,
}

pub(crate) struct Scheduler {
    pub(crate) generic_schedulers: [Padded<*mut SubScheduler>; MAX_SUB_SCHEDULERS],
    pub(crate) worker_state_copy: [u64; MAX_WORKERS_PER_SCHED],
    iterations: u64,
}
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
    ///it might happen that the last N % default_workers tasks are not delivered with the lock_less approach
    ///it's better for batches of size, well, N % default_worker
    ///has the lowest update guarantee and singular calls might not even be executed at all
    pub fn any_task_lockless<F, T>(&mut self, exec: TaskWrapper<F>) -> Result<(), WorkerError<F>>
    where F: FIDCache + FnMut(),
          T: IdxCache,
    {
        let tid = unsafe {T::empty::<T>().get_tid()};

        if tid >= MAX_SUB_SCHEDULERS {
            core::hint::cold_path();
            panic!("You tried to input types that have not been registered yet into either the register or this function")
        }
        let sub_scheduler = unsafe { &*self.generic_schedulers[tid].0};

        let available_idx: usize;

        let available_workers = self.worker_state_copy[tid];
        //I don't know if the false dependency on tzcnt is still a thing
        //I have also seen it being a problem on popcnt but that seems to be resolved (?)
        unsafe {
            asm!(
            "xor {dst}, {dst}",
            "tzcnt {dst}, {src}",
            dst = out(reg) available_idx,
            src = in(reg) available_workers,
            )
        }

        if available_idx == 64 {
            let completed = WORKER_STATE[tid].get().load(Acquire);
            if completed.count_ones() as usize == sub_scheduler.workers{
                self.worker_state_copy[tid] = completed;
                //TODO unless i change this code the store is fine.. i hope i dont forget
                WORKER_STATE[tid].get().store(0, Release);
            }
            return Err(WorkerError::Busy(exec, LockVariant::None));
        }
        self.worker_state_copy[tid] &= !(1 << available_idx);

        unsafe {self.mail_task(exec, sub_scheduler, available_idx)}

        return Ok(())
    }


    ///still has locking instructions, but they are reduced at the cost of "immediate" updates
    ///as its first being checked if the mask even changed in the first place
    ///with a steady stream of tasks everything should be resolved
    ///TODO its broken dont use it
    pub fn any_task_weak_locking<F, T>(&mut self, exec: TaskWrapper<F>) -> Result<(), WorkerError<F>>
    where F: FIDCache + FnMut(),
          T: IdxCache,
    {
        let tid = unsafe {T::empty::<T>().get_tid()};

        if tid >= MAX_SUB_SCHEDULERS {
            core::hint::cold_path();
            panic!("You tried to input types that have not been registered yet into either the register or this function")
        }
        let sub_scheduler = unsafe { &*self.generic_schedulers[tid].0};

        let available_idx: usize;

        let available_workers = WORKER_STATE[tid].get().load(Acquire);

        let is_different = available_workers != self.worker_state_copy[tid];

        unsafe {
            asm!(
            "xor {dst}, {dst}",
            "tzcnt {dst}, {src}",
            dst = out(reg) available_idx,
            src = in(reg) available_workers,
            )
        }
        if available_idx == 64 {
            return Err(WorkerError::Busy(exec, LockVariant::Weak));
        }


        if is_different{
            IS_DIFFERENT.fetch_add(1, Acquire);
            WORKER_STATE[tid].get().fetch_and(!(1 << available_idx), Release);
            self.worker_state_copy[tid] = WORKER_STATE[tid].get().load(Acquire);
        }else {
            self.worker_state_copy[tid] &= !(1 << available_idx);
        }

        unsafe {self.mail_task(exec, sub_scheduler, available_idx)}

        return Ok(())
    }

    ///No artificial buffering, immediate results but also the slowest and most prone to contention
    pub fn any_task_locking<F, T>(&mut self, exec: TaskWrapper<F>) -> Result<(), WorkerError<F>>
    where F: FIDCache + FnMut(),
          T: IdxCache,
    {
        let tid = unsafe {T::empty::<T>().get_tid()};

        if tid >= MAX_SUB_SCHEDULERS {
            core::hint::cold_path();
            panic!("You tried to input types that have not been registered yet into either the register or this function")
        }
        let sub_scheduler = unsafe { &*self.generic_schedulers[tid].0};

        let available_idx: usize;


        let available_workers = WORKER_STATE[tid].get().load(Acquire);
        unsafe {
            asm!(
            "xor {dst}, {dst}",
            "tzcnt {dst}, {src}",
            dst = out(reg) available_idx,
            src = in(reg) available_workers,
            )
        }
        if available_idx == 64 {
            return Err(WorkerError::Busy(exec, LockVariant::Strong));
        }
        WORKER_STATE[tid].get().fetch_and(!(1 << available_idx), Release);

        unsafe {self.mail_task(exec, sub_scheduler, available_idx)}

        return Ok(())
    }

    unsafe fn mail_task<F: FnMut()>(&mut self, exec: TaskWrapper<F>, sub_scheduler: &SubScheduler, available_idx: usize){
        let fid = exec.get_fid();
        //Well get or init the size of a function if it hasn't been seen before
        let is_init = unsafe {TASK_POSITION_LOOKUP[fid].get().is_some()};

        let task_offset = *unsafe {TASK_POSITION_LOOKUP[fid].get_or_init(|| {
            *&raw mut OCCUPIED_BYTES
        })};


        if !is_init{
            unsafe {
                if *&raw mut OCCUPIED_BYTES + size_of::<F>() <= 4096{
                    let dest = (&raw mut TASK_SLOTS[OCCUPIED_BYTES]) as *mut F;
                    dest.write(exec.into_inner());
                    OCCUPIED_BYTES += size_of::<F>();
                }else {
                    panic!("You dont have enough space to store more unique tasks, fid {fid}")
                }
            }
        }

        let offset = sub_scheduler.offset;
        let slot: *mut Task = unsafe { &raw mut MAIL_BOX[offset + available_idx]};

        let raw = unsafe {
            (&raw mut TASK_SLOTS[task_offset]) as *mut F
        };

        unsafe {slot.write_volatile(Task::new(raw))};
    }

    ///block until a worker received the task and NOT until the task has finished executing
    pub fn block_until_arrival<F: FnMut(), For: IdxCache>(&mut self, busy: Result<(), WorkerError<F>>){
        match busy {
            Ok(()) => {},
            Err(WorkerError::Busy(task, variant)) => {
                let mut temp = task;
                match variant {
                    LockVariant::None => while let Err(WorkerError::Busy(t, _)) = self.any_task_lockless::<_, For>(temp) { temp = t; }
                    LockVariant::Weak => while let Err(WorkerError::Busy(t, _)) = self.any_task_weak_locking::<_, For>(temp) { temp = t; }
                    LockVariant::Strong => while let Err(WorkerError::Busy(t, _)) = self.any_task_locking::<_, For>(temp) { temp = t; }
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
    ///you should generally just go through the create_task macro or function
    pub unsafe fn create_task_unchecked<F: FnMut()>(task: F) -> TaskWrapper<F> {
        return transmute_copy(&task)
    }
}

#[repr(C)]
#[derive(Debug)]
pub(crate) struct SubScheduler{
    //amount of workers
    workers: usize,
    //offset within the global "WORKER_STATE" mask
    offset: usize,


    handles: [Option<JoinHandle<()>>; MAX_WORKERS_PER_SCHED],

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
        let tid = unsafe {T::empty::<T>().get_tid()};


        assert!(tid < MAX_SUB_SCHEDULERS, "TID greater than MAX_SUB_SCHEDULERS");
        assert!(offset + workers <= unsafe {(*&raw mut MAIL_BOX).len()}, "not enough global task slots for this sub_scheduler");


        //set up the masks
        let mut mask = 0u64;
        mask |= (1 << workers) - 1;

        //println!("mask {:064b}", mask);

        //init the states for a given sub_scheduler
        WORKER_STATE[tid].get().store(mask, Release);
        let handles: [Option<JoinHandle<()>>; MAX_WORKERS_PER_SCHED] = [const { None }; MAX_WORKERS_PER_SCHED];
        let heartbeats = [const { Padded(AtomicBool::new(false)) }; MAX_WORKERS_PER_SCHED];
        let terminate = Padded([const { AtomicBool::new(false) }; MAX_WORKERS_PER_SCHED]);

        //This should be sound since im not creating any mutable references, hence "raw mut"
        let incomplete_schedulers: *mut SubScheduler = unsafe {(&raw mut SUB_SCHEDULERS[tid]).cast::<SubScheduler>()};

        unsafe {
            core::ptr::write(&raw mut (*incomplete_schedulers).workers, workers);
            core::ptr::write(&raw mut (*incomplete_schedulers).offset, offset);
            core::ptr::write(&raw mut (*incomplete_schedulers).worker_terminate, terminate);
            core::ptr::write(&raw mut (*incomplete_schedulers).handles, handles);
            core::ptr::write(&raw mut (*incomplete_schedulers).heartbeat_test, heartbeats);

            for w in 0..workers {
                let global_slot = offset + w;
                let worker = Worker::new(w,
                                         tid,
                                         (*incomplete_schedulers).heartbeat_test[global_slot].get(),
                                         &(*incomplete_schedulers).worker_terminate.0[w],
                                         AtomicPtr::new(&raw mut MAIL_BOX[global_slot] as *mut Task)
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
