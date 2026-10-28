#![allow(private_bounds)]
#![allow(private_interfaces)]
use crate::builder::SchedulerBuilder;
use crate::config::{Config, MAX_SUB_SCHEDULERS, MAX_WORKERS_PER_SCHED};
use crate::idx_cache::{FIDCache, IdxCache, SchedCache};
use crate::task::Task;
use crate::worker::Worker;
use core::arch::asm;
use core::cell::OnceCell;
use core::mem::transmute_copy;
use core::mem::MaybeUninit;
use core::ptr;
use core::sync::atomic::Ordering::Acquire;
use core::sync::atomic::Ordering::Release;
use core::sync::atomic::{AtomicBool, AtomicPtr, AtomicU64, Ordering};
use std::hint::spin_loop;
use std::marker::PhantomData;
use std::sync::{Arc, Condvar, Mutex};
use std::thread::JoinHandle;
use std::time::{Duration, Instant};



#[macro_export]
macro_rules! create_task {
    ($task:ident($($arg:expr),*)) => {
        Scheduler::create_task(|| $task($($arg),*))
    };
}

#[macro_export]
macro_rules! create_task_with_callback {
    ($task:ident($($arg:expr),*), $callback:ident($($callback_arg:expr),*)) => {
        Scheduler::create_task_with_callback(|| {
            let mut exec = $task($($arg),*);
            exec();
            $callback($($callback_arg),*)
        })
    };
}

//TODO Really just use the custom callback? idk
//i dont see the gain in haveing some specialized thing other than maybe some extra performance
#[repr(u8)]
pub enum Callback{
    //the reason there is no condvar is that the scheduler is single threaded... so there not really a reason to halt it unless you want to
    //fuck yourself over?
    //like idk tell me a good reason for it
    NotifyWhenDone(/*where to notify*/&'static AtomicBool) = 1 << 0,
    CompletionCounter(&'static AtomicU64) = 1 << 2,

}

#[derive(Debug)]
#[repr(transparent)]
#[allow(private_bounds)]
pub struct TaskWrapper<Callee: IdxCache, const N: usize, F: FnOnce()>((F, bool, PhantomData<Callee>));
#[allow(private_bounds)]
impl<Callee: IdxCache, const N: usize, F: FnOnce()> TaskWrapper<Callee,N, F> {
    fn into_inner(self) -> F {
        self.0.0
    }
}

//The task every worker gets
pub static mut MAIL_BOX: [Task; 64] = [const { Task::empty() }; 64];

pub static mut TASK_SLOTS: [u8; 4096] = [0; 4096];

pub static mut OCCUPIED_BYTES: usize = 0;

pub static mut WORKER_STATE: [[Padded<AtomicU64>; 64]; 8]
= [const {[ const { Padded(AtomicU64::new(0)) }; MAX_WORKERS_PER_SCHED] }; 8];

//due to how FID works duplicates are impossible meaning 128 unique Tasks can be stored in a program
pub static mut TASK_POSITION_LOOKUP: [OnceCell<usize>; 128] = [const {OnceCell::new()}; 128];


#[repr(align(64))]
#[derive(Clone, Copy, Debug)]
///64 byte aligned T
pub struct Padded<T>(pub(crate) T);
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
pub(crate) enum LockVariant{
    Weak,
    Strong,
    None
}


pub(crate) enum WorkerError<Callee: IdxCache, const N: usize, F: FnOnce()>{
    Busy(TaskWrapper<Callee, N, F>, LockVariant),
    Misc,
}

pub struct Scheduler<const N: usize> {
    pub(crate) generic_schedulers: [Padded<*mut SubScheduler>; MAX_SUB_SCHEDULERS],
    pub(crate) worker_state_copy: [u64; MAX_WORKERS_PER_SCHED],
}
impl<const N: usize> Scheduler<N> {
    pub fn new(config: Config) -> SchedulerBuilder {
        unsafe {<Scheduler<N> as SchedCache>::empty::<Self>().get_sid();}

        SchedulerBuilder{
            config,
            incomplete: Scheduler {
                generic_schedulers: [Padded(ptr::null_mut()); MAX_SUB_SCHEDULERS],
                worker_state_copy: [0; MAX_WORKERS_PER_SCHED],
            },
            registrations: 0,
            total_workers: 0,
        }
    }

    pub fn get_sub_scheduler<T: IdxCache>(&self) -> *mut SubScheduler {
        return self.generic_schedulers[unsafe {T::empty::<T>().get_tid()}].0
    }
    ///it might happen that the last N % default_workers tasks are not delivered with the lock_less approach
    ///it's better for batches of size, well, N % default_worker
    ///has the lowest update guarantee and singular calls might not even be executed at all

    pub fn any_task_lockless<F, T>(&mut self, exec: TaskWrapper<T, N, F>) -> Result<(), WorkerError<T, N, F>>
    where F: FIDCache + FnOnce(),
          T: IdxCache,
    {
        let tid = unsafe {T::empty::<T>().get_tid()};

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
            let completed = unsafe {(*&raw mut WORKER_STATE[self.get_sid()])[tid].get().load(Acquire)};
            if completed.count_ones() as usize == sub_scheduler.workers{
                self.worker_state_copy[tid] = completed;
                //TODO unless i change this code the store is fine.. i hope i dont forget
                unsafe {(*&raw mut WORKER_STATE[self.get_sid()])[tid].get().store(0, Release)};
            }
            return Err(WorkerError::Busy(exec, LockVariant::None));
        }
        self.worker_state_copy[tid] &= !(1 << available_idx);

        unsafe {self.mail_task(exec, sub_scheduler, available_idx)}

        return Ok(())
    }


    ///No artificial buffering, immediate results but also the slowest and most prone to contention
    pub fn any_task_locking<F, T>(&mut self, exec: TaskWrapper<T, N, F>) -> Result<(), WorkerError<T, N, F>>
    where F: FIDCache + FnOnce(),
          T: IdxCache,
    {
        let tid = unsafe {T::empty::<T>().get_tid()};
        let sub_scheduler = unsafe { &*self.generic_schedulers[tid].0};

        let available_idx: usize;


        let available_workers = unsafe {(*&raw mut WORKER_STATE[self.get_sid()])[tid].get().load(Acquire)};
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
        unsafe {(*&raw mut WORKER_STATE[self.get_sid()])[tid].get().fetch_and(!(1 << available_idx), Release) };

        unsafe {self.mail_task(exec, sub_scheduler, available_idx)}


        return Ok(())
    }

    unsafe fn mail_task<T: IdxCache, F: FnOnce()>(&mut self, exec: TaskWrapper<T, N, F>, sub_scheduler: &SubScheduler, available_idx: usize){
        let replace = exec.0.1;
        let fid = exec.get_fid();
        //Well get or init the size of a function if it hasn't been seen before
        let is_init = unsafe {TASK_POSITION_LOOKUP[fid].get().is_some()};

        let task_offset = *unsafe {TASK_POSITION_LOOKUP[fid].get_or_init(|| {
            *&raw mut OCCUPIED_BYTES
        })};


        if replace{
            unsafe {
                if *&raw mut OCCUPIED_BYTES + size_of::<F>() <= 4096{
                    let dest = (&raw mut TASK_SLOTS[task_offset]) as *mut F;
                    dest.write_unaligned(exec.into_inner());
                    OCCUPIED_BYTES += size_of::<F>() * (!is_init) as usize;
                }else {
                    panic!("You dont have enough space to store more unique tasks, fid {fid}")
                }
            }
        }

        //println!("task_offset: {:?}", task_offset);
        //println!("occupied: {:?}", *&raw const OCCUPIED_BYTES);

        let offset = sub_scheduler.offset;
        let slot: *mut Task = unsafe { &raw mut MAIL_BOX[offset + available_idx]};

        let raw = unsafe {
            (&raw mut TASK_SLOTS[task_offset]) as *mut F
        };


        unsafe {slot.write_volatile(Task::new(raw))};

    }

    ///block until a worker received the task and NOT until the task has finished executing
    pub fn block_until_arrival<F: FnOnce(), For: IdxCache>(&mut self, busy: Result<(), WorkerError<For, N, F>>){
        //TODO instead of trying to send i could just check the bitmask, if it didnt change then there is no point in trying
        match busy {
            Ok(()) => {},
            Err(WorkerError::Busy(task, variant)) => {
                let mut temp = task;
                //if its busy and hasn't changed its logically still busy and no point in checking
                while self.worker_state_copy[unsafe {For::empty::<For>().get_tid()}] == u64::MAX{
                    spin_loop()
                }
                match variant {
                    //this is still in a loop but should fall through immediately
                    LockVariant::None => while let Err(WorkerError::Busy(t, _)) = self.any_task_lockless::<_, For>(temp) { temp = t; }
                    LockVariant::Strong => while let Err(WorkerError::Busy(t, _)) = self.any_task_locking::<_, For>(temp) { temp = t; },
                    _ => unreachable!(),
                }
            }
            Err(WorkerError::Misc) => {}
        }
    }

    ///Its your responsibility to make sure the predicate lives long enough and has a state that ends the while loop
    pub fn block_until<F: FnOnce(), For: IdxCache, P: Fn() -> bool>(&mut self, busy: Result<(), WorkerError<For, N, F>>, predicate: P){
        //first wait for the arrival of the task
        self.block_until_arrival(busy);
        while !predicate() {
            spin_loop()
        }
    }
    //real tongue twister huh
    pub fn block_until_timeout<F: FnOnce(), For: IdxCache, P: Fn() -> bool>(&mut self, busy: Result<(), WorkerError<For, N, F>>, predicate: P, timeout: Duration){
        //first wait for the arrival of the task
        self.block_until_arrival(busy);
        let now = Instant::now();
        while !predicate() && now.elapsed() < timeout {
            spin_loop()
        }
    }
    #[inline]
    ///this just exists for if you want to be explicit you should generally use the macro
    pub fn create_task<Callee: IdxCache, T: FnOnce() -> F, F>(task: T) -> TaskWrapper<Callee, N, F> where F: FnOnce() {
        TaskWrapper((task(), true, PhantomData))
    }
    #[inline]
    pub fn create_task_with_callback<Callee: IdxCache, T, F>(task: T)
        -> TaskWrapper<Callee, N, F>
        where F: FnOnce(),
              T: FnOnce() -> F
    {

        TaskWrapper((task(), true, PhantomData))
    }

    //partially ignores borrowing rules
    ///you should generally just go through the create_task macro or function
    pub unsafe fn create_task_unchecked<Callee: IdxCache, F: FnMut()>(task: F, replace: bool) -> TaskWrapper<Callee, N, F> {
        return transmute_copy(&(task, replace))
    }
}

#[repr(C)]
#[derive(Debug)]
pub struct SubScheduler{
    worker_pausing: Vec<Arc<(Mutex<bool>, Condvar)>>,
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

    pub(crate) fn new<T: IdxCache>(parent: &Scheduler<0>, offset: usize, workers: usize) -> *mut Self {
        let tid = unsafe {T::empty::<T>().get_tid()};


        assert!(tid < MAX_SUB_SCHEDULERS, "TID greater than MAX_SUB_SCHEDULERS");
        assert!(offset + workers <= unsafe {(*&raw mut MAIL_BOX).len()}, "not enough global task slots for this sub_scheduler");



        //init the states for a given sub_scheduler
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
            core::ptr::write(&raw mut (*incomplete_schedulers).worker_pausing, Vec::new());

            for w in 0..workers {
                let global_slot = offset + w;
                let cond = Arc::new((Mutex::new(false), Condvar::new()));
                (*incomplete_schedulers).worker_pausing.push(cond);
                let worker = Worker::new(
                                         (&*incomplete_schedulers).worker_pausing[w].clone(),
                                         parent.get_sid(),
                                         w,
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
impl<const N: usize> Drop for Scheduler<N> {
    fn drop(&mut self) {
        //You cant iterate only the registered items anymore due to changes, you have to iter all schedulers
        for sh in self.generic_schedulers[0..].into_iter(){
            unsafe {
                if !sh.0.is_null(){
                    sh.0.drop_in_place()
                }
            };
        }
    }
}
