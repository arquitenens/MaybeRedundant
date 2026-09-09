use crate::builder::{SchedulerBuilder, TypesIdx, FID};
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
use std::thread::JoinHandle;
use crate::builder;

union MaybeUninitTask{
    init: ManuallyDrop<Task>,
    uninit: Padded<()>
}

//global task slots
//due to how FID works duplicates are impossible meaning 128 unique Tasks can be stored in a program
pub static mut TASK_SLOTS: [Option<Task>; 128] = [const { None }; 128];

//Each Worker gets their own reference to this
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
    Busy(F),
    Misc,
}

pub(crate) struct Scheduler {
    pub(crate) generic_schedulers: [*mut SubScheduler; MAX_SUB_SCHEDULERS],
}

pub(crate) static DIRTY_ITER: AtomicU64 = AtomicU64::new(0);
impl Scheduler {
    pub(crate) fn new(config: Config) -> SchedulerBuilder {
        SchedulerBuilder{
            config,
            incomplete: Scheduler { generic_schedulers: [ptr::null_mut(); MAX_SUB_SCHEDULERS] },
            registrations: 0,
            total_workers: 0,
        }
    }

    pub(crate) fn any_task<F, T>(&mut self, exec: F) -> Result<(), WorkerError<F>>
    where F: FID + FnMut(),
          T: TypesIdx,
    {
        let tid = T::get_or_register_tid();
        let fid = exec.get_or_register_fid();

        if tid >= MAX_SUB_SCHEDULERS || fid >= MAX_WORKERS_PER_SCHED {
            core::hint::cold_path();
            panic!("You tried to input types that have not been registered yet into either the register or this function")
        }

        let available_workers = WORKER_STATE[tid].get().load(Ordering::Acquire);

        let available_idx = available_workers.trailing_zeros() as usize;


        if available_idx == 64 {
            return Err(WorkerError::Busy(exec));
        }

        //SAFETY this is not a fetch_and due to the fact that the Scheduler is single threaded
        //and its only requirement is "finish this write before the next function call"
        //And the dependency prevents the cpu from reordering
        unsafe {WORKER_STATE[tid].get().as_ptr().write_volatile(available_workers & (!(1u64 << available_idx)))};
        //create true dependency to prevent OoOe
        let mut _no_use = unsafe {WORKER_STATE[tid].get().as_ptr().read_volatile()};
        //the volatile and blackbox are purely for the compiler to not do any tricks
        //and try to reorder and or eliminate any operations
        black_box(_no_use);
        //get the correct offset from the sub_scheduler for the given workers
        let offset = unsafe { (*self.generic_schedulers[tid]).offset };
        //the reference should be fine since the function providing the closure is global and "static"
        //TODO if reference isn't fine gonna try working with allocating the tasks either in .data or heap or something like that idk
        let raw = ptr::from_ref(&exec) as *mut F;
        //To my knowledge Zen5 doesn't have a dependency elimination (zeroing idioms) on
        //add x, !x and only on Cmp, Sub, Xor and SBB
        //TODO i dont know about other platforms and should probably be some cfg flags
        //TODO but the operation is pretty opaque in general
        let no_op_added: usize = unsafe {
            let negated = _no_use as usize;
            asm!(
            "neg {0}",
            "add {1}, {0}",
            in(reg) negated,
            inout(reg) _no_use);
            _no_use as usize
        };
        //overwrite the previous slot after setting the worker to busy
        //use the "no_op_added" inside-of the memory indexing so the cpu cannot start executing until the writes/reads are done
        //though its only 0, it doesn't change anything
        let slot: *mut Task = unsafe {ptr::from_ref(&TASK_SLOTS[offset + available_idx + no_op_added]) as *mut _};
        unsafe {slot.write_volatile(Task::new(raw))};


        //dirty iteration check
        //TODO remove, its bad and unsafe
        let old = unsafe {*DIRTY_ITER.as_ptr()};
        unsafe {DIRTY_ITER.as_ptr().write(old + 1)};
        return Ok(())
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

    //every worker has their own UNIQUE index into it and terminates once it's set to true
    //since its mostly only reads they can be in the same cache-line
    worker_terminate: Padded<[AtomicBool; MAX_WORKERS_PER_SCHED]>,

}

static mut SUB_SCHEDULERS:
[MaybeUninit<SubScheduler>; MAX_SUB_SCHEDULERS] =
    [const { MaybeUninit::uninit() }; MAX_SUB_SCHEDULERS];
impl SubScheduler {
    pub(crate) fn new<T: TypesIdx>(offset: usize, workers: usize) -> *mut Self {
        let tid = T::get_or_register_tid();
        assert!(tid < MAX_SUB_SCHEDULERS, "TID greater than MAX_SUB_SCHEDULERS");
        assert!(offset + workers <= unsafe {(*&raw mut TASK_SLOTS).len()}, "not enough global task slots for this sub_scheduler");


        //set uo the masks
        let mut mask = 0u64;
        mask |= (1 << workers) - 1;

        //println!("mask {:064b}", mask);

        //init the states for a given sub_scheduler
        WORKER_STATE[T::get_or_register_tid()].get().store(mask, Release);
        let handles: Box<[Option<JoinHandle<()>>; MAX_WORKERS_PER_SCHED]> = Box::new([const { None }; MAX_WORKERS_PER_SCHED]);
        let terminate = Padded([const { AtomicBool::new(false) }; MAX_WORKERS_PER_SCHED]);

        //This should be sound since im not creating any mutable references, hence "raw mut"
        let incomplete_schedulers: *mut SubScheduler = unsafe {(&raw mut SUB_SCHEDULERS[T::get_or_register_tid()]).cast::<SubScheduler>()};

        unsafe {
            core::ptr::write_volatile(&raw mut (*incomplete_schedulers).workers, workers);
            //println!("workers {}", (*incomplete_schedulers).workers);
            core::ptr::write_volatile(&raw mut (*incomplete_schedulers).offset, offset);
            //println!("offset {}", (*incomplete_schedulers).offset);
            core::ptr::write_volatile(&raw mut (*incomplete_schedulers).worker_terminate, terminate);
            core::ptr::write_volatile(&raw mut (*incomplete_schedulers).handles, handles);

            for w in 0..workers {
                let global_slot = offset + w;
                let worker = Worker::new(w,
                                         tid,
                                         &(*incomplete_schedulers).worker_terminate.get()[w],
                                         AtomicPtr::new(ptr::from_ref(&TASK_SLOTS[global_slot]) as *mut _)
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
            //println!("WHOLE: {:?}", self);
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
        //also the user cant call "T::get_or_register_tid()" anyway
        for sh in self.generic_schedulers[0..DropRange::get_or_register_tid()].into_iter(){
            unsafe {sh.drop_in_place()};
        }
    }
}
