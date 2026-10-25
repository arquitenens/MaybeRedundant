use core::arch::asm;
use core::hint::black_box;
use core::mem::{transmute, MaybeUninit};
use core::sync::atomic::{AtomicUsize, Ordering};
use std::any::{Any, TypeId};
use std::arch::x86_64::{__m512i, _mm512_cmpeq_epi64_mask, _mm512_loadu_si512, _mm512_set1_epi64};
use std::ptr::drop_in_place;
use crate::scheduler::Scheduler;
//non-static type_id replacement
//must be inline never so llvm doesn't break the recursion
//This probably only works on llvm or as long as rustc is using it so pfffttt

//later note, im fed up with typeid and I hate typeid
//also yeah im totally ignoring parametricity but ehh rules
#[inline(never)]
fn private_tid<T: IdxCache>(own: &T) -> usize {
    let _unique_t: fn(&T) -> &T = |x|{
        //This is super awkward but essentially allows me to stop LLVM from merging the functions thus breaking this
        //its seeing the cyclic dependency and thus doesn't merge
        //-------------------------------------------------------
        //it is not transitive.
        //And this is the only case we can't convert to less-equal-greater comparison.
        //It is a seldom case, 4-5 functions of 10000 (checked in test-suite),
        //and, we hope, the reader would forgive us
        //for such a sacrifice in order to get the O(log(N)) pass time.
        let value = unsafe { asm!("mov {0}, {0}", in(reg) x); x };
        //crate self reference / cyclic
        let mut imlosingit = private_tid(value);
        unsafe { asm!("mov {0}, {0}", inout(reg) imlosingit); imlosingit };
        value
    };
    unsafe { asm!("mov {0}, {0}", in(reg) own); own };
    _unique_t as usize
}

#[inline]
fn bucket(tid: u64) -> usize {
    (tid >> 5 & 63) as usize
}


pub(crate) trait IdxCache{
    ///The Tid might not appear in the order you registered them but is always the same and increments sequentially
    ///You can register 63 unique types, why not 64 you might ask? well it just is that way
    fn get_tid(&self) -> usize {
        static mut CONTAINED: [usize; 64] = [usize::MAX; 64];
        static REGISTRATIONS: AtomicUsize = AtomicUsize::new(0);
        let cnt = bucket(private_tid(&self) as u64);
        unsafe {
            //Not yet seen
            if CONTAINED[cnt] == usize::MAX {
                let old = REGISTRATIONS.fetch_add(1, Ordering::Relaxed);
                assert!(old <= 62);
                CONTAINED[cnt] = old;
            }

        }
        unsafe {CONTAINED[cnt]}
    }


    //I don't need a valid instance of T
    //This is no doubt really unsafe
    #[inline]
    unsafe fn empty<'a, T>() -> &'a Self where Self: Sized {
        unsafe {
            transmute(&MaybeUninit::<T>::uninit())
        }
    }
}

///FID is just a different static counter so it doesnt clash with IdxCache for all other types
pub(crate) trait FIDCache{
    ///The Tid might not appear in the order you registered them but is always the same and increments sequentially
    fn get_fid(&self) -> usize {
        static mut CONTAINED: [usize; 64] = [usize::MAX; 64];
        static REGISTRATIONS: AtomicUsize = AtomicUsize::new(0);
        let cnt = bucket(private_tid(&self) as u64);
        unsafe {
            //Not yet seen
            if CONTAINED[cnt] == usize::MAX {
                let old = REGISTRATIONS.fetch_add(1, Ordering::Relaxed);
                assert!(old <= 62);
                CONTAINED[cnt] = old;
            }

        }
        unsafe {CONTAINED[cnt]}
    }
    //I don't need a valid instance of T
    //but still pretty unsafe
    #[inline]
    unsafe fn empty<'a, T>() -> &'a Self where Self: Sized {
        unsafe {
            transmute(&MaybeUninit::<T>::uninit())
        }
    }
}

pub(crate) trait SchedCache{
    fn get_sid(&self) -> usize {
        static mut CONTAINED: [usize; 64] = [usize::MAX; 64];
        static REGISTRATIONS: AtomicUsize = AtomicUsize::new(0);
        let cnt = bucket(private_tid(&self) as u64);
        unsafe {
            //Not yet seen
            if CONTAINED[cnt] == usize::MAX {
                let old = REGISTRATIONS.fetch_add(1, Ordering::Relaxed);
                assert!(old <= 62);
                CONTAINED[cnt] = old;
            }

        }
        unsafe {CONTAINED[cnt]}
    }
    //I don't need a valid instance of T
    //but still pretty unsafe
    #[inline]
    unsafe fn empty<'a, T>() -> &'a Self where Self: Sized {
        unsafe {
            transmute(&MaybeUninit::<T>::uninit())
        }
    }
}
impl<T> IdxCache for T{}
impl<T> FIDCache for T{}

pub trait NoMove{
    fn no_move(self) where Self: Sized{
        black_box(self);
    }
}

impl<T> NoMove for T {}

impl<const N: usize> SchedCache for Scheduler<N>{}
