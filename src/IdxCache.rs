use std::arch::asm;
use std::hint::black_box;
use std::mem::{transmute, MaybeUninit};
use std::sync::atomic::{AtomicUsize, Ordering};

const MAX_ID_SLOTS: usize = 128;
const HYBRID_SWITCH_THRESHOLD: usize = 16;

//non-static type_id replacement
//must be inline never so llvm doesn't break the recursion
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
#[inline(never)]
fn private_search_style<T: IdxCache>(_: &T, table: &[usize], tid: usize) -> Result<usize, usize> {
    if MAX_ID_SLOTS >= HYBRID_SWITCH_THRESHOLD {
        return table.binary_search(&tid)
    }else {
        for (i, v) in table.iter().enumerate(){
            if *v == tid {
                return Result::Ok(i);
            }
        }
        return Result::Err(0);

    }
}
pub trait IdxCache{
    ///The Tid might not appear in the order you registered them but is always the same and increments sequentially
    fn get_tid(&self) -> usize {
        static REG_ITEMS: AtomicUsize = AtomicUsize::new(0);
        static mut ID_TABLE: [usize; MAX_ID_SLOTS] = [usize::MAX; MAX_ID_SLOTS];

        let tid = private_tid(&self);

        //the reference dies before the is indexed so it's fine
        let item = private_search_style(&self, unsafe {&*&raw const ID_TABLE}, tid);

        //No dup
        if item.is_ok(){
            return item.unwrap();
        };
        //needs to happen after the duplicate check otherwise you're wasting slots
        let idx = REG_ITEMS.fetch_add(1, Ordering::Relaxed);
        assert!(idx < MAX_ID_SLOTS,
                "You only have more than {} registered items, you can change it via the flags", MAX_ID_SLOTS);

        unsafe {
            *&raw mut ID_TABLE[idx] = tid;
            (*&raw mut ID_TABLE).sort();
        };
        black_box(&self);
        return idx;

    }
    //I don't need a valid instance of T
    fn empty<'a, T>() -> &'a Self where Self: Sized {
        unsafe {
            transmute(&MaybeUninit::<T>::zeroed())
        }
    }
}


pub trait FIDCache{
    ///The Tid might not appear in the order you registered them but is always the same and increments sequentially
    fn get_fid(&self) -> usize {
        static REG_ITEMS: AtomicUsize = AtomicUsize::new(0);
        static mut ID_TABLE: [usize; MAX_ID_SLOTS] = [usize::MAX; MAX_ID_SLOTS];

        let tid = private_tid(&self);

        //the reference dies before the is indexed so it's fine
        let item = private_search_style(&self, unsafe {&*&raw const ID_TABLE}, tid);

        //No dup
        if item.is_ok(){
            return item.unwrap();
        };
        //needs to happen after the duplicate check otherwise you're wasting slots
        let idx = REG_ITEMS.fetch_add(1, Ordering::Relaxed);
        assert!(idx < MAX_ID_SLOTS,
                "You only have more than {} registered items, you can change it via the flags", MAX_ID_SLOTS);

        unsafe {
            *&raw mut ID_TABLE[idx] = tid;
            (*&raw mut ID_TABLE).sort();
        };
        black_box(&self);
        return idx;

    }
    //I don't need a valid instance of T
    fn empty<'a, T>() -> &'a Self where Self: Sized {
        unsafe {
            transmute(&MaybeUninit::<T>::zeroed())
        }
    }
}
impl<T> IdxCache for T{}
impl<T> FIDCache for T{}
