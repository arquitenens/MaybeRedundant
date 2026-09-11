use std::arch::asm;
use std::hint::black_box;
use std::mem::{transmute, MaybeUninit};
use std::sync::atomic::{AtomicUsize, Ordering};

const MAX_ID_SLOTS: usize = 64;

const ADDRESS_SPACE_BITS: usize = 48;

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

fn private_search_style<T: IdxCache>(_ident: &T, table: &[usize], tid: usize) -> Result<usize, usize> {
    black_box(_ident);
    for (i, v) in table.iter().enumerate(){
        if *v == tid {
            return Result::Ok(i);
        }
    }
    return Result::Err(0);
}
pub trait IdxCache{
    ///The Tid might not appear in the order you registered them but is always the same and increments sequentially
    fn get_tid(&self) -> usize {
        static REG_ITEMS: AtomicUsize = AtomicUsize::new(0);
        static mut ID_TABLE: [[usize; MAX_ID_SLOTS]; 32] = [[usize::MAX; MAX_ID_SLOTS]; 32];

        let tid = private_tid(&self);

        //Max is ADDRESS_SPACE_BITS / 2 or 24 with 48 as address space;
        //a few times redunction in average case of N linear search while not being too expensive
        let addr = tid & ((1u64 << ADDRESS_SPACE_BITS) - 1) as usize;
        let mask = addr & 0x5555555555555555;
        let extra = (tid & 4095) % 7;
        let bucket: usize = ((mask.count_ones() + extra as u32) & 31) as usize;

        //the reference dies before the is indexed so it's fine
        let item = private_search_style(&self, unsafe {&*&raw const ID_TABLE[bucket]}, tid);

        //No dup
        if item.is_ok(){
            return item.unwrap();
        };

        //needs to happen after the duplicate check otherwise you're wasting slots
        let idx = REG_ITEMS.fetch_add(1, Ordering::Acquire);

        assert!(idx < MAX_ID_SLOTS,
                "You only have more than {} registered items, you can change it via the flags", MAX_ID_SLOTS);

        unsafe {
            (&raw mut ID_TABLE[bucket][idx]).write_volatile(tid);
        };
        black_box(&self);
        return idx;

    }
    //I don't need a valid instance of T
    #[inline]
    fn empty<'a, T>() -> &'a Self where Self: Sized {
        unsafe {
            transmute(&MaybeUninit::<T>::uninit())
        }
    }
}


pub trait FIDCache{
    ///The Tid might not appear in the order you registered them but is always the same and increments sequentially
    fn get_fid(&self) -> usize {
        static REG_ITEMS: AtomicUsize = AtomicUsize::new(0);
        static mut ID_TABLE: [[usize; MAX_ID_SLOTS]; 32] = [[usize::MAX; MAX_ID_SLOTS]; 32];

        let tid = private_tid(&self);

        //Max is ADDRESS_SPACE_BITS / 2 or 24 with 48 as address space;
        let addr = tid & ((1u64 << ADDRESS_SPACE_BITS) - 1) as usize;
        let mask = addr & 0x5555555555555555;
        let extra = (tid & 4095) % 7;
        let bucket: usize = ((mask.count_ones() + extra as u32) & 31) as usize;

        //the reference dies before the is indexed so it's fine
        let item = private_search_style(&self, unsafe {&*&raw const ID_TABLE[bucket]}, tid);

        //No dup
        if item.is_ok(){
            return item.unwrap();
        };

        //needs to happen after the duplicate check otherwise you're wasting slots
        let idx = REG_ITEMS.fetch_add(1, Ordering::Acquire);

        assert!(idx < MAX_ID_SLOTS,
                "You only have more than {} registered items, you can change it via the flags", MAX_ID_SLOTS);

        unsafe {
            (&raw mut ID_TABLE[bucket][idx]).write_volatile(tid);
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
