use core::ptr::null_mut;
use core::sync::atomic::AtomicBool;
use core::ptr;
use std::mem;

#[repr(align(64))]
#[repr(C)]
#[derive(Debug)]
pub struct Task {
    pub(crate) callable: unsafe fn(*const ()),
    pub(crate) data: *const (),
    pub(crate) dropper: unsafe fn(*const ()),
}
unsafe impl Send for Task {}
unsafe impl Sync for Task {}

trait Taskable{
    unsafe fn execute(this: *const ());
    unsafe fn drop(this: *const ());

}

impl<F: FnOnce()> Taskable for F {
    #[inline(always)]
    unsafe fn execute(this: *const ()) {
        unsafe { ((this as *mut F).read_unaligned())() }
    }
    unsafe fn drop(this: *const ()) {
        //TODO PFft dont know if this is safe
        unsafe { (this as *mut F).swap(null_mut()) }
    }

}


unsafe fn noop_call(_: *const ()) {}

unsafe fn noop_drop(_: *const ()) {}

impl Task {
    pub(crate) const fn new<T>(data: *const T) -> Self where T: Taskable{
        Self {
            data: data as *const _,
            callable: <T as Taskable>::execute,
            dropper: <T as Taskable>::drop,
        }
    }
    pub(crate) const fn empty() -> Self {
        Self { data: ptr::null(), callable: noop_call, dropper: noop_drop }
    }

    #[inline(always)]
    pub(crate) unsafe fn t_execute(&mut self){
        unsafe { (self.callable)(self.data) }
    }


}
