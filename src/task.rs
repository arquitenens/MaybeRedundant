use std::{mem, ptr};
use std::ptr::{null, null_mut};
use std::sync::atomic::AtomicBool;
use crate::scheduler::Padded;

#[repr(align(64))]
#[repr(C)]
#[derive(Debug)]
pub struct Task {
    //TODO i dont know if i need this to be padded, most likely not
    pub(crate) is_exclusive: AtomicBool,
    pub(crate) callable: unsafe fn(*const ()),
    pub(crate) data: *const (),
    pub(crate) dropper: unsafe fn(*const ()),
}

trait Taskable{
    unsafe fn execute(this: *const ());
    unsafe fn drop(this: *const ());
}

impl<F: FnMut()> Taskable for F {
    #[inline(always)]
    unsafe fn execute(this: *const ()) {
        unsafe { (*(this as *mut F))() }
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
            is_exclusive: AtomicBool::new(false),
            data: data as *const _,
            callable: <T as Taskable>::execute,
            dropper: <T as Taskable>::drop,
        }
    }
    pub(crate) const fn empty() -> Self {
        Self { is_exclusive: AtomicBool::new(false), data: ptr::null(), callable: noop_call, dropper: noop_drop }
    }

    #[inline(always)]
    pub unsafe fn execute(&mut self){
        unsafe { (self.callable)(self.data) }
    }


}
