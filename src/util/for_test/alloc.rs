use jemallocator::Jemalloc;
use static_assertions::assert_not_impl_any;
use std::alloc::*;
use std::cell::RefCell;

thread_local! {
    pub static STATS: RefCell<Stats> = RefCell::new(Stats::default());
}

#[global_allocator]
static TEST_ALLOC: TestAlloc<Jemalloc> = TestAlloc {
    inner: Jemalloc,
};

pub fn track() -> Tracker {
    Tracker::new()
}

pub struct Tracker {
    start: Stats,
    _no_send_sync: std::marker::PhantomData<*mut ()>,
}

assert_not_impl_any!(Tracker: Send, Sync);

impl Tracker {
    fn new() -> Self {
        let start = STATS.with(|v| *v.borrow());
        Self {
            start,
            _no_send_sync: Default::default(),
        }
    }

    pub fn stats(&self) -> Stats {
        let end = STATS.with(|v| *v.borrow());
        end - self.start
    }
}

#[derive(Clone, Copy, Default, Debug, Hash, PartialEq, Eq)]
pub struct Stats {
    pub allocs: u64,
    pub deallocs: u64,
}

impl std::ops::Add for Stats {
    type Output = Self;

    fn add(self, rhs: Self) -> Self::Output {
        Self {
            allocs: self.allocs + rhs.allocs,
            deallocs: self.deallocs + rhs.deallocs,
        }
    }
}

impl std::ops::Sub for Stats {
    type Output = Self;

    fn sub(self, rhs: Self) -> Self::Output {
        Self {
            allocs: self.allocs - rhs.allocs,
            deallocs: self.deallocs - rhs.deallocs,
        }
    }
}

#[derive(Default, Debug)]
struct TestAlloc<T> {
    inner: T,
}

impl<T> TestAlloc<T> {
    fn track(f: impl FnOnce(&mut Stats)) {
        STATS.with(|v| f(&mut v.borrow_mut()))
    }
}

unsafe impl<T: GlobalAlloc> GlobalAlloc for TestAlloc<T> {
    unsafe fn alloc(&self, layout: Layout) -> *mut u8 {
        Self::track(|s| { s.allocs = s.allocs.checked_add(1).unwrap(); });
        self.inner.alloc(layout)
    }

    unsafe fn dealloc(&self, ptr: *mut u8, layout: Layout) {
        Self::track(|s| { s.deallocs = s.deallocs.checked_add(1).unwrap(); });
        self.inner.dealloc(ptr, layout)
    }

    unsafe fn alloc_zeroed(&self, layout: Layout) -> *mut u8 {
        Self::track(|s| { s.allocs = s.allocs.checked_add(1).unwrap(); });
        self.inner.alloc_zeroed(layout)
    }

    unsafe fn realloc(&self, ptr: *mut u8, layout: Layout, new_size: usize) -> *mut u8 {
        self.inner.realloc(ptr, layout, new_size)
    }
}