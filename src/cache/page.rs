pub mod internal;
pub mod lock;

use std::alloc;
use std::ops::{Deref, DerefMut};
use std::ptr::NonNull;

use crate::async_rt::*;
use super::scope::Scope;
use internal::{Page_, PagePtr};

/// Logical page identifier.
pub type PageId = u32;

struct AlignedBuf {
    p: NonNull<u8>,
    layout: alloc::Layout,
}

impl AlignedBuf {
    fn new(size: usize, align: usize) -> Self {
        let layout = alloc::Layout::from_size_align(size, align).unwrap();
        let p = NonNull::new(unsafe {
            alloc::alloc_zeroed(layout)
        }).expect("out of memory");
        Self {
            p,
            layout,
        }
    }
}

impl Deref for AlignedBuf {
    type Target = [u8];

    fn deref(&self) -> &Self::Target {
        unsafe {
            std::slice::from_raw_parts(self.p.as_ptr(), self.layout.size())
        }
    }
}

impl DerefMut for AlignedBuf {
    fn deref_mut(&mut self) -> &mut Self::Target {
        unsafe {
            std::slice::from_raw_parts_mut(self.p.as_ptr(), self.layout.size())
        }
    }
}

impl Drop for AlignedBuf {
    fn drop(&mut self) {
        unsafe {
            alloc::dealloc(self.p.as_ptr(), self.layout);
        }
    }
}

pub struct Page {
    dirty: bool,
    bytes: AlignedBuf,
}

impl Page {
    pub(in super) fn new(size: usize, align: usize) -> Self {
        Self {
            dirty: false,
            bytes: AlignedBuf::new(size, align),
        }
    }

    pub fn is_dirty(&self) -> bool {
        self.dirty
    }

    pub fn set_dirty(&mut self, dirty: bool) {
        self.dirty = dirty;
    }
}

impl Deref for Page {
    type Target = [u8];

    fn deref(&self) -> &Self::Target {
        &self.bytes
    }
}

impl DerefMut for Page {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.bytes
    }
}

pub struct PageReadGuard<'a>(AsyncRwLockReadGuard<'a, Page>);

impl Deref for PageReadGuard<'_> {
    type Target = Page;

    fn deref(&self) -> &Self::Target {
        &*self.0
    }
}

pub struct PageWriteGuard<'a>(AsyncRwLockWriteGuard<'a, Page>);

impl<'a> PageWriteGuard<'a> {
    fn new(mut inner: AsyncRwLockWriteGuard<'a, Page>) -> Self {
        inner.set_dirty(true);
        Self(inner)
    }
}

impl Deref for PageWriteGuard<'_> {
    type Target = Page;

    fn deref(&self) -> &Self::Target {
        &*self.0
    }
}

impl DerefMut for PageWriteGuard<'_> {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut *self.0
    }
}

#[derive(Debug)]
pub struct PageRef<T: Scope>(PagePtr<T>);

impl<T: Scope> PageRef<T> {
    pub(in super) fn new(p: PagePtr<T>) -> Self {
        Self(p)
    }

    pub fn id(&self) -> PageId {
        self.page().id()
    }

    pub fn scope(&self) -> &T {
        self.page().scope().scope()
    }

    pub fn len(&self) -> usize {
        self.page().scope().page_len()
    }

    pub async fn read(&self) -> PageReadGuard<'_> {
        PageReadGuard(self.page().data().read().await)
    }

    pub fn try_read(&self) -> Option<PageReadGuard> {
        self.page().data().try_read().ok().map(PageReadGuard)
    }

    pub async fn write(&self) -> PageWriteGuard<'_> {
        PageWriteGuard::new(self.page().data().write().await)
    }

    pub fn try_write(&self) -> Option<PageWriteGuard> {
        self.page().data().try_write().ok().map(PageWriteGuard::new)
    }

    fn page(&self) -> &Page_<T> {
        unsafe { self.0.as_ref() }
    }
}

impl<T: Scope> Clone for PageRef<T> {
    fn clone(&self) -> Self {
        self.page().lock_shared_data().unwrap()
    }
}

impl<T: Scope> Drop for PageRef<T> {
    fn drop(&mut self) {
        self.page().lock().unlock_shared();
    }
}

unsafe impl<T: Scope> Send for PageRef<T> {}
unsafe impl<T: Scope> Sync for PageRef<T> {}