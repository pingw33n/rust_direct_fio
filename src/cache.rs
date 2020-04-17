mod page;
mod scope;

use matches::matches;
use parking_lot::*;
use std::cmp;
use std::io::{Error, ErrorKind, Result};
use std::sync::Arc;
use std::sync::atomic::*;

pub use page::{Page, PageId, PageReadGuard, PageRef, PageWriteGuard};
pub use scope::Scope;

use crate::async_rt::*;
use page::internal::*;
use page::lock::LockState;
use scope::*;

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct Options {
    pub max_resident: usize,
    pub max_non_resident: usize,
    pub page_len: usize,
    pub page_align: usize,
}

pub fn new<T: Scope>(options: Options) -> CacheHandle<T> {
    CacheHandle(Arc::new(Cache::new(options)))
}

pub struct CacheHandle<T: Scope>(Arc<Cache<T>>);

impl<T: Scope> CacheHandle<T> {
    pub fn max_resident(&self) -> usize {
        self.0.options.max_resident
    }

    pub fn max_non_resident(&self) -> usize {
        self.0.options.max_non_resident
    }

    pub fn page_len(&self) -> usize {
        self.0.options.page_len
    }

    pub fn hit_count(&self) -> u64 {
        self.0.hit_count()
    }

    pub fn miss_count(&self) -> u64 {
        self.0.miss_count()
    }

    pub fn read_count(&self) -> u64 {
        self.0.read_count()
    }

    pub fn write_count(&self) -> u64 {
        self.0.write_count()
    }

    pub fn new_scope(&self, scope: T) -> ScopeHandle<T> {
        ScopeHandle {
            cache: self.clone(),
            scope: ScopeRef::new(scope, self.0.options.page_len),
        }
    }
}

impl<T: Scope> Clone for CacheHandle<T> {
    fn clone(&self) -> Self {
        Self(self.0.clone())
    }
}

pub struct ScopeHandle<T: Scope> {
    cache: CacheHandle<T>,
    scope: ScopeRef<T>,
}

impl<T: Scope> ScopeHandle<T> {
    pub fn downgrade(&self) -> WeakScopeHandle<T> {
        WeakScopeHandle {
            cache: self.cache.clone(),
            scope: self.scope.downgrade(),
        }
    }

    pub fn cache(&self) -> &CacheHandle<T> {
        &self.cache
    }

    pub fn get(&self) -> &T {
        self.scope.get()
    }

    pub async fn page(&self, id: PageId) -> Result<PageRef<T>> {
        self.cache.0.page(&self.scope, id).await
    }

    /// Writes all dirty pages clearing the dirty flag.
    /// Pages that are write-locked will be skipped.
    /// Returns the number of pages written.
    pub async fn flush(&self) -> Result<usize> {
        self.cache.0.flush(&self.scope).await
    }

    /// Clears the dirty flag of all pages.
    /// Pages that are write-locked will be skipped.
    pub fn discard(&self) {
        self.cache.0.discard(&self.scope)
    }
}

impl<T: Scope> Clone for ScopeHandle<T> {
    fn clone(&self) -> Self {
        Self {
            cache: self.cache.clone(),
            scope: self.scope.clone(),
        }
    }
}

pub struct WeakScopeHandle<T: Scope> {
    cache: CacheHandle<T>, // TODO Make this weak too.
    scope: WeakScopeRef<T>,
}

impl<T: Scope> WeakScopeHandle<T> {
    pub fn upgrade(&self) -> Option<ScopeHandle<T>> {
        self.scope.upgrade()
            .map(|scope| ScopeHandle {
                cache: self.cache.clone(),
                scope,
            })
    }
}

impl<T: Scope> Clone for WeakScopeHandle<T> {
    fn clone(&self) -> Self {
        Self {
            cache: self.cache.clone(),
            scope: self.scope.clone(),
        }
    }
}

struct Stop<T>(Option<T>);

impl<T: Eq> Stop<T> {
    pub fn new() -> Self {
        Self(None)
    }

    pub fn track(&mut self, v: T) {
        if self.0.is_none() {
            self.0 = Some(v);
        }
    }

    #[must_use]
    pub fn check(&self, v: T) -> bool {
        self.0 != Some(v)
    }
}

#[derive(Clone, Copy, Debug)]
enum PageCountKind {
    Hot,
    Cold {
        resident: bool,
    },
}

#[derive(Clone, Copy, Debug)]
enum PageCountUpdate {
    Inc,
    Dec,
}

struct DonorPage<T: Scope> {
    id: PageId,
    scope: Arc<ScopeShare<T>>,
}

struct CacheInner<T: Scope> {
    options: Options,
    pages: PagePtr<T>,
    hot_page_count: usize,
    resident_cold_page_count: usize,
    non_resident_page_count: usize,
    io_page_count: usize,
    hand_hot: PagePtr<T>,
    // HAND_cold is used to search for a resident cold page for replacement.
    hand_cold: PagePtr<T>,
    hand_cold_cost: usize,
    max_hand_cold_cost: usize,
    hand_test: PagePtr<T>,
}

impl<T: Scope> CacheInner<T> {
    fn new(options: Options) -> Self {
        assert!(options.max_resident > 0);

        let pages = Page_::end();

        Self {
            options,
            pages,
            hot_page_count: 0,
            resident_cold_page_count: 0,
            non_resident_page_count: 0,
            io_page_count: 0,
            hand_hot: pages,
            hand_cold: pages,
            hand_cold_cost: 0,
            max_hand_cold_cost: 0,
            hand_test: pages,
        }
    }

    #[cfg(test)]
    fn dump_short(&self) {
        let end = self.end();
        let mut page_ptr = self.pages;
        while page_ptr != end {
            let page = unsafe { page_ptr.as_ref() };
            page.dump_short();
            page_ptr = page.next;
        }
        println!();
    }

    #[cfg(test)]
    fn dump(&self, pages: bool) {
        println!("hot_page_count: {}", self.hot_page_count);
        println!("resident_cold_page_count: {}", self.resident_cold_page_count);
        println!("non_resident_page_count: {}", self.non_resident_page_count);
        println!("hand_hot: {:?}", unsafe { self.hand_hot.as_ref() }.id());
        println!("hand_cold: {:?}", unsafe { self.hand_cold.as_ref() }.id());
        println!("hand_cold_cost: {}", self.hand_cold_cost);
        println!("max_hand_cold_cost: {}", self.max_hand_cold_cost);
        println!("hand_test: {:?}", unsafe { self.hand_test.as_ref() }.id());

        if pages {
            let end = self.end();
            let mut page = self.pages;
            let mut i = 0;
            while page != end {
                println!("Page #{} ({:?})", i, page);
                self.dump_page(page);
                page = unsafe { page.as_ref() }.next;
                i += 1;
            }
        }
    }

    #[cfg(test)]
    fn dump_page(&self, page: PagePtr<T>) {
        let page = unsafe { page.as_ref() };
        println!("  id: {:?}", page.id());
        println!("  next: {:?}", page.next);
        println!("  prev: {:?}", page.prev);
        println!("  resident: {:?}", page.is_resident());
        println!("  status: {:?}", page.status);
        println!("  lock: {:?}", page.lock().state());
        println!("  refd: {}", page.is_refd());
    }

    fn run_hand_test(&mut self) {
        let stop = self.hand_test;
        loop {
            let mut page = self.hand_test;
            let page = unsafe { page.as_mut() };
            // HAND_Test moves forward and stops at the next cold page.
            self.hand_test = page.next;
            match page.status {
                Status::Cold | Status::Test => {
                    page.status = Status::Cold;
                    if !page.is_resident() {
                        let page = page.ptr();
                        self.remove_page(page, PageCountKind::Cold { resident: false });
                        break;
                    }
                }
                _ => {}
            }
            if self.hand_test == stop {
                break;
            }
        }
    }

    fn run_hand_cold(&mut self) -> (DonorPage<T>, Box<AsyncRwLock<Page>>) {
        if self.resident_cold_page_count == 0 {
            self.run_hand_hot(true);
        }
        debug_assert!(self.resident_cold_page_count > 0);

        // The hand will keep moving until it encounters a cold page eligible for replacement,
        // and stops at the next resident cold page.
        loop {
            self.hand_cold_cost += 1;

            let mut page = self.hand_cold;
            let page = unsafe { page.as_mut() };
            self.hand_cold = page.next;

            match page.status {
                Status::Cold | Status::Test if page.is_resident() => {
                    if page.lock().is_locked()
                        || page.take_refd()
                        || !page.lock().lock_exclusive()
                    {
                        // If reference bit is set.

                        if page.status == Status::Test {
                            // If page is in its test period, we turn the cold page into a hot page, and ask
                            // HAND_hot for its actions.
                            page.status = Status::Hot;
                            self.update_page_count(PageCountKind::Cold { resident: true }, PageCountUpdate::Dec);
                            self.update_page_count(PageCountKind::Hot, PageCountUpdate::Inc);
                            self.run_hand_hot(true);
                        } else {
                            // If reference bit is set but it is not in its test period, there are no status
                            // change as well as HAND_hot actions.
                        }

                        // In both of the cases, reference bit is reset, and we move the page to the list head.

                        self.remove_page_from_list(page);
                        self.add_page_to_list(page);
                    } else {
                        // If the reference bit of the cold page currently pointed to by HAND_cold is unset,
                        // we replace the cold page for a free space.
                        let page_data = page.take_data().unwrap();

                        let id = page.id();
                        let scope = page.scope().clone();

                        if page.status == Status::Test {
                            // The replaced cold page will remain in the list as a non-resident cold page until it
                            // runs out of its test period, if it is in its test period.

                            // We keep track of the number of non-resident cold pages.
                            // Once the number exceeds _m_, the memory size in the number of pages, we
                            // terminate the test period of the cold page pointed to by HAND_test.
                            self.update_page_count(PageCountKind::Cold { resident: true }, PageCountUpdate::Dec);
                            self.update_page_count(PageCountKind::Cold { resident: false }, PageCountUpdate::Inc);

                            debug_assert!(self.non_resident_page_count <= self.options.max_non_resident + 1);
                            if self.non_resident_page_count > self.options.max_non_resident {
                                self.run_hand_test();
                                debug_assert!(self.non_resident_page_count == self.options.max_non_resident);
                            }
                        } else {
                            // If not (in the test period), we move it out of the clock.
                            self.remove_page(page.ptr(), PageCountKind::Cold { resident: true });
                        }

                        break (DonorPage { id, scope }, page_data);
                    }
                }
                _ => {}
            }
        }
    }

    fn acquire_page_data(&mut self) -> Option<(Option<DonorPage<T>>, Box<AsyncRwLock<Page>>)> {
        debug_assert!(self.resident_page_count() <= self.options.max_resident);
        debug_assert!(self.io_page_count <= self.resident_page_count());
        if self.resident_page_count() < self.options.max_resident {
            let page = Page::new(self.options.page_len, self.options.page_align);
            return Some((None, Box::new(AsyncRwLock::new(page))));
        } else if self.resident_page_count() == self.io_page_count {
            // All potential donor pages are locked in IO.
            // The operation must be retried.
            return None;
        }
        let (donor_page, data) = self.run_hand_cold();
        debug_assert!(self.resident_page_count() < self.options.max_resident);
        self.max_hand_cold_cost = cmp::max(self.max_hand_cold_cost, self.hand_cold_cost);
        Some((Some(donor_page), data))
    }

    fn resident_page_count(&self) -> usize {
        self.hot_page_count + self.resident_cold_page_count
    }

    /// If `cold_page_required` is `true` this method ensures that there's at least one resident
    /// cold page available on return.
    fn run_hand_hot(&mut self, cold_page_required: bool) {
        let mut stop = Stop::new();
        while stop.check(self.hand_hot) {
            let mut page = self.hand_hot;
            let page = unsafe { page.as_mut() };
            self.hand_hot = page.next;

            match page.status {
                Status::Hot => if self.handle_hot(page) {
                    return;
                }
                Status::Cold | Status::Test => {
                    // Whenever the hand encounters a cold page, it will terminate the page’s test period.
                    page.status = Status::Cold;

                    // The hand will also remove the cold page from the clock if it is non-resident
                    // (the most probable case).
                    // TODO maybe not remove here and remove only when needed (non_resident_page_count > options.max_non_resident)
                    if !page.is_resident() {
                        let page = page.ptr();
                        self.remove_page(page, PageCountKind::Cold { resident: false });
                    } else {
                        stop.track(page.ptr())
                    }
                }
                Status::ProtectedTest | Status::End => stop.track(page.ptr()),
            }
        }
        // We did a cycle but could turn hot page into cold.
        // Cold hand relies on always being able to return a buffer when no new buffers can be created.
        // If the latter is the case we need to ensure there's at least one cold resident page available.
        if cold_page_required
            && self.resident_cold_page_count == 0
            && self.hot_page_count > 0
        {
            loop {
                let mut page = self.hand_hot;
                let page = unsafe { page.as_mut() };
                self.hand_hot = page.next;
                if page.status == Status::Hot && self.handle_hot(page) {
                    break;
                }
            }
        }
        debug_assert!(!cold_page_required || self.resident_cold_page_count > 0);
    }

    #[must_use]
    fn handle_hot(&mut self, page: &mut Page_<T>) -> bool {
        debug_assert_eq!(page.status, Status::Hot);
        if page.lock().is_locked() || page.take_refd() {
            // If the ref bit is set, which indicates the page has been re-accessed, we spare
            // this page, reset its reference bit and keep it as a hot page.
            false
        } else {
            // If the reference bit of the hot page pointed to by HAND_hot is unset, we can simply
            // change its status and then move the hand forward.
            debug_assert!(page.is_resident());
            page.status = Status::Test;
            self.update_page_count(PageCountKind::Hot, PageCountUpdate::Dec);
            self.update_page_count(PageCountKind::Cold { resident: true }, PageCountUpdate::Inc);
            true
        }
    }

    fn end(&self) -> PagePtr<T> {
        unsafe { self.pages.as_ref() }.prev
    }

    #[inline]
    fn update_page_count(&mut self, kind: PageCountKind, update: PageCountUpdate) {
        match update {
            PageCountUpdate::Inc => match kind {
                PageCountKind::Hot => self.hot_page_count += 1,
                PageCountKind::Cold { resident: false } => self.non_resident_page_count += 1,
                PageCountKind::Cold { resident: true } => self.resident_cold_page_count += 1,
            }
            PageCountUpdate::Dec => match kind {
                PageCountKind::Hot => self.hot_page_count -= 1,
                PageCountKind::Cold { resident: false } => self.non_resident_page_count -= 1,
                PageCountKind::Cold { resident: true } => self.resident_cold_page_count -= 1,
            }
        }
    }

    #[inline]
    fn transit_page_count(&mut self, from: PageCountKind, to: PageCountKind) {
        self.update_page_count(from, PageCountUpdate::Dec);
        self.update_page_count(to, PageCountUpdate::Inc);
    }

    fn add_page_to_list(&mut self, page: &mut Page_<T>) {
        let mut end = self.end();
        let end = unsafe { end.as_mut() };
        debug_assert_ne!(page.ptr(), end.ptr());
        if self.pages == end.ptr() {
            self.pages = page.ptr();
            end.next = page.ptr();
        } else {
            unsafe { end.prev.as_mut() }.next = page.ptr();
        }
        page.prev = end.prev;
        end.prev = page.ptr();
        page.next = end.ptr();
    }

    fn add_page(&mut self, mut page: Box<Page_<T>>, count_kind: PageCountKind) -> PagePtr<T> {
        self.add_page_to_list(&mut page);
        self.update_page_count(count_kind, PageCountUpdate::Inc);

        let id = page.id();
        let ptr = page.ptr();

        {
            let mut page_map = page.scope().page_map_w();
            page_map.update(id, page.ptr());
            page_map.refresh();
        }

        std::mem::forget(page);

        ptr
    }

    fn remove_page_from_list(&mut self, page: &mut Page_<T>) {
        debug_assert_ne!(page.ptr(), self.end());
        let next = page.next;
        if self.pages == page.ptr() {
            self.pages = next;
        }
        if self.hand_hot == page.ptr() {
            self.hand_hot = next;
        }
        if self.hand_cold == page.ptr() {
            self.hand_cold = next;
        }
        if self.hand_test == page.ptr() {
            self.hand_test = next;
        }
        unsafe { page.prev.as_mut() }.next = next;
        unsafe { page.next.as_mut() }.prev = page.prev;
    }

    fn remove_page(&mut self, page: PagePtr<T>, count_kind: PageCountKind) {
        let mut page = unsafe { Page_::from_ptr(page) };

        self.remove_page_from_list(&mut page);
        self.update_page_count(count_kind, PageCountUpdate::Dec);

        let mut page_map = page.scope().page_map_w();
        page_map.empty(page.id());
        page_map.refresh();
    }

    fn replace_page_data(&mut self, mut page: PagePtr<T>, data: Box<AsyncRwLock<Page>>)
        -> PagePtr<T>
    {
        let page = unsafe { page.as_mut() };

        debug_assert_eq!(page.lock().state(), LockState::Exclusive);

        // If the cold page is in the list (must be in its test period), the faulted page turns
        // into a hot page and is placed at the head of the list.
        debug_assert_eq!(page.status, Status::ProtectedTest);
        self.remove_page_from_list(page);
        self.update_page_count(PageCountKind::Cold { resident: false }, PageCountUpdate::Dec);

        page.status = Status::Hot;
        page.set_data(data);

        // We run HAND_hot to turn a hot page with a large recency into a cold page.
        self.run_hand_hot(false);

        self.add_page_to_list(page);
        self.update_page_count(PageCountKind::Hot, PageCountUpdate::Inc);

        page.ptr()
    }

    fn add_page_data(&mut self, scope: &ScopeRef<T>, id: PageId, data: Box<AsyncRwLock<Page>>)
        -> PagePtr<T>
    {
        // If the faulted cold page is not in the list, its reuse distance is highly likely to
        // be larger than the recency of hot pages. So the page is still categorized a
        // cold page and is placed at the list head. The page also initiates its test period.

        // TODO reuse pages.
        let page = Page_::new(scope, id, data);

        let page = self.add_page(page, PageCountKind::Cold { resident: true });

        page
    }
}

unsafe impl<T: Scope> Send for CacheInner<T> {}

impl<T: Scope> Drop for CacheInner<T> {
    fn drop(&mut self) {
        let end = self.end();
        let mut page = self.pages;
        while page != end {
            page = unsafe { Page_::from_ptr(page) }.next;
        }
        unsafe { Page_::from_ptr(end); }
    }
}

struct Cache<T: Scope> {
    inner: Mutex<CacheInner<T>>,
    options: Options,
    hit_count: AtomicU64,
    miss_count: AtomicU64,
    read_count: AtomicU64,
    write_count: AtomicU64,
}

impl<T: Scope> Cache<T> {
    fn new(options: Options) -> Self {
        Self {
            inner: Mutex::new(CacheInner::new(options.clone())),
            options,
            hit_count: 0.into(),
            miss_count: 0.into(),
            read_count: 0.into(),
            write_count: 0.into(),
        }
    }

    pub fn hit_count(&self) -> u64 {
        self.hit_count.load(Ordering::Relaxed)
    }

    pub fn miss_count(&self) -> u64 {
        self.miss_count.load(Ordering::Relaxed)
    }

    pub fn read_count(&self) -> u64 {
        self.read_count.load(Ordering::Relaxed)
    }

    pub fn write_count(&self) -> u64 {
        self.write_count.load(Ordering::Relaxed)
    }

    pub async fn page(self: &Arc<Self>, scope: &ScopeRef<T>, id: PageId) -> Result<PageRef<T>> {
        if let Some(page) = scope.page_map_r().get(&id) {
            let mut page = *page.iter().next().unwrap();
            let page = unsafe { page.as_mut() };
            if let Some(data) = page.lock_shared_data() {
                page.mark_refd();
                self.hit_count.fetch_add(1, Ordering::Relaxed);
                return Ok(data);
            }
        }

        self.miss_count.fetch_add(1, Ordering::Relaxed);

        // When there is a page fault, the faulted page must be a cold page.

        let (donor_page, mut page, page_io) = loop {
            let mut inner = self.inner.lock();

            let page = {
                if let Some(page) = scope.page_map_r().get(&id) {
                    Some(*page.iter().next().unwrap())
                } else {
                    None
                }
            };
            if let Some(mut page) = page {
                let page = unsafe { page.as_mut() };
                if page.io().is_running() {
                    // Other task is doing IO on the page.
                    // Register itself as waiter.
                    page.io_mut().register_waiter();
                    break (None, page.ptr(), None);
                } else if let Some(page) = page.lock_shared_data() {
                    // Other task has already done IO on the page.
                    // Still not counting this as a hit.
                    return Ok(page);
                }

                debug_assert!(!page.is_resident());

                // Protect the page from being removed or altered otherwise.
                debug_assert!(matches!(page.status, Status::Test | Status::ProtectedTest),
                    "{:?}", page.status);
                page.status = Status::ProtectedTest;
            }

            // We first run HAND_cold for a free space.

            let (data_page_id, data) = if let Some(v) = inner.acquire_page_data() {
                v
            } else {
                continue;
            };
            let page = if let Some(page) = page {
                inner.replace_page_data(page, data)
            } else {
                inner.add_page_data(scope, id, data)
            };

            scope.page_map_w().refresh();

            // Must be done inside the current lock scope so other threads won't lock it before
            // we do.
            let page_io = unsafe { &mut *page.as_ptr() }.io_mut().begin();

            inner.hand_cold_cost = 0;
            inner.io_page_count += 1;

            break (data_page_id, page, Some(page_io));
        };

        // Here it's guaranteed the page doesn't get dropped because PageIo.waiters is the deferred
        // shared lock count of the page.

        {
            if let Some(page_io) = page_io {
                let (write_result, read_result) = {
                    let data = unsafe { page.as_ref() }.data();
                    let mut data = data.try_write().unwrap();
                    let write_result = if let Some(donor_page) = donor_page {
                        self.write(&donor_page.scope, donor_page.id, &mut data).await
                    } else  {
                        Ok(false)
                    };
                    let read_result = if write_result.is_ok() {
                        Some(self.read(scope.share(), id, &mut data).await)
                    } else {
                        None
                    };
                    (write_result, read_result)
                };

                let (page_io_result, result) = match (write_result, read_result) {
                    (Ok(_), Some(r))
                    => {
                        let pr = if r.is_ok() { Ok(()) } else { Err(IoError::Read) };
                        (pr, r)
                    }
                    (Err(e), None) => {
                        (Err(IoError::Write), Err(e))
                    }
                    _ => unreachable!(),
                };

                {
                    let mut inner = self.inner.lock();
                    unsafe { page.as_mut() }.finish_io(page_io, page_io_result);
                    inner.io_page_count -= 1;

                    if result.is_err() {
                        // TODO Recycle the page data in case of error.
                        let page = unsafe { page.as_mut() };
                        page.take_data().unwrap();
                        let from = match page.status {
                            Status::Hot => {
                                page.status = Status::Cold;
                                PageCountKind::Hot
                            }
                            Status::Test => PageCountKind::Cold { resident: true },
                            | Status::Cold
                            | Status::ProtectedTest
                            | Status::End
                            => unreachable!(),
                        };
                        inner.transit_page_count(from, PageCountKind::Cold { resident: false });

                        // The page is left locked exclusively.

                        return result.map(|_| unreachable!());
                    }
                }
            } else {
                match unsafe { page.as_ref() }.io().wait_finish().await {
                    Ok(_) => {},
                    Err(IoError::Read) => return Err(Error::new(ErrorKind::Other,
                        format!("error reading page {}", id))),
                    Err(IoError::Write) => return Err(Error::new(ErrorKind::Other,
                        format!("error writing page {}", donor_page.as_ref().unwrap().id))),
                }

            }
        }

        Ok(unsafe { page.as_ref() }.data_ref())
    }

    pub async fn flush(&self, scope: &ScopeRef<T>) -> Result<usize> {
        let mut count = 0;
        for (id, page) in self.find_dirty_pages(scope) {
            if let Some(mut page) = page.try_write() {
                if self.write(scope.share(), id, &mut page).await? {
                    count += 1;
                }
            }
        }
        Ok(count)
    }

    pub fn discard(&self, scope: &ScopeRef<T>) {
        for (_, page) in self.find_dirty_pages(scope) {
            if let Some(mut page) = page.try_write() {
                page.set_dirty(false);
            }
        }
    }

    fn find_dirty_pages(&self, scope: &ScopeRef<T>) -> Vec<(PageId, PageRef<T>)> {
        scope.page_map_r().read().unwrap().iter()
            .filter_map(|(_, page)| {
                let mut page = *page.iter().next().unwrap();
                let page = unsafe { page.as_mut() };
                page.lock_shared_data()
                    // If a page is write-locked this situation is interpreted as if the page was
                    // clean here but got dirty later, before this method returned.
                    // This interpretation allows skipping write-locked pages.
                    .filter(|data| data.try_read().map(|data| data.is_dirty()).unwrap_or(false))
                    .map(|data| (page.id(), data))
            })
            .collect::<Vec<_>>()
    }

    async fn read(&self, scope: &ScopeShare<T>, id: PageId, page: &mut Page) -> Result<()> {
        self.read_count.fetch_add(1, Ordering::Relaxed);
        scope.scope().read(id, page).await?;
        page.set_dirty(false);
        Ok(())
    }

    async fn write(&self, scope: &ScopeShare<T>, id: PageId, page: &mut Page) -> Result<bool> {
        Ok(if page.is_dirty() {
            self.read_count.fetch_add(1, Ordering::Relaxed);
            scope.scope().write(id, &page).await?;
            page.set_dirty(false);
            true
        } else {
            false
        })
    }

    #[cfg(test)]
    fn dump(&self, pages: bool) {
        println!("hit_count: {}", self.hit_count.load(Ordering::Relaxed));
        println!("miss_count: {}", self.miss_count.load(Ordering::Relaxed));
        println!("read_count: {}", self.read_count.load(Ordering::Relaxed));
        println!("write_count: {}", self.write_count.load(Ordering::Relaxed));
        self.inner.lock().dump(pages);
    }

    #[cfg(test)]
    fn dump_short(&self) {
        self.inner.lock().dump_short();
    }
}

#[cfg(test)]
mod test {
    use crate::util::for_test::alloc as test_alloc;
    use super::*;

    use itertools::Itertools;
    use lazy_static::lazy_static;
    use matches::matches;
    use rand::prelude::*;
    use static_assertions::{assert_impl_all, assert_not_impl_any};
    use std::collections::HashMap;
    use std::convert::TryFrom;
    use std::future::Future;
    use std::time::SystemTime;

    lazy_static! {
        static ref RAND_SEED: u64 = {
            let seed = SystemTime::now()
                .duration_since(SystemTime::UNIX_EPOCH).unwrap().as_secs();
            eprintln!("RNG seed initialized to 0x{:x}", seed);
            seed
        };
    }

    async fn check_alloc<F: Future>(f: F) -> F::Output {
        // Exclude lazy allocations.
        eprintln!();
        // test_block_on(async {});

        let t = test_alloc::track();
        let r = f.await;
        let stats = t.stats();
        assert_eq!(stats.allocs, stats.deallocs,
            "number of allocations != number of deallocations");

        r
    }

    fn check_alloc_blocking<F: Future>(f: F) -> F::Output {
        test_block_on(check_alloc(f))
    }

    fn new_rng() -> impl rand::Rng {
         new_seeded_rng(*RAND_SEED)
    }

    fn new_seeded_rng(seed: u64) -> impl rand::Rng {
        StdRng::seed_from_u64(seed)
    }

    #[test]
    fn check_alloc_ok() {
        check_alloc_blocking(async { dbg!(*RAND_SEED); });
    }

    #[test]
    #[should_panic(expected = "number of allocations != number of deallocations")]
    fn check_alloc_fail() {
        let v = check_alloc_blocking(async {
            Box::new(0)
        });
        dbg!(v);
    }

    #[derive(Clone, Copy, Debug, Eq, Ord, PartialEq, PartialOrd)]
    enum ScopeOp {
        Read {
            scope_id: u32,
            page_id: PageId,
        },
        Write {
            scope_id: u32,
            page_id: PageId,
            value: u32,
        },
        Drop(u32),
    }

    #[derive(Clone, Copy, Debug, Eq, PartialEq)]
    struct ScopeWrite(PageId);

    #[derive(Debug, Default)]
    struct ScopeOpCounts {
        reads: AtomicUsize,
        writes: AtomicUsize,
        drops: AtomicUsize,
    }

    #[derive(Clone, Debug)]
    enum ScopeOps {
        Full(Arc<Mutex<Vec<ScopeOp>>>),
        Count(Arc<ScopeOpCounts>),
    }

    impl ScopeOps {
        fn new_full() -> Self {
            ScopeOps::Full(Default::default())
        }

        fn new_count() -> Self {
            ScopeOps::Count(Default::default())
        }

        fn push(&self, op: ScopeOp) {
            match self {
                ScopeOps::Full(f) => f.lock().push(op),
                ScopeOps::Count(c) => {
                    match op {
                        ScopeOp::Read { .. } => &c.reads,
                        ScopeOp::Write { .. } => &c.writes,
                        ScopeOp::Drop(_) => &c.drops,
                    }.fetch_add(1, Ordering::SeqCst);
                }
            }
        }

        fn take(&self) -> Vec<ScopeOp> {
            match self {
                ScopeOps::Full(f) => f.lock().drain(..).collect(),
                ScopeOps::Count(_) => panic!(),
            }
        }

        fn read_count(&self) -> usize {
            match self {
                ScopeOps::Full(f) => f.lock().iter().filter(|op| matches!(op, ScopeOp::Read { .. })).count(),
                ScopeOps::Count(c) => c.reads.load(Ordering::SeqCst),
            }

        }

        fn write_count(&self) -> usize {
            match self {
                ScopeOps::Full(f) => f.lock().iter().filter(|op| matches!(op, ScopeOp::Write { .. })).count(),
                ScopeOps::Count(c) => c.writes.load(Ordering::SeqCst),
            }
        }
    }

    #[derive(Clone, Copy, Debug, Eq, PartialEq)]
    struct TestScopeError(ScopeOp);

    impl std::fmt::Display for TestScopeError {
        fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
            write!(f, "{:?}", self)
        }
    }

    impl std::error::Error for TestScopeError {}

    #[derive(Debug)]
    struct TestScope {
        id: u32,
        ops: ScopeOps,
        page_vals: Mutex<HashMap<PageId, u32>>,
        read_fails: bool,
        write_fails: bool,
    }

    impl TestScope {
        fn new(id: u32, ops: ScopeOps) -> Self {
            Self {
                id,
                ops: ops.clone(),
                page_vals: Default::default(),
                read_fails: false,
                write_fails: false,
            }
        }

        fn decode(buf: &[u8]) -> (PageId, u32) {
            let mut v1 = [0; 8];
            v1.copy_from_slice(&buf[..8]);
            let mut v2 = [0; 4];
            v2.copy_from_slice(&buf[8..12]);

            (PageId::try_from(u64::from_ne_bytes(v1)).unwrap(), u32::from_ne_bytes(v2))
        }

        fn encode(buf: &mut [u8], id: PageId, v: u32) {
            buf[..8].copy_from_slice(&(id as u64).to_ne_bytes()[..]);
            buf[8..12].copy_from_slice(&v.to_ne_bytes()[..]);
        }
    }

    #[async_trait::async_trait]
    impl Scope for TestScope {
        async fn read(&self, id: PageId, buf: &mut [u8]) -> Result<()> {
            let op = ScopeOp::Read { scope_id: self.id, page_id: id };
            self.ops.push(op);
            yield_now().await;
            if self.read_fails {
                return Err(Error::new(ErrorKind::Other, TestScopeError(op)));
            }
            for b in &mut *buf {
                *b = 0xff;
            }
            let v2 = *self.page_vals.lock().entry(id).or_insert(0);
            Self::encode(buf, id.into(), v2);
            Ok(())
        }

        async fn write(&self, id: PageId, buf: &[u8]) -> Result<()> {
            let (actual_id, value) = Self::decode(buf);
            assert_eq!(actual_id, id);
            let op = ScopeOp::Write { scope_id: self.id, page_id: id, value };
            self.ops.push(op);
            yield_now().await;
            if self.write_fails {
                return Err(Error::new(ErrorKind::Other, TestScopeError(op)));
            }
            assert!(self.page_vals.lock().insert(id, value).is_some());
            Ok(())
        }
    }

    impl Drop for TestScope {
        fn drop(&mut self) {
            self.ops.push(ScopeOp::Drop(self.id));
        }
    }

    type CacheHandle = super::CacheHandle<TestScope>;
    type ScopeHandle = super::ScopeHandle<TestScope>;

    assert_impl_all!(CacheHandle: Send, Sync);
    assert_impl_all!(ScopeHandle: Send);
    assert_not_impl_any!(ScopeHandle: Sync);
    assert_impl_all!(WeakScopeHandle<TestScope>: Send, Sync);

    fn new(max_resident: usize, max_non_resident: usize)
        -> (ScopeHandle, ScopeOps)
    {
        let ops = ScopeOps::new_full();
        let cache = new_ops(max_resident, max_non_resident, ops.clone());
        (cache, ops)
    }

    fn new_scope(max_resident: usize, max_non_resident: usize, scope: TestScope)
        -> ScopeHandle
    {
        new_cache(max_resident, max_non_resident).new_scope(scope)
    }

    fn new_ops(max_resident: usize, max_non_resident: usize, ops: ScopeOps)
        -> ScopeHandle
    {
        new_scope(max_resident, max_non_resident, TestScope::new(0, ops))
    }

    fn new_cache(max_resident: usize, max_non_resident: usize) -> CacheHandle {
        let r = super::new(Options {
            max_resident,
            max_non_resident,
            page_len: 8 + 4,
            page_align: 1,
        });
        assert_eq!(r.max_resident(), max_resident);
        assert_eq!(r.max_non_resident(), max_non_resident);
        r
    }

    async fn get(cache: &ScopeHandle, id: PageId) -> PageId {
        let page = cache.page(id).await.unwrap();
        let page = page.read().await;
        TestScope::decode(&page).0
    }

    async fn vget(cache: &ScopeHandle, id: PageId) {
        assert_eq!(get(cache, id).await, id);
    }

    async fn get_err(c: &ScopeHandle, id: PageId) -> TestScopeError {
        *c.page(id).await.unwrap_err().into_inner().unwrap().downcast_ref::<TestScopeError>().unwrap()
    }

    fn rd(page_id: PageId) -> ScopeOp {
        rdsc(0, page_id)
    }

    fn rdsc(scope_id: u32, page_id: PageId) -> ScopeOp {
        ScopeOp::Read { scope_id, page_id }
    }

    fn rds(page_ids: &[PageId]) -> Vec<ScopeOp> {
        page_ids.iter().copied().map(rd).collect()
    }

    fn wr(page_id: PageId, value: u32) -> ScopeOp {
        wrsc(0, page_id, value)
    }

    fn wrsc(scope_id: u32, page_id: PageId, value: u32) -> ScopeOp {
        ScopeOp::Write { scope_id, page_id, value }
    }

    fn drop_sc(scope_id: u32) -> ScopeOp {
        ScopeOp::Drop(scope_id)
    }

    #[test]
    fn one_page() {
        check_alloc_blocking(async {
            let (c, ops) = &new(1, 1);

            assert_eq!(c.cache().hit_count(), 0);
            assert_eq!(c.cache().miss_count(), 0);
            assert_eq!(c.cache().read_count(), 0);
            assert_eq!(c.cache().write_count(), 0);

            vget(c, 123_456).await;
            assert_eq!(c.cache().hit_count(), 0);
            assert_eq!(c.cache().miss_count(), 1);
            assert_eq!(c.cache().read_count(), 1);
            assert_eq!(c.cache().write_count(), 0);

            vget(c, 123_456).await;
            assert_eq!(c.cache().hit_count(), 1);
            assert_eq!(c.cache().miss_count(), 1);
            assert_eq!(c.cache().read_count(), 1);
            assert_eq!(c.cache().write_count(), 0);

            vget(c, 456_123).await;
            assert_eq!(c.cache().hit_count(), 1);
            assert_eq!(c.cache().miss_count(), 2);
            assert_eq!(c.cache().read_count(), 2);
            assert_eq!(c.cache().write_count(), 0);

            assert_eq!(ops.take(), vec![rd(123_456), rd(456_123)]);
        });
    }

    #[test]
    fn resident_to_hot() {
        check_alloc_blocking(async {
            let (c, ops) = &new(2, 0);

            vget(c, 1).await;
            vget(c, 1).await;
            vget(c, 2).await;

            vget(c, 3).await; // page 2 removed because page 1 goes hot -> test by cold hand

            vget(c, 1).await; // page 1 is still resident
            vget(c, 2).await; // page 2 will be re-read

            assert_eq!(ops.take(), rds(&[1, 2, 3, 2]));
        });
    }

    #[test]
    fn non_resident_to_hot() {
        check_alloc_blocking(async {
            let (c, ops) = &new(2, 2);

            for id in 1..=4 {
                vget(c, id).await;
            }
            // 1T0- 2T0- 3T0+ 4T0+

            vget(c, 2).await;
            // 4C0+ 2H0+

            vget(c, 3).await;
            // 2H0+ 3T0+

            vget(c, 4).await;
            // 2H0+ 3T0- 4T0+

            vget(c, 2).await;
            // 2H1+ 3T0- 4T0+

            assert_eq!(ops.take(), rds(&[1, 2, 3, 4, 2, 3, 4]));
        });
    }

    #[test]
    fn dirty_write() {
        check_alloc_blocking(async {
            const MAX_RES_COUNT: usize = 10;
            const ITERS: usize = 1000;
            const PAGE_ID_RANGE: usize = 100;

            let (c, ops) = &new(MAX_RES_COUNT, 10);
            let mut page_state = vec![0; PAGE_ID_RANGE];

            for i in 0..ITERS {
                let id = (i % PAGE_ID_RANGE) as PageId;
                let page = c.page(id).await.unwrap();
                let v = {
                    let page = page.read().await;
                    let v = TestScope::decode(&page);
                    assert_eq!(v, (id, page_state[id as usize]));
                    v.1
                };
                {
                    let mut page = page.write().await;
                    assert_eq!(page.is_dirty(), true);
                    let v = v.checked_add(1).unwrap();
                    TestScope::encode(&mut page, id, v);
                    page_state[id as usize] = v;
                }
            }

            assert_eq!(ops.read_count(), ITERS);
            assert_eq!(ops.write_count(), ITERS - MAX_RES_COUNT);

            c.flush().await.unwrap();

            assert_eq!(ops.read_count(), ITERS);
            assert_eq!(ops.write_count(), ITERS);
        });
    }

    async fn run_iter(
        max_resident: usize,
        max_non_resident: usize,
        ids: impl IntoIterator<Item=PageId>,
    ) -> u64 {
        check_alloc(async {
            let ops = ScopeOps::new_count();
            let c = &new_ops(max_resident, max_non_resident, ops.clone());

            let mut count = 0;
            for id in ids {
                vget(c, id).await;

                count += 1;
            }

            assert_eq!(c.cache().miss_count(), ops.read_count() as u64);
            assert_eq!(c.cache().hit_count() + c.cache().miss_count(), count);

            c.cache().hit_count()
        }).await
    }

    #[test]
    fn all_resident_seq() {
        test_block_on(async {
            const MAX_RES_COUNT: usize = 100;
            const ITER_COUNT: usize = 1000;
            let ids = (0..ITER_COUNT).map(|id| (id % MAX_RES_COUNT) as PageId);
            let hit_count = run_iter(MAX_RES_COUNT, 100, ids).await;
            assert_eq!(hit_count as usize, ITER_COUNT - MAX_RES_COUNT);
        });
    }

    async fn run_random(
        iters: u32,
        page_id_range: PageId,
        max_resident: usize,
        max_non_resident: usize,
    ) -> u64 {
        let mut rng = new_rng();
        let ids = (0..iters).map(|_| rng.gen_range(0, page_id_range));
        run_iter(max_resident, max_non_resident, ids).await
    }

    #[test]
    fn random_i1000_id100_res1_nres0() {
        test_block_on(run_random(1000, 100, 1, 0));
    }

    #[test]
    fn random_i1000_id100_res50_nres50() {
        test_block_on(run_random(1000, 100, 1, 0));
    }

    #[test]
    fn random_i1000_id100_res100_nres100() {
        test_block_on(async {
            const ITERS: u32 = 1000;
            const PAGE_ID_RANGE: PageId = 100;
            let hit_count = run_random(ITERS, PAGE_ID_RANGE, 100, 100).await;
            assert_eq!(hit_count, ITERS as u64 - PAGE_ID_RANGE as u64);
        });
    }

    async fn page_in_use_on_cache_drop(f: impl FnOnce()) {
        let (c, _) = new(1, 1);
        let page = c.page(0).await.unwrap();
        f();
        std::mem::drop(c);
        std::mem::drop(page);
    }

    #[test]
    #[should_panic(expected = "page 0 was locked during drop")]
    fn panics_if_page_in_use_on_cache_drop() {
        test_block_on(page_in_use_on_cache_drop(|| {}));
    }

    #[test]
    #[should_panic(expected = "!!!TEST PANIC!!!")]
    fn no_panic_override_if_page_in_use_on_cache_drop() {
        test_block_on(page_in_use_on_cache_drop(|| panic!("!!!TEST PANIC!!!")));
    }

    async fn page_dirty_on_cache_drop(f: impl FnOnce()) {
        let (c, _) = new(1, 1);
        {
            let page = c.page(0).await.unwrap();
            let page = page.write().await;
            assert!(page.is_dirty());
        }
        f();
        std::mem::drop(c);
    }

    #[test]
    #[should_panic(expected = "page 0 was dirty during drop")]
    fn panics_if_page_dirty_on_cache_drop() {
        test_block_on(page_dirty_on_cache_drop(|| {}));
    }

    #[test]
    #[should_panic(expected = "!!!TEST PANIC!!!")]
    fn no_panic_override_if_page_dirty_on_cache_drop() {
        test_block_on(page_dirty_on_cache_drop(|| panic!("!!!TEST PANIC!!!")));
    }

    async fn run_random_concurrent(
        concurrency: u32,
        iters: u32,
        page_id_range: PageId,
        max_resident: usize,
        max_non_resident: usize,
    ) {
        let ops = ScopeOps::new_count();
        let c = new_ops(max_resident, max_non_resident, ops.clone());
        let mut page_state = Vec::new();
        for _ in 0..page_id_range {
            page_state.push(AtomicU32::new(0));
        }
        let page_state = Arc::new(page_state);

        let mut rng = new_rng();
        let mut threads = Vec::new();
        for i in 0..concurrency {
            let mut rng = new_seeded_rng(rng.next_u64());
            let c = c.clone();
            let page_state = page_state.clone();
            let thread = std::thread::Builder::new()
                .name(format!(
                    "{mod_path}::random_concurrent_c{concurrency}_i{iters}_id{pid_range}_res{res}_nres{nres}#{i}",
                    mod_path=module_path!(),
                    concurrency=concurrency,
                    iters=iters,
                    pid_range=page_id_range,
                    res=max_resident,
                    nres=max_non_resident,
                    i=i));
            threads.push(thread.spawn(move || {
                for _ in 0..iters {
                    let id = rng.gen_range(0, page_id_range);
                    let page = test_block_on(c.page(id)).unwrap();
                    {
                        let page = test_block_on(page.read());
                        let v = TestScope::decode(&page);
                        assert_eq!(v, (id, page_state[id as usize].load(Ordering::SeqCst)),
                            "{:?}", &page[0] as *const _ as *const () as usize);
                    }
                    {
                        let mut page = test_block_on(page.write());
                        {
                            assert_eq!(page.is_dirty(), true);
                            let v = TestScope::decode(&page);
                            assert_eq!(v, (id, page_state[id as usize].load(Ordering::SeqCst)));
                            let v = v.1.checked_add(1).unwrap();
                            TestScope::encode(&mut page, id, v);
                            page_state[id as usize].store(v, Ordering::SeqCst);
                        }
                    }
                }
            }).unwrap());
        }

        for t in threads {
            t.join().unwrap();
        }

        assert!(c.cache().miss_count() >= ops.read_count() as u64);
        let total_gets = (concurrency * iters) as u64;
        assert_eq!(c.cache().hit_count() + c.cache().miss_count(), total_gets);

        if page_id_range as usize <= max_resident {
            assert_eq!(ops.read_count(), max_resident);
        }

        c.flush().await.unwrap();
    }

    #[test]
    fn random_concurrent_c32_i200_id100_res1_nres0() {
        test_block_on(run_random_concurrent(32, 200, 100, 1, 0));
    }

    #[test]
    fn random_concurrent_c32_i200_id100_res1_nres1() {
        test_block_on(run_random_concurrent(32, 200, 100, 1, 1));
    }

    #[test]
    fn random_concurrent_c64_i400_id100_res100_nres100() {
        test_block_on(run_random_concurrent(64, 400, 100, 100, 100));
    }

    #[test]
    fn cache_handle_size() {
        use std::mem::size_of;
        assert_eq!(size_of::<CacheHandle>(), size_of::<*const ()>());
    }

    #[test]
    fn scoped_cache_handle_size() {
        use std::mem::size_of;
        assert_eq!(size_of::<ScopeHandle>(), size_of::<*const ()>() * 7);
    }

    #[test]
    fn flushes() {
        test_block_on(async {
            let (c, ops) = new(10, 10);

            let _ = c.page(1).await.unwrap().write().await;
            let _ = c.page(2).await.unwrap().read().await;
            let _ = c.page(3).await.unwrap().write().await;

            assert_eq!(c.flush().await.unwrap(), 2);

            assert!(!c.page(1).await.unwrap().read().await.is_dirty());
            assert!(!c.page(2).await.unwrap().read().await.is_dirty());
            assert!(!c.page(3).await.unwrap().read().await.is_dirty());

            let ops = ops.take();
            assert_eq!(&ops[..3], &[rd(1), rd(2), rd(3)]);
            assert_eq!(&ops[3..].iter().copied().sorted().collect::<Vec<_>>(),
                &[wr(1, 0), wr(3, 0)]);
        });
    }

    #[test]
    fn flush_ignores_locked() {
        test_block_on(async {
            let (c, ops) = new(10, 10);

            let b1 = c.page(1).await.unwrap();
            let _ = b1.write().await;
            let b1 = b1.read().await;

            let b2 = c.page(2).await.unwrap();
            let mut b2 = b2.write().await;
            b2.set_dirty(false);

            let _ = c.page(3).await.unwrap().write().await;

            assert_eq!(c.flush().await.unwrap(), 1);
            assert!(!c.page(3).await.unwrap().read().await.is_dirty());
            assert_eq!(&ops.take(), &[rd(1), rd(2), rd(3), wr(3, 0)]);

            std::mem::drop(b1);
            std::mem::drop(b2);

            assert!(c.page(1).await.unwrap().read().await.is_dirty());
            assert!(!c.page(2).await.unwrap().read().await.is_dirty());

            assert_eq!(c.flush().await.unwrap(), 1);
            assert_eq!(&ops.take(), &[wr(1, 0)]);

            assert!(!c.page(1).await.unwrap().read().await.is_dirty());
            assert!(!c.page(2).await.unwrap().read().await.is_dirty());
        });
    }

    #[test]
    fn discards() {
        test_block_on(async {
            let (c, ops) = new(10, 10);

            let _ = c.page(1).await.unwrap().write().await;
            let _ = c.page(2).await.unwrap().read().await;
            let _ = c.page(3).await.unwrap().write().await;

            c.discard();

            assert!(!c.page(1).await.unwrap().read().await.is_dirty());
            assert!(!c.page(2).await.unwrap().read().await.is_dirty());
            assert!(!c.page(3).await.unwrap().read().await.is_dirty());

            assert_eq!(&ops.take(), &[rd(1), rd(2), rd(3)]);
        });
    }

    #[test]
    fn discard_ignores_locked() {
        test_block_on(async {
            let (c, ops) = new(10, 10);

            let b1 = c.page(1).await.unwrap();
            let _ = b1.write().await;
            let b1 = b1.read().await;

            let b2 = c.page(2).await.unwrap();
            let mut b2 = b2.write().await;
            b2.set_dirty(false);

            let _ = c.page(3).await.unwrap().write().await;

            c.discard();
            assert!(!c.page(3).await.unwrap().read().await.is_dirty());
            assert_eq!(&ops.take(), &[rd(1), rd(2), rd(3)]);

            std::mem::drop(b1);
            std::mem::drop(b2);

            assert!(c.page(1).await.unwrap().read().await.is_dirty());
            assert!(!c.page(2).await.unwrap().read().await.is_dirty());

            c.discard();
            assert_eq!(&ops.take(), &[]);

            assert!(!c.page(1).await.unwrap().read().await.is_dirty());
            assert!(!c.page(2).await.unwrap().read().await.is_dirty());
        });
    }

    #[test]
    fn read_error() {
        test_block_on(async {
            let ops = ScopeOps::new_full();
            let mut sc = TestScope::new(0, ops.clone());
            sc.read_fails = true;
            let c = &new_scope(10, 10, sc);

            assert_eq!(get_err(c, 1).await, TestScopeError(rd(1)));
            assert_eq!(get_err(c, 1).await, TestScopeError(rd(1)));
            assert_eq!(get_err(c, 2).await, TestScopeError(rd(2)));
            assert_eq!(get_err(c, 2).await, TestScopeError(rd(2)));

            assert_eq!(ops.take(), rds(&[1, 1, 2, 2]))
        });
    }

    #[test]
    fn write_error() {
        test_block_on(async {
            let ops = ScopeOps::new_full();
            let mut sc = TestScope::new(0, ops.clone());
            sc.write_fails = true;
            let c = &new_scope(1, 0, sc);

            c.page(1).await.unwrap().write().await.set_dirty(true);
            assert_eq!(get_err(c, 2).await, TestScopeError(wr(1, 0)));

            c.page(2).await.unwrap().write().await.set_dirty(true);
            assert_eq!(get_err(c, 3).await, TestScopeError(wr(2, 0)));

            assert_eq!(&ops.take(), &[rd(1), wr(1, 0), rd(2), wr(2, 0)]);
        });
    }

    #[test]
    fn multi_scope_dirty_write() {
        test_block_on(async {
            let c = new_cache(1, 1);
            let ops = ScopeOps::new_full();
            let c1 = &c.new_scope(TestScope::new(1, ops.clone()));
            let c2 = &c.new_scope(TestScope::new(2, ops.clone()));

            c1.page(1).await.unwrap().write().await.set_dirty(true);
            vget(c1, 1).await;

            c2.page(1).await.unwrap().write().await.set_dirty(true);
            vget(c2, 1).await;

            c1.page(1).await.unwrap().write().await.set_dirty(true);
            vget(c1, 1).await;

            assert_eq!(&ops.take(), &[
                rdsc(1, 1),
                wrsc(1, 1, 0), rdsc(2, 1),
                wrsc(2, 1, 0), rdsc(1, 1)]);

            c1.flush().await.unwrap();
            c2.flush().await.unwrap();

            assert_eq!(&ops.take(), &[wrsc(1, 1, 0)]);
        });
    }

    #[test]
    fn scope_drop() {
        test_block_on(async {
            let c = new_cache(1, 1);
            let ops = ScopeOps::new_full();
            let c1 = c.new_scope(TestScope::new(1, ops.clone()));
            let c2 = c.new_scope(TestScope::new(2, ops.clone()));

            c1.page(1).await.unwrap();
            c1.page(2).await.unwrap();

            std::mem::drop(c1);
            assert_eq!(&ops.take(), &[rdsc(1, 1), rdsc(1, 2)]);

            c2.page(10).await.unwrap();
            assert_eq!(&ops.take(), &[rdsc(2, 10)]);

            c2.page(20).await.unwrap();
            assert_eq!(&ops.take(), &[drop_sc(1), rdsc(2, 20)]);
        });
    }

    #[test]
    fn dump() {
        new_cache(1, 2).0.dump(true);
    }

    #[test]
    fn dump_short() {
        new_cache(1, 2).0.dump_short();
    }
}