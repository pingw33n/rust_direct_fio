use std::future::Future;
use std::marker::Unpin;
use std::pin::Pin;
use std::task::*;

pub use tokio::{
    sync::{
        RwLock as AsyncRwLock,
        RwLockReadGuard as AsyncRwLockReadGuard,
        RwLockWriteGuard as AsyncRwLockWriteGuard,
    },
    task::spawn_blocking,
};

#[cfg(test)]
pub use tokio::task::yield_now;

#[cfg(test)]
pub fn test_block_on<F: Future>(f: F) -> F::Output {
    lazy_static::lazy_static! {
        static ref RT: tokio::runtime::Runtime = tokio::runtime::Builder::new()
            .threaded_scheduler()
            .build()
            .unwrap();
    }
    RT.enter(move || tokio::runtime::Handle::current().block_on(f))
}

/// Unsafe because if aliasing may occur if the resulting future is forgotten.
/// Panics if the resulting future is not driven to completion.
pub async unsafe fn spawn_blocking_scoped<'a, F, T>(f: F) -> T
where
    F: FnOnce() -> T + Send + 'a,
    T: Send + 'static,
{
    struct Fut<F, T>(Option<F>)
        where F: Future<Output=T>;

    impl<F, T> Future for Fut<F, T>
    where F: Future<Output=T> + Unpin
    {
        type Output = T;

        fn poll(mut self: Pin<&mut Self>, cx: &mut Context) -> Poll<Self::Output> {
            let r = Pin::new(self.0.as_mut().unwrap()).poll(cx);
            if r.is_ready() {
                self.0 = None;
            }
            r
        }
    }

    impl<F, T> Drop for Fut<F, T>
    where F: Future<Output=T>
    {
        fn drop(&mut self) {
            if let Some(f) = self.0.take() {
                tokio::runtime::Handle::current().block_on(f);
            }
        }
    }

    let f: Box<dyn FnOnce() -> T + Send> = Box::new(f);
    let f: Box<dyn FnOnce() -> T + Send + 'static> = std::mem::transmute(f);
    let r = Fut(Some(spawn_blocking(f))).await;

    r.unwrap()
}
