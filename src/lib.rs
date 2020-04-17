#![deny(dead_code)]
#![deny(non_snake_case)]
#![deny(unused_imports)]
#![deny(unused_must_use)]

mod async_rt;
mod cache;
mod file;
mod util;

pub use std::fs::OpenOptions;
pub use cache::PageId;
pub use file::{File, FileSync};
pub use file::cursor::FileCursor;
pub use file::system::{FileSystem, Options};
