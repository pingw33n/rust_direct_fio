#[cfg(target_os = "macos")]
mod macos;

#[cfg(unix)]
mod unix;

#[cfg(windows)]
mod windows;

#[cfg(target_os = "macos")]
pub use macos::*;

#[cfg(unix)]
pub use unix::*;

#[cfg(windows)]
pub use windows::*;

#[cfg(test)]
mod test {
    use super::*;
    use std::fs::{File, OpenOptions};
    use tempfile::NamedTempFile;

    #[test]
    fn file_id() {
        let mut tmpf = NamedTempFile::new().unwrap();
        tmpf.as_file_mut().set_len(42).unwrap();

        let id_len = FileId::of(&File::open(&tmpf).unwrap()).unwrap();
        assert_eq!(id_len.1, 42);

        let id_len2 = FileId::of(&File::open(&tmpf).unwrap()).unwrap();
        assert_eq!(id_len, id_len2);

        let mut path = tmpf.path().to_path_buf();
        let filename = path.file_name().unwrap().to_os_string();
        path.pop();
        path.push(".");
        path.push(filename);
        let id_len3 = FileId::of(&File::open(&path).unwrap()).unwrap();
        assert_eq!(id_len, id_len3);
    }

    #[test]
    fn open_() {
        let tmpf = NamedTempFile::new().unwrap();
        open(&tmpf.path(), &OpenOptions::new().read(true)).unwrap();
    }
}