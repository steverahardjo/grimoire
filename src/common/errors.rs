use std::fs;

pub enum DiskError {
    Io(std::io::Error),
    InvalidFile,
}

impl From<std::io::Error> for DiskError {
    fn from(err: std::io::Error) -> Self {
        DiskError::Io(err)
    }
}
