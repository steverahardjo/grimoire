use std::fmt;
use std::error::Error;

#[derive(Debug)]
pub enum DiskError {
    IoError(std::io::Error),
    PageNotFound(i32),
}
