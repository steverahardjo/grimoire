use std::fmt;
use std::error::Error;

#[derive(Debug)]
pub enum DiskError {
    Io(std::io::Error),
    InvalidFile,
    PageNotFound(i32),
    Other(String),
}

// Allow automatic conversion from std::io::Error
impl From<std::io::Error> for DiskError {
    fn from(err: std::io::Error) -> Self {
        DiskError::Io(err)
    }
}

// Implement Display for pretty-printing
impl fmt::Display for DiskError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            DiskError::Io(e) => write!(f, "I/O Error: {}", e),
            DiskError::InvalidFile => write!(f, "Invalid file"),
            DiskError::PageNotFound(id) => write!(f, "Page not found: {}", id),
            DiskError::Other(msg) => write!(f, "Disk error: {}", msg),
        }
    }
}

// Implement std::error::Error for compatibility with other error handling
impl Error for DiskError {
    fn source(&self) -> Option<&(dyn Error + 'static)> {
        match self {
            DiskError::Io(e) => Some(e),
            _ => None,
        }
    }
}

