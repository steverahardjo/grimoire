
use std::{
    collections::HashMap,
    fs::{File, OpenOptions},
    io::{Read, Seek, SeekFrom, Write},
    path::{Path, PathBuf},
    sync::{Arc, Mutex},
};
use crate::common::types::{FrameId, PageId};


use std::fs;

/// GRIMOIRE page size in bytes.
pub const GRIMOIRE_PAGE_SIZE: usize = 4096;

pub struct DiskManager {
    db_file_name: PathBuf,
    log_file_name: PathBuf,
    db_io: Mutex<File>,
    log_io: Mutex<File>,

    pages: Mutex<HashMap<i32, u64>>,
    free_slots: Mutex<Vec<u64>>,

    num_writes: Mutex<i32>,
    num_deletes: Mutex<i32>,
    num_flushes: Mutex<i32>,
    flush_log: Mutex<bool>,

    page_capacity: Mutex<usize>

}

impl DiskManager{
    //make a new DiskManager when an engine is started
    pub fn new(db_file: &Path) -> Result<Self, DiskError> {
        let db_file_name = db_file.to_path_buf();

        let log_file_name = db_file_name
            .file_stem()
            .map(|stem| PathBuf::from(format!("{}.log", stem.to_string_lossy())))
            .unwrap_or_else(|| PathBuf::from("bustub.log"));

        let db_io = OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .open(&db_file_name)?; // `?` converts std::io::Error → DiskError via From impl

        let log_io = OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .open(&log_file_name)?; // Note: log file name here

        let initial_capacity = 128;
        db_io.set_len(((initial_capacity + 1) * BUSTUB_PAGE_SIZE) as u64)?;

        // Initialize all fields
        Ok(Self {
            db_file_name,
            log_file_name,

            db_io: Mutex::new(db_io),
            log_io: Mutex::new(log_io),

            pages: Mutex::new(HashMap::new()),
            free_slots: Mutex::new(Vec::new()),

            num_writes: Mutex::new(0),
            num_deletes: Mutex::new(0),
            num_flushes: Mutex::new(0),
            flush_log: Mutex::new(false),

            page_capacity: Mutex::new(initial_capacity),
        })
    }
    pub fn WritePage(&self, page_id: PageId, page_data: &[u8]) -> Result<(), DiskError> {
        // Implementation here
        Ok(())
    }

    pub fn Shutdown(&self){
        db_io.close();
        log_io.close();
    }

    pub fn ReadPage(&self, page_id:PageId, page_data:&[u8]){
        Ok(())
    }

    pub fn DeletePage(&self, page_id:PageId, page_data:&[u8]){
        Ok(())
    }
}




