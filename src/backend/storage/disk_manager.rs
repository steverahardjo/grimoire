//! Disk Manager Module inspired by CMU's BusStub Database Engine
//! This provide interaction between Buffer and Files to:
//! - Read
//! - Write
//! - Delete
//! this module are triggered and organized by disk_scheduler

use std::{
    collections::HashMap,
    fs::{File, OpenOptions},
    io::{Read, Seek, SeekFrom, Write},
    path::{Path, PathBuf},
    sync::{Mutex},
};

use crate::common::types::{PageId};

pub const GRIMOIRE_PAGE_SIZE: usize = 4096;

#[derive(Debug)]
pub enum DiskError {
    IoError(std::io::Error),
    PageNotFound(PageId),
}

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
    buffer_used: Mutex<Vec<u8>>,
    page_capacity: Mutex<usize>,
}

impl DiskManager {
    pub fn new(db_file: &Path) -> Result<Self, DiskError> {
        let db_file_name = db_file.to_path_buf();
        let log_file_name = db_file_name
            .file_stem()
            .map(|stem| PathBuf::from(format!("{}.log", stem.to_string_lossy())))
            .unwrap_or_else(|| PathBuf::from("grimoire.log"));

        let db_io = OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .open(&db_file_name)
            .map_err(DiskError::IoError)?;

        let log_io = OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .open(&log_file_name)
            .map_err(DiskError::IoError)?;

        let initial_capacity = 128;
        db_io.set_len(((initial_capacity + 1) * GRIMOIRE_PAGE_SIZE) as u64)
            .map_err(DiskError::IoError)?;

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
            buffer_used: Mutex::new(Vec::new()),
            page_capacity: Mutex::new(initial_capacity),
        })
    }
    /* get the offset, if not there then input an offset
    Open db, jump to the db file, write it, and flush your dbn_io
    Args:
    - page_id:u8
     */
    pub fn write_page(&self, page_id: PageId, page_data: &[u8]) -> Result<(), DiskError> {
        if page_data.len() != GRIMOIRE_PAGE_SIZE {
            panic!("page_data must be exactly {} bytes", GRIMOIRE_PAGE_SIZE);
        }

        let mut pages = self.pages.lock().unwrap();
        let offset = *pages.entry(page_id).or_insert_with(|| self.allocate_page());

        let mut db = self.db_io.lock().unwrap();
        let mut num_writes = self.num_writes.lock().unwrap();

        db.seek(SeekFrom::Start(offset))
            .map_err(DiskError::IoError)?;
        db.write_all(page_data).map_err(DiskError::IoError)?;
        db.flush().map_err(DiskError::IoError)?;

        *num_writes += 1;
        pages.insert(page_id, offset);

        Ok(())
    }
    /*
    Read through the pages through offset, access db.file
    and load it into your program into a buffer page_data which can be accessed
    by disk scheduler()
    Aegs:
    - page_id:u8 
    - page_data: [u8]
     */
    pub fn read_page(&self, page_id: PageId, page_data: &mut [u8]) -> Result<(), DiskError> {
        if page_data.len() != GRIMOIRE_PAGE_SIZE {
            panic!("page_data must be exactly {} bytes", GRIMOIRE_PAGE_SIZE);
        }

        let pages = self.pages.lock().unwrap();
        let offset = pages
            .get(&page_id)
            .ok_or(DiskError::PageNotFound(page_id))?;

        let mut db = self.db_io.lock().unwrap();
        db.seek(SeekFrom::Start(*offset))
            .map_err(DiskError::IoError)?;
        db.read_exact(page_data).map_err(DiskError::IoError)?;

        Ok(())
    }

    pub fn delete_page(&self, page_id: PageId) -> Result<(), DiskError> {
        let mut pages = self.pages.lock().unwrap();
        if let Some(offset) = pages.remove(&page_id) {
            let mut free_slots = self.free_slots.lock().unwrap();
            free_slots.push(offset);

            let mut num_deletes = self.num_deletes.lock().unwrap();
            *num_deletes += 1;

            Ok(())
        } else {
            Err(DiskError::PageNotFound(page_id))
        }
    }

    pub fn write_log(&self, log_data: &[u8]) -> Result<(), DiskError> {
        let mut buffer = self.buffer_used.lock().unwrap();
        if *buffer != log_data {
            let mut log_file = self.log_io.lock().unwrap();
            log_file.write_all(log_data).map_err(DiskError::IoError)?;
            log_file.sync_all().map_err(DiskError::IoError)?;
            buffer.clear();
            buffer.extend_from_slice(log_data);
        }
        Ok(())
    }

    pub fn get_num_writes(&self) -> i32 {
        *self.num_writes.lock().unwrap()
    }

    pub fn get_num_deletes(&self) -> i32 {
        *self.num_deletes.lock().unwrap()
    }

    pub fn get_num_flushes(&self) -> i32 {
        *self.num_flushes.lock().unwrap()
    }

    pub fn get_flush_state(&self) -> bool {
        *self.flush_log.lock().unwrap()
    }

    fn allocate_page(&self) -> u64 {
        if let Some(offset) = self.free_slots.lock().unwrap().pop() {
            return offset;
        }
        let pages = self.pages.lock().unwrap();
        (pages.len() as u64) * GRIMOIRE_PAGE_SIZE as u64
    }

    pub fn get_file_size(&self, file_name: &str) -> i64 {
        match std::fs::metadata(file_name) {
            Ok(meta) => meta.len() as i64,
            Err(_) => -1,
        }
    }
}


#[cfg(test)]
mod tests {
    use super::*;
    use std::fs;
    use tempfile::tempdir;

    #[test]
    fn test_write_and_read_page() {
        let dir = tempdir().unwrap();
        let db_path = dir.path().join("test.db");
        let dm = DiskManager::new(&db_path).expect("failed to create disk manager");
        let page_id: PageId = 1;
        let mut page_data = vec![0u8; GRIMOIRE_PAGE_SIZE];
        page_data[0..4].copy_from_slice(&42u32.to_le_bytes());
        dm.write_page(page_id, &page_data).expect("write failed");

        let mut read_buf = vec![0u8; GRIMOIRE_PAGE_SIZE];
        {
            let mut db = dm.db_io.lock().unwrap();
            db.seek(SeekFrom::Start(0)).unwrap();
            db.read_exact(&mut read_buf).unwrap();
        }

        let read_value = u32::from_le_bytes(read_buf[0..4].try_into().unwrap());
        assert_eq!(read_value, 42);

        let num_writes = *dm.num_writes.lock().unwrap();
        assert_eq!(num_writes, 1);

        dir.close().unwrap();
    }

    #[test]
    fn test_delete_page() {
        let dir = tempdir().unwrap();
        let db_path = dir.path().join("test_delete.db");
        let dm = DiskManager::new(&db_path).unwrap();

        let page_id = 5;
        let dummy_data = vec![1u8; GRIMOIRE_PAGE_SIZE];
        dm.write_page(page_id, &dummy_data).unwrap();

        // Delete the page
        dm.DeletePage(page_id, &dummy_data).unwrap();

        // Check free_slots got populated
        let free_slots = dm.free_slots.lock().unwrap();
        assert!(!free_slots.is_empty());

        dir.close().unwrap();
    }
}

