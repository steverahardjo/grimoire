//! Disk Manager Module inspired by CMU's BusStub Database Engine
//! This provide interaction between Buffer and Files to:
//! - Read
//! - Write
//! - Delete
//! this module are triggered and organized by disk_scheduler

//! Async Disk Manager using Tokio
//! Provides non-blocking I/O operations for page management

use std::{
    collections::HashMap,
    path::{Path, PathBuf},
    sync::Arc,
};
use tokio::{
    fs::{File, OpenOptions},
    io::{AsyncReadExt, AsyncSeekExt, AsyncWriteExt},
    sync::{RwLock, Semaphore},
};

pub const GRIMOIRE_PAGE_SIZE: usize = 4096;

#[derive(Debug)]
pub enum DiskError {
    IoError(std::io::Error),
    PageNotFound(i32),
}

pub struct DiskManager {
    db_file_path: PathBuf,
    log_file_path: PathBuf,
    
    // Page mapping: page_id -> offset
    pages: Arc<RwLock<HashMap<i32, u64>>>,
    
    // Free slots for reuse
    free_slots: Arc<RwLock<Vec<u64>>>,
    
    // Capacity tracking
    page_capacity: Arc<RwLock<usize>>,
    
    // Statistics
    stats: Arc<RwLock<DiskStats>>,
    
    // Semaphore to limit concurrent I/O operations
    io_semaphore: Arc<Semaphore>,
}

#[derive(Default)]
struct DiskStats {
    num_writes: u64,
    num_reads: u64,
    num_deletes: u64,
    num_flushes: u64,
}

impl DiskManager {
    pub async fn new(db_file: &Path) -> Result<Self, DiskError> {
        let db_file_path = db_file.to_path_buf();
        let log_file_path = db_file_path
            .file_stem()
            .map(|stem| PathBuf::from(format!("{}.log", stem.to_string_lossy())))
            .unwrap_or_else(|| PathBuf::from("grimoire.log"));

        // Create db file
        let db_file = OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .open(&db_file_path)
            .await
            .map_err(DiskError::IoError)?;

        // Create log file
        OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .open(&log_file_path)
            .await
            .map_err(DiskError::IoError)?;

        let initial_capacity = 128;
        db_file
            .set_len(((initial_capacity + 1) * GRIMOIRE_PAGE_SIZE) as u64)
            .await
            .map_err(DiskError::IoError)?;

        Ok(Self {
            db_file_path,
            log_file_path,
            pages: Arc::new(RwLock::new(HashMap::new())),
            free_slots: Arc::new(RwLock::new(Vec::new())),
            page_capacity: Arc::new(RwLock::new(initial_capacity)),
            stats: Arc::new(RwLock::new(DiskStats::default())),
            io_semaphore: Arc::new(Semaphore::new(10)), // Limit to 10 concurrent I/O ops
        })
    }

    /// Write a page to disk asynchronously
    pub async fn write_page(&self, page_id: i32, page_data: &[u8]) -> Result<(), DiskError> {
        if page_data.len() != GRIMOIRE_PAGE_SIZE {
            panic!("page_data must be exactly {} bytes", GRIMOIRE_PAGE_SIZE);
        }

        // Acquire semaphore permit to limit concurrent I/O
        let _permit = self.io_semaphore.acquire().await.unwrap();

        // Get or allocate offset
        let offset = {
            let pages = self.pages.read().await;
            if let Some(&offset) = pages.get(&page_id) {
                Some(offset)
            } else {
                None
            }
        };

        let offset = match offset {
            Some(o) => o,
            None => self.allocate_page(page_id).await,
        };

        // Open file and write
        let mut file = OpenOptions::new()
            .write(true)
            .open(&self.db_file_path)
            .await
            .map_err(DiskError::IoError)?;

        file.seek(std::io::SeekFrom::Start(offset))
            .await
            .map_err(DiskError::IoError)?;
        
        file.write_all(page_data)
            .await
            .map_err(DiskError::IoError)?;
        
        file.sync_all()
            .await
            .map_err(DiskError::IoError)?;

        // Update stats
        let mut stats = self.stats.write().await;
        stats.num_writes += 1;

        Ok(())
    }

    /// Read a page from disk asynchronously
    pub async fn read_page(&self, page_id: i32, page_data: &mut [u8]) -> Result<(), DiskError> {
        if page_data.len() != GRIMOIRE_PAGE_SIZE {
            panic!("page_data must be exactly {} bytes", GRIMOIRE_PAGE_SIZE);
        }

        let _permit = self.io_semaphore.acquire().await.unwrap();

        // Get offset
        let offset = {
            let pages = self.pages.read().await;
            pages
                .get(&page_id)
                .copied()
                .ok_or(DiskError::PageNotFound(page_id))?
        };

        // Open file and read
        let mut file = File::open(&self.db_file_path)
            .await
            .map_err(DiskError::IoError)?;

        file.seek(std::io::SeekFrom::Start(offset))
            .await
            .map_err(DiskError::IoError)?;
        
        file.read_exact(page_data)
            .await
            .map_err(DiskError::IoError)?;

        // Update stats
        let mut stats = self.stats.write().await;
        stats.num_reads += 1;

        Ok(())
    }

    /// Delete a page (mark slot as free)
    pub async fn delete_page(&self, page_id: i32) -> Result<(), DiskError> {
        let mut pages = self.pages.write().await;
        
        if let Some(offset) = pages.remove(&page_id) {
            drop(pages); // Release write lock before acquiring next lock
            
            let mut free_slots = self.free_slots.write().await;
            free_slots.push(offset);
            drop(free_slots);

            let mut stats = self.stats.write().await;
            stats.num_deletes += 1;

            Ok(())
        } else {
            Err(DiskError::PageNotFound(page_id))
        }
    }

    /// Write log data asynchronously
    pub async fn write_log(&self, log_data: &[u8]) -> Result<(), DiskError> {
        let mut file = OpenOptions::new()
            .append(true)
            .open(&self.log_file_path)
            .await
            .map_err(DiskError::IoError)?;

        file.write_all(log_data)
            .await
            .map_err(DiskError::IoError)?;
        
        file.sync_all()
            .await
            .map_err(DiskError::IoError)?;

        Ok(())
    }

    /// Allocate a new page offset
    async fn allocate_page(&self, page_id: i32) -> u64 {
        // Check free slots first
        {
            let mut free_slots = self.free_slots.write().await;
            if let Some(offset) = free_slots.pop() {
                let mut pages = self.pages.write().await;
                pages.insert(page_id, offset);
                return offset;
            }
        }

        // Need to allocate new page
        let mut pages = self.pages.write().await;
        let mut capacity = self.page_capacity.write().await;

        // Check if expansion needed
        if pages.len() >= *capacity {
            let new_capacity = *capacity * 2;
            *capacity = new_capacity;

            // Expand file (do this after releasing locks would be better,
            // but for simplicity we keep it here)
            let new_size = (new_capacity + 1) as u64 * GRIMOIRE_PAGE_SIZE as u64;
            
            // Open temporarily to resize
            if let Ok(file) = OpenOptions::new()
                .write(true)
                .open(&self.db_file_path)
                .await
            {
                let _ = file.set_len(new_size).await;
            }
        }

        // Calculate new offset
        let offset = pages.len() as u64 * GRIMOIRE_PAGE_SIZE as u64;
        pages.insert(page_id, offset);

        offset
    }

    // Statistics methods
    pub async fn get_num_writes(&self) -> u64 {
        self.stats.read().await.num_writes
    }

    pub async fn get_num_reads(&self) -> u64 {
        self.stats.read().await.num_reads
    }

    pub async fn get_num_deletes(&self) -> u64 {
        self.stats.read().await.num_deletes
    }
}

// Example usage and tests
#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::tempdir;

    #[tokio::test]
    async fn test_write_and_read_page() {
        let dir = tempdir().unwrap();
        let db_path = dir.path().join("test.db");
        let dm = DiskManager::new(&db_path).await.unwrap();

        let page_id = 1;
        let mut page_data = vec![0u8; GRIMOIRE_PAGE_SIZE];
        page_data[0..11].copy_from_slice(b"Hello World");

        // Write page
        dm.write_page(page_id, &page_data).await.unwrap();

        // Read it back
        let mut read_buf = vec![0u8; GRIMOIRE_PAGE_SIZE];
        dm.read_page(page_id, &mut read_buf).await.unwrap();

        assert_eq!(&read_buf[0..11], b"Hello World");
        assert_eq!(dm.get_num_writes().await, 1);
        assert_eq!(dm.get_num_reads().await, 1);
    }

    #[tokio::test]
    async fn test_concurrent_writes() {
        let dir = tempdir().unwrap();
        let db_path = dir.path().join("test_concurrent.db");
        let dm = Arc::new(DiskManager::new(&db_path).await.unwrap());

        let mut handles = vec![];

        // Spawn 100 concurrent write operations
        for i in 0i32..100i32 {
            let dm_clone = Arc::clone(&dm);
            let handle = tokio::spawn(async move {
                let mut page_data = vec![0u8; GRIMOIRE_PAGE_SIZE];
                page_data[0..4].copy_from_slice(&i.to_le_bytes());
                dm_clone.write_page(i, &page_data).await.unwrap();
            });
            handles.push(handle);
        }

        // Wait for all writes to complete
        for handle in handles {
            handle.await.unwrap();
        }

        // Verify all pages were written
        assert_eq!(dm.get_num_writes().await, 100);
    }

    #[tokio::test]
    async fn test_delete_and_reuse() {
        let dir = tempdir().unwrap();
        let db_path = dir.path().join("test_delete.db");
        let dm = DiskManager::new(&db_path).await.unwrap();

        let page_data = vec![42u8; GRIMOIRE_PAGE_SIZE];

        // Write page 1
        dm.write_page(1, &page_data).await.unwrap();
        
        // Delete page 1
        dm.delete_page(1).await.unwrap();

        // Write page 2 (should reuse deleted slot)
        dm.write_page(2, &page_data).await.unwrap();

        assert_eq!(dm.get_num_deletes().await, 1);
    }
}