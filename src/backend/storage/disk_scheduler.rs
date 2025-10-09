// src/storage/disk_scheduler.rs

//! DiskScheduler module
//! This module used to schedule write and read requests to DiskManager
//! This can be used by other components such as BufferPoolmanager to queue disk requests
//! represented by the DiskRequest struct.
//! 
//! Translated from BusTub C++ skeleton into Rust.
//! 
//! See https://github.com/cmu-db/bustub/blob/master/src/storage/disk/disk_scheduler.cpp
//! DiskScheduler module
//! Handles queued disk I/O requests for the DiskManager.

use std::{
    collections::VecDeque,
    path::{Path},
    sync::Arc,
};

use tokio::{
    sync::{RwLock, Semaphore, oneshot},
};

use tokio::time::sleep;

use crate::common::{errors::DiskError, types::PageId};
use crate::backend::storage::disk_manager::DiskManager;

/// A request to read or write a page from disk.
pub struct DiskRequest {
    pub is_write: bool,
    pub data: Vec<u8>,
    pub page_id: PageId,
    pub callback: oneshot::Sender<Result<Vec<u8>, DiskError>>,
}

/// The DiskScheduler queues DiskRequests and executes them in order.
pub struct DiskScheduler {
    manager: Arc<DiskManager>,
    requests_queue: Arc<RwLock<VecDeque<DiskRequest>>>,
    io_semaphore: Arc<Semaphore>,
}

impl DiskScheduler {
    pub fn new(manager: Arc<DiskManager>) -> Result<Self, DiskError> {
        Ok(Self {
            manager,
            requests_queue: Arc::new(RwLock::new(VecDeque::new())),
            io_semaphore: Arc::new(Semaphore::new(10)),
        })
    }

    /// Enqueue a new disk request.
    pub async fn enqueue(&self, req: DiskRequest) {
        let mut queue = self.requests_queue.write().await;
        queue.push_back(req);
    }

    /// Worker loop (background thread).
    pub fn start_worker_thread(self: Arc<Self>, thread_num: usize, count_load: usize) {
        std::thread::spawn(move || {
            let runtime = tokio::runtime::Builder::new_multi_thread()
                .worker_threads(thread_num)
                .enable_all()
                .build()
                .expect("Failed to build Tokio runtime");

            runtime.block_on(async move {
                loop {
                    // Schedule a batch of work
                    if let Err(e) = self.schedule(count_load).await {
                        eprintln!("DiskScheduler error: {:?}", e);
                    }

                    // Small delay to avoid busy looping if queue is empty
                    tokio::time::sleep(std::time::Duration::from_millis(50)).await;
                }
            });
        });
    }

    /// Process up to `count` queued requests.
    pub async fn schedule(&self, count: usize) -> Result<(), DiskError> {
        let mut queue = self.requests_queue.write().await;
        let count = count.min(queue.len());
        let reqs: Vec<DiskRequest> = queue.drain(0..count).collect();
        drop(queue);

        // Spawn tasks concurrently with semaphore limiting concurrent I/O
        let mut handles = vec![];

        for mut req in reqs {
            let manager = self.manager.clone();
            let semaphore = self.io_semaphore.clone();
            
            let handle = tokio::spawn(async move {
                // Acquire semaphore permit for I/O operation
                let _permit = semaphore.acquire().await.expect("Semaphore closed");
                
                let result = if req.is_write {
                    match manager.write_page(req.page_id, &req.data).await {
                        Ok(_) => Ok(req.data),
                        Err(e) => Err(e),
                    }
                } else {
                    match manager.read_page(req.page_id, &mut req.data).await {
                        Ok(_) => Ok(req.data),
                        Err(e) => Err(e),
                    }
                };
                
                let _ = req.callback.send(result);
            });

            handles.push(handle);
        }

        // Wait for all tasks to complete
        for handle in handles {
            let _ = handle.await;
        }

        Ok(())
    }

    pub fn deallocate_page(&self, _delete_page_id: PageId) -> Option<PageId> {
        None
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::tempdir;
    use tokio::sync::oneshot;
    use std::sync::Arc;

    // Helper to create a basic DiskManager
    async fn make_disk_manager(path: &Path) -> Arc<DiskManager> {
        Arc::new(DiskManager::new(path).await.unwrap())
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn test_worker_schedule_mixed_requests() {
        // Temporary directory and file
        let dir = tempdir().unwrap();
        let file_path = dir.path().join("test.db");

        // Create DiskManager
        let manager = make_disk_manager(&file_path).await;

        // Create scheduler
        let scheduler = Arc::new(DiskScheduler::new(manager.clone()).unwrap());

        // Prepare sample data
        let page_id_1: i32 = 1;
        let page_id_2: i32 = 2;
        let data_write_1 = vec![42u8; 4096];
        let data_write_2 = vec![77u8; 4096];
        let data_read_1 = vec![0u8; 4096];
        let data_read_2 = vec![0u8; 4096];

        // --- Write requests ---
        let (tx1, rx1) = oneshot::channel();
        scheduler.enqueue(DiskRequest {
            is_write: true,
            data: data_write_1.clone(),
            page_id: page_id_1,
            callback: tx1,
        }).await;

        let (tx2, rx2) = oneshot::channel();
        scheduler.enqueue(DiskRequest {
            is_write: true,
            data: data_write_2.clone(),
            page_id: page_id_2,
            callback: tx2,
        }).await;

        // --- Read requests ---
        let (tx3, rx3) = oneshot::channel();
        scheduler.enqueue(DiskRequest {
            is_write: false,
            data: data_read_1.clone(),
            page_id: page_id_1,
            callback: tx3,
        }).await;

        let (tx4, rx4) = oneshot::channel();
        scheduler.enqueue(DiskRequest {
            is_write: false,
            data: data_read_2.clone(),
            page_id: page_id_2,
            callback: tx4,
        }).await;

        // Spawn background worker
        let scheduler_clone = Arc::clone(&scheduler);
        tokio::spawn(async move {
            scheduler_clone.schedule(10).await.unwrap();
        });

        // Wait for all callbacks to confirm completion
        let result1 = rx1.await.unwrap().unwrap();
        let result2 = rx2.await.unwrap().unwrap();
        let result3 = rx3.await.unwrap().unwrap();
        let result4 = rx4.await.unwrap().unwrap();

        // --- Verify content correctness ---
        assert_eq!(result3, data_write_1);
        assert_eq!(result4, data_write_2);

        let mut buf = vec![0u8; 4096];
        manager.read_page(page_id_1, &mut buf).await.unwrap();
        assert_eq!(buf, data_write_1);

        manager.read_page(page_id_2, &mut buf).await.unwrap();
        assert_eq!(buf, data_write_2);
    }
}