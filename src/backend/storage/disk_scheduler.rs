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


/*

use std::{
    collections::VecDeque,
    sync::{Arc, Mutex},
    thread,
    time::Duration,
};
use anyhow::Result;
use tokio::sync::oneshot;

use crate::common::types::PageId;
use crate::backend::storage::disk_manager::DiskManager;

/// A request to read or write a page from disk.
pub struct DiskRequest {
    pub is_write: bool,
    pub data: Vec<u8>,
    pub page_id: PageId,
    pub callback: oneshot::Sender<bool>,
}

/// The DiskScheduler queues DiskRequests and executes them in order.
pub struct DiskScheduler {
    manager: Arc<DiskManager>,
    requests_queue: Mutex<VecDeque<DiskRequest>>,
}

impl DiskScheduler {
    pub fn new(manager: Arc<DiskManager>) -> Self {
        Self {
            manager,
            requests_queue: Mutex::new(VecDeque::new()),
        }
    }

    /// Enqueue a new disk request.
    pub fn enqueue(&self, req: DiskRequest) {
        let mut queue = self.requests_queue.lock().unwrap();
        queue.push_back(req);
    }

    /// Worker loop (background thread).
    pub fn start_worker_thread(self: &Arc<Self>) {
        let ds_clone = Arc::clone(self);
        thread::spawn(move || 
            loop {
                let done = {
                    let queue = ds_clone.requests_queue.lock().unwrap();
                    queue.is_empty()
                };
                if done {
                    break;
                }
            ds_clone.schedule(4).unwrap_or_default();
            thread::sleep(Duration::from_millis(50));
        });
    }

    /// Process up to `count` queued requests.
    pub fn schedule(&self, count: usize) -> Result<()> {
        let mut queue = self.requests_queue.lock().unwrap();
        let take_count = std::cmp::min(queue.len(), count);
        let requests: Vec<_> = queue.drain(0..take_count).collect();
        drop(queue);

        for mut req in requests {
            println!("Queue length: {}", self.requests_queue.lock().unwrap().len());
            let result = if req.is_write {
                self.manager.write_page(req.page_id, &req.data)
            } else {
                self.manager.read_page(req.page_id, &mut req.data)
            };
            let _ = req.callback.send(result.is_ok());
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

    #[test]
    fn test_worker_schedule_mixed_requests() {
        // 1. Setup temporary disk
        let dir = tempdir().unwrap();
        let db_path = dir.path().join("test.db");

        let dm = Arc::new(DiskManager::new(&db_path).expect("failed to create disk manager"));
        let ds = DiskScheduler::new(dm.clone());

        // 2. Prepare data for pages
        let page_data: Vec<Vec<u8>> = vec![
            vec![1u8; 4096],
            vec![2u8; 4096],
            vec![3u8; 4096],
        ];

        // 3. Create a mixed vector of requests (both write and read)
        let mut requests = Vec::new();
        let mut callbacks = Vec::new();

        for (i, data) in page_data.iter().enumerate() {
            // Write request
            let (tx_w, rx_w) = oneshot::channel();
            requests.push(DiskRequest {
                page_id: i as i32,
                data: data.clone(),
                is_write: true,
                callback: tx_w,
            });
            callbacks.push(rx_w);

            // Read request
            let (tx_r, rx_r) = oneshot::channel();
            requests.push(DiskRequest {
                page_id: i as i32,
                data: vec![0u8; 4096],
                is_write: false,
                callback: tx_r,
            });
            callbacks.push(rx_r);
        }

        // 4. Enqueue and run
        for req in requests {
            ds.enqueue(req);
        }

        ds.schedule(6).expect("schedule failed");

        // 5. Check callbacks
        for (i, rx) in callbacks.into_iter().enumerate() {
            assert!(rx.blocking_recv().unwrap(), "callback {} failed", i);
        }

        // 6. Verify disk data
        for (i, expected) in page_data.iter().enumerate() {
            let mut buf = vec![0u8; 4096];
            dm.read_page(i as i32, &mut buf).unwrap();
            assert_eq!(&buf, expected);
        }

        dir.close().unwrap();
    }
}
*/
