// src/buffer/arc_replacer.rs

//! ArcReplacer (Adaptive Replacement Cache)
//!
//! Translated from BusTub C++ skeleton into Rust.
//! Implements the ARC eviction policy used in the buffer pool manager.
//! See https://github.com/cmu-db/bustub/blob/master/src/buffer/arc_replacer.cpp

use std::collections::{HashMap, VecDeque};
use std::sync::Mutex;
use anyhow::{Result};
//use log::{error, info};
use crate::common::types::{FrameId, PageId};

/// Access type (needed for leaderboard tests).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum AccessType {
    Scan,
    Lookup,
    Index,
    Unknown, 
}
#[derive(Debug, Clone)]
pub enum ArcStatus{
    //most recently used
    MRU,
    //most frequently used
    MFU,
    //evicted MRU
    MRUGhost,
    //evicted MFU
    MFUGhost,
}

/// Metadata for a frame tracked by the replacer.
#[derive(Clone)]
pub struct FrameStatus {
    pub page_id: PageId,
    pub frame_id: FrameId,
    pub evictable: bool,
    pub arc_status: ArcStatus,
}

/// Adaptive Replacement Cache (ARC) Replacer.
/// Keeps track of MRU, MFU, and their ghost lists.
pub struct ArcReplacer {
    replacer_size: usize,
    mru_list: VecDeque<FrameId>,
    mfu_list: VecDeque<FrameId>,
    mru_ghost_list: VecDeque<FrameId>,
    mfu_ghost_list: VecDeque<FrameId>,
    pin_table: HashMap<FrameId, FrameStatus>,
    latch : Mutex<()>,
}

impl ArcReplacer {
    /// Create a new ArcReplacer, with empty lists and size 0.
    pub fn new(num_frames: usize) -> Self {
        Self {
            replacer_size: num_frames,
            mru_list: VecDeque::new(),
            mfu_list: VecDeque::new(),
            mru_ghost_list: VecDeque::new(),
            mfu_ghost_list: VecDeque::new(),
            pin_table: HashMap::new(),
            latch: Mutex::new(()),
        }
    }

    pub fn evict(&mut self) -> Option<FrameId> {
        let _guard = self.latch.lock().unwrap();

        // Scan MRU list first
        let mut idx = 0;
        while idx < self.mru_list.len() {
            let candt = self.mru_list[idx];
            if let Some(status) = self.pin_table.get(&candt) {
                if status.evictable {
                    let candt = self.mru_list.remove(idx).unwrap();
                    self.mru_ghost_list.push_back(candt);
                    if let Some(entry) = self.pin_table.get_mut(&candt) {
                        entry.arc_status = ArcStatus::MRUGhost;
                    }
                    return Some(candt);
                }
            }
            idx += 1;
        }

        // Then scan MFU list
        let mut idx = 0;
        while idx < self.mfu_list.len() {
            let candt = self.mfu_list[idx];
            if let Some(status) = self.pin_table.get(&candt) {
                if status.evictable {
                    let candt = self.mfu_list.remove(idx).unwrap();
                    self.mfu_ghost_list.push_back(candt);
                    if let Some(entry) = self.pin_table.get_mut(&candt) {
                        entry.arc_status = ArcStatus::MFUGhost;
                    }
                    return Some(candt);
                }
            }
            idx += 1;
        }
        log::error!("No evictable frame found");
        None
    }


    /// Record access to a frame and update ARC bookkeeping.
    /// {TODO: after buffer pool manager}
    /// Four cases:
    /// 1. Frame exists in MRU/MFU
    /// 2. Frame exists in MRU ghost
    /// 3. Frame exists in MFU ghost
    /// 4. Miss everywhere
    pub fn record_access(&mut self, frame_id: FrameId, page_id: PageId, _access_type: AccessType) {

    }

    /// Toggle whether a frame is evictable.
    /// Updates replacer size accordingly.
    pub fn set_keep(&mut self, frame_id: FrameId) -> Result<()> {
        if let Some(status) = self.pin_table.get_mut(&frame_id) {
            status.evictable = false;
        }
        Ok(())
    }

    pub fn set_evicted(&mut self, frame_id: FrameId) -> Result<()> {
        if let Some(status) = self.pin_table.get_mut(&frame_id) {
            status.evictable = true;
        }
        Ok(())
    }

    /// Remove an evictable frame from the replacer.
    /// If frame is not evictable → error.
    pub fn remove(&mut self, frame_id: FrameId) -> Result<()> {
        let _guard = self.latch.lock().unwrap();

        // 1. Lookup frame
        if let Some(status) = self.pin_table.get(&frame_id) {
            // 2. Check evictable
            if !status.evictable {
                return Err(anyhow::anyhow!("Frame {} is not evictable", frame_id));
            }
            // 3. Remove from the correct list
            match status.arc_status {
                ArcStatus::MRU => {
                    if let Some(pos) = self.mru_list.iter().position(|&id| id == frame_id) {
                        self.mru_list.remove(pos);
                    }
                }
                ArcStatus::MFU => {
                    if let Some(pos) = self.mfu_list.iter().position(|&id| id == frame_id) {
                        self.mfu_list.remove(pos);
                    }
                }
                ArcStatus::MRUGhost => {
                    if let Some(pos) = self.mru_ghost_list.iter().position(|&id| id == frame_id) {
                        self.mru_ghost_list.remove(pos);
                    }
                }
                ArcStatus::MFUGhost => {
                    if let Some(pos) = self.mfu_ghost_list.iter().position(|&id| id == frame_id) {
                        self.mfu_ghost_list.remove(pos);
                    }
                }
            }
            //remove from table
            self.pin_table.remove(&frame_id);
            Ok(())
        } else {
            // Frame not tracked
            Err(anyhow::anyhow!("Frame {} not found in replacer", frame_id))
        }
    }

    /// Return the number of evictable frames.
    pub fn size(&self) -> usize {
        let _guard = self.latch.lock().unwrap();
        return self.pin_table.values().filter(|status| status.evictable).count()
    }

   //delete from ghost deques if they exceed set replacer size
    fn delete_ghost(&mut self){
        if self.mru_ghost_list.len() > self.replacer_size {
            self.mru_ghost_list.pop_front();
        } else if self.mfu_ghost_list.len() > self.replacer_size {
            self.mfu_ghost_list.pop_front();
        }
    }

}
/*
#[cfg(test)]
mod tests {
    use crate::backend::buffer::arc_replacer::{ArcReplacer, ArcStatus};
    //use crate::common::types::FrameId;

    #[test]
    fn test_insert_and_evict() {
        let mut replacer = ArcReplacer::new(3); // capacity 3

        // Insert frames
        replacer.insert(1);
        replacer.insert(2);
        replacer.insert(3);

        // Initially, all frames are evictable
        assert!(replacer.pin_table.get(&1).unwrap().evictable);
        assert!(replacer.pin_table.get(&2).unwrap().evictable);
        assert!(replacer.pin_table.get(&3).unwrap().evictable);

        // Evict one frame
        let victim = replacer.evict();
        assert!(victim.is_some());
        let victim_id = victim.unwrap();

        // The victim should now be in the ghost list
        let ghost_status = replacer.pin_table.get(&victim_id).unwrap();
        match ghost_status.arc_status {
            ArcStatus::MRUGhost | ArcStatus::MFUGhost => {}
            _ => panic!("Evicted frame not in ghost list"),
        }
    }

    #[test]
    fn test_set_evictable() {
        let mut replacer = ArcReplacer::new(2);
        replacer.insert(10);
        replacer.insert(20);

        // Pin frame 10 (set evictable = false)
        replacer.set_evictable(10, false).unwrap();

        // Trying to evict should skip frame 10 if it is MRU/MFU head
        let victim = replacer.evict().unwrap();
        assert_ne!(victim, 10);
    }

    #[test]
    fn test_remove() {
        let mut replacer = ArcReplacer::new(2);
        replacer.insert(100);
        replacer.insert(200);

        // Remove a frame
        replacer.remove(100).unwrap();
        assert!(replacer.table.get(&100).is_none());
    }
}
    */ 
