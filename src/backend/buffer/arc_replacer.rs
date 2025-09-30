// src/buffer/arc_replacer.rs

//! ArcReplacer (Adaptive Replacement Cache)
//!
//! Translated from BusTub C++ skeleton into Rust.
//! Implements the ARC eviction policy used in the buffer pool manager.
//! See https://github.com/cmu-db/bustub/blob/master/src/buffer/arc_replacer.cpp

use std::collections::{HashMap, VecDeque};
use std::sync::Mutex;

use crate::common::types::{FrameId, PageId};

/// Access type (needed for leaderboard tests).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum AccessType {
    Scan,
    Lookup,
    Index,
    Unknown, 
}

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
#[derive(Debug, Clone)]
pub struct FrameStatus {
    pub page_id: PageId,
    pub frame_id: FrameId,
    pub evictable: bool,
    pub arc_status: ArcStatus,
}

/// Adaptive Replacement Cache (ARC) Replacer.
///
/// TODO(P1): Implement ARC as described in the writeup.
/// Keeps track of MRU, MFU, and their ghost lists.
pub struct ArcReplacer {
    replacer_size: usize,
    mru_list: VecDeque<FrameId>,
    mfu_list: VecDeque<FrameId>,
    mru_ghost_list: VecDeque<FrameId>,
    table: HashMap<FrameId, FrameStatus>,
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
            table: HashMap::new(),
            latch: Mutex::new(()),
        }
    }

    pub fn evict(&mut self) -> Option<FrameId> {
        // TODO(P1)
        None
    }

    /// Record access to a frame and update ARC bookkeeping.
    ///
    /// Four cases:
    /// 1. Frame exists in MRU/MFU
    /// 2. Frame exists in MRU ghost
    /// 3. Frame exists in MFU ghost
    /// 4. Miss everywhere
    pub fn record_access(&mut self, frame_id: FrameId, page_id: PageId, _access_type: AccessType) {
        // TODO(P1)
    }

    /// Toggle whether a frame is evictable.
    /// Updates replacer size accordingly.
    pub fn set_evictable(&mut self, frame_id: FrameId, set_evictable: bool) -> Result<()> {
        // TODO(P1)
        Ok(())
    }

    /// Remove an evictable frame from the replacer.
    /// If frame is not evictable → error.
    pub fn remove(&mut self, frame_id: FrameId) -> Result<()> {
        // TODO(P1)
        Ok(())
    }

    /// Return the number of evictable frames.
    pub fn size(&self) -> usize {
        // TODO(P1)

    }
}