// src/storage/disk_scheduler.rs

//! DiskScheduler module
//! This module used to schedule write and read requests to DiskManager
//! This can be used by other components such as BufferPoolmanager to queue disk requests
//! represented by the DiskRequest struct.
//! 
//! Translated from BusTub C++ skeleton into Rust.
//! 
//! See https://github.com/cmu-db/bustub/blob/master/src/storage/disk/disk_scheduler.cpp


use std::sync::Mutex;
use anyhow::{Result};
use std::sync::mpsc::channel;
use std::thread;
use std::sync::oneshot;

use crate::common::types::{FrameId, PageId};
use crate::backend::storage::disk_manager::DiskManager;

//Disk Reuqest being passed around and queued
pub struct DiskRequest{
    pub is_write:bool,
    pub data:char,
    pub page_id:PageId,
    pub callback: oneshot::Sender<bool>
}


struct DiskScheduler<'a>{
    manager: &'a DiskManager,
    requests: Mutex<Vec<DiskRequest>>,
    scheduler_promise: oneshot::Sender<()>,

}

impl DiskScheduler{
    pub fn new(manager: &DiskManager)->self{
        Self{
            manager,
            requests: Mutex::new(vec![]),
        }
    }
    //start threads and decide requests is process and access to disk in order
    pub fn StartWorkerThread(&self){
        None
    }
    pub fn CreatePromise(&self)->oneshot::Sender<()>{
        None
    }
    pub fn Schedule(&self, requests:&Vec<DiskRequest>)->Result<()>{

    }
    pub fn DeallocatePage(&self, delete_page_id:PageId)->Option<PageId>{
        None
    }

}
