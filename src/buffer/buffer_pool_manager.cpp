//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// buffer_pool_manager.cpp
//
// Identification: src/buffer/buffer_pool_manager.cpp
//
// Copyright (c) 2015-2019, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "buffer/buffer_pool_manager.h"
#include "common/logger.h"

#include <list>
#include <unordered_map>

namespace bustub {

BufferPoolManager::BufferPoolManager(size_t pool_size, DiskManager *disk_manager, LogManager *log_manager)
    : pool_size_(pool_size), disk_manager_(disk_manager), log_manager_(log_manager) {
  // We allocate a consecutive memory space for the buffer pool.
  pages_ = new Page[pool_size_];
  replacer_ = new ClockReplacer(pool_size);

  // Initially, every page is in the free list.
  for (size_t i = 0; i < pool_size_; ++i) {
    free_list_.emplace_back(static_cast<int>(i));
  }
  rw_latch_.SetMaxReaders(pool_size);
}

BufferPoolManager::~BufferPoolManager() {
  delete[] pages_;
  delete replacer_;
}

Page *BufferPoolManager::FetchPageImpl(page_id_t page_id) {
  std::lock_guard<std::mutex> lock(latch_);

  // frame id in which we will fetch page_id.
  frame_id_t fid;

  // Search the page table for the requested page (P).
  if (page_table_.count(page_id) > 0) {
    // If P exists, pin it and return it immediately.
    fid = page_table_[page_id];
    pages_[fid].pin_count_++;
    replacer_->Pin(fid);
    return &pages_[fid];
  }

  rw_latch_.RLock();
  // If P does not exist, find a replacement page (R) from either the free list or the replacer.
  if (!free_list_.empty()) {
    fid = free_list_.back();
    free_list_.pop_back();
  } else {
    if (!replacer_->Victim(&fid)) {
      return nullptr;
    }

    // Delete R from the page table
    page_table_.erase(pages_[fid].GetPageId());
    // If R is dirty, write it back to the disk.
    if (pages_[fid].IsDirty()) {
      disk_manager_->WritePage(pages_[fid].GetPageId(), pages_[fid].GetData());
    }
  }
  // Insert P to page table
  page_table_[page_id] = fid;
  replacer_->Pin(fid);

  // Update P's metadata, read in the page content from disk, and then return a pointer to P.
  pages_[fid].page_id_ = page_id;
  pages_[fid].is_dirty_ = false;
  pages_[fid].pin_count_ = 1;
  disk_manager_->ReadPage(page_id, pages_[fid].GetData());
  return &pages_[fid];
}

bool BufferPoolManager::UnpinPageImpl(page_id_t page_id, bool is_dirty) {
  std::lock_guard<std::mutex> lock(latch_);
  

  if (page_table_.count(page_id) == 0) {
    LOG_DEBUG("Unpin page %d that is not in the buffer pool", page_id);
    return false;
  }

  frame_id_t fid = page_table_[page_id];

  if (pages_[fid].GetPinCount() <= 0) {
    LOG_DEBUG("Unpin page %d that have pin count = %d", page_id, pages_[fid].pin_count_);
    return false;
  }

  pages_[fid].pin_count_--;
  pages_[fid].is_dirty_ |= is_dirty;

  if (pages_[fid].pin_count_ == 0) {
    replacer_->Unpin(fid);
  }
  return true;
}

bool BufferPoolManager::FlushPageImpl(page_id_t page_id) {
  std::lock_guard<std::mutex> lock(latch_);

  if (page_table_.count(page_id) == 0) {
    return false;
  }

  frame_id_t fid = page_table_[page_id];
  if (pages_[fid].IsDirty()) {
    disk_manager_->WritePage(page_id, pages_[fid].GetData());
    pages_[fid].is_dirty_ = false;
  }
  return false;
}

Page *BufferPoolManager::NewPageImpl(page_id_t *page_id) {
  std::lock_guard<std::mutex> lock(latch_);

  *page_id = disk_manager_->AllocatePage();
  frame_id_t fid;

  // Pick a victim page R from either the free list or the replacer. Always pick from the free list first.
  if (!free_list_.empty()) {
    fid = free_list_.back();
    free_list_.pop_back();
  } else {
    if (!replacer_->Victim(&fid)) {
      return nullptr;
    }

    // delete R from page table.
    page_table_.erase(page_table_.find(pages_[fid].GetPageId()));

    // If R is dirty, write it back to the disk.
    if (pages_[fid].IsDirty()) {
      disk_manager_->WritePage(pages_[fid].GetPageId(), pages_[fid].GetData());
    }
  }

  // Update P's metadata, zero out memory and add P to the page table.
  page_table_[*page_id] = fid;
  replacer_->Pin(fid);

  pages_[fid].page_id_ = *page_id;
  pages_[fid].is_dirty_ = false;
  pages_[fid].pin_count_ = 1;
  pages_[fid].ResetMemory();

  // Set the page ID output parameter. Return a pointer to P.
  return &pages_[fid];
}

bool BufferPoolManager::DeletePageImpl(page_id_t page_id) {
  std::lock_guard<std::mutex> lock(latch_);

  if (page_table_.count(page_id) > 0) {
    frame_id_t fid = page_table_[page_id];
    if (pages_[fid].GetPinCount() > 0) {
      return false;
    }

    page_table_.erase(page_table_.find(page_id));
    pages_[fid].ResetMemory();
    pages_[fid].page_id_ = INVALID_PAGE_ID;
    pages_[fid].pin_count_ = 0;
    pages_[fid].is_dirty_ = false;
    free_list_.push_back(fid);
  }
  disk_manager_->DeallocatePage(page_id);
  return true;
}

void BufferPoolManager::FlushAllPagesImpl() {
  for (size_t i = 0; i < pool_size_; i++) {
    FlushPageImpl(pages_[i].GetPageId());
  }
}

}  // namespace bustub
