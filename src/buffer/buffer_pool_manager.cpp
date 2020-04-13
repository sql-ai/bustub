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
// The BufferPoolManager is responsible for fetching database pages from the DiskManager and storing them in memory.
// BufferPoolManager can also write dirty pages out to disk when it is either explicitly instructed to do so
// or when it needs to evict a page to make space for a new page.

// All in-memory pages in the system are represented by Page objects. BufferPoolManager does not need to understand 
// the contents of these pages. Page objects are just containers for memory in the buffer pool and thus are not specific to a unique page.

// That is, each Page object contains a block of memory that the DiskManager will use as a location to
// copy the contents of a physical page that it reads from disk. The BufferPoolManager will reuse the same Page object to
// store data as it moves back and forth to disk. This means that the same Page object may contain a different physical page
// throughout the life of the system. 

// The Page object's identifer (page_id) keeps track of what physical page it contains;
// if a Page object does not contain a physical page, then its page_id must be set to INVALID_PAGE_ID.

// Each Page object also maintains a counter for the number of threads that have "pinned" that page.
// BufferPoolManager is not allowed to free a Page that is pinned. 

// Each Page object also keeps track of whether it is dirty or not and BufferPoolManager records whether a page was modified before it is unpinned.
// BufferPoolManager write the contents of a dirty Page back to disk before that object can be reused.

// BufferPoolManager uses the ClockReplacer class to keep track of when Page objects are accessed so that 
// it can decide which one to evict when it must free a frame to make room for copying a new physical page from disk.

//===----------------------------------------------------------------------===//

#include "buffer/buffer_pool_manager.h"

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
}

BufferPoolManager::~BufferPoolManager() {
  delete[] pages_;
  delete replacer_;
}

Page *BufferPoolManager::FetchPageImpl(page_id_t page_id) {
  // 1.     Search the page table for the requested page (P).
  // 1.1    If P exists, pin it and return it immediately.
  // 1.2    If P does not exist, find a replacement page (R) from either the free list or the replacer.
  //        Note that pages are always found from the free list first.
  // 2.     If R is dirty, write it back to the disk.
  // 3.     Delete R from the page table and insert P.
  // 4.     Update P's metadata, read in the page content from disk, and then return a pointer to P.
  return nullptr;
}

bool BufferPoolManager::UnpinPageImpl(page_id_t page_id, bool is_dirty) 
{ 
  return false; 
}

bool BufferPoolManager::FlushPageImpl(page_id_t page_id) {
  // Make sure you call DiskManager::WritePage!
  return false;
}

Page *BufferPoolManager::NewPageImpl(page_id_t *page_id) {
  // 0.   Make sure you call DiskManager::AllocatePage!
  // 1.   If all the pages in the buffer pool are pinned, return nullptr.
  // 2.   Pick a victim page P from either the free list or the replacer. Always pick from the free list first.
  // 3.   Update P's metadata, zero out memory and add P to the page table.
  // 4.   Set the page ID output parameter. Return a pointer to P.

  page_id_t page = disk_manager_->AllocatePage();
  frame_id_t frame;
  if (free_list_.empty())
  {
    if (!replacer_->Victim(&frame))
    {
      return nullptr;
    }

    page_id_t old_page = pages_[frame].GetPageId();
    if (pages_[frame].IsDirty())
    {
      disk_manager_->WritePage(old_page, pages_[frame].GetData());
    }

    page_table_.erase(page_table_.find(old_page));

  } 
  else 
  {
    frame = free_list_.back();
    free_list_.pop_back();
  }

  page_table_[page] = frame;
  replacer_->Pin(frame);

  pages_[frame].ResetMemory();
  pages_[frame].page_id_ = page;
  pages_[frame].is_dirty_ = false;
  pages_[frame].pin_count_ = 1;  
  *page_id = page;
  return &pages_[frame];
}

bool BufferPoolManager::DeletePageImpl(page_id_t page_id) {
  // 0.   Make sure you call DiskManager::DeallocatePage!
  // 1.   Search the page table for the requested page (P).
  // 1.   If P does not exist, return true.
  // 2.   If P exists, but has a non-zero pin-count, return false. Someone is using the page.
  // 3.   Otherwise, P can be deleted. Remove P from the page table, reset its metadata and return it to the free list.
  return false;
}

void BufferPoolManager::FlushAllPagesImpl() {
  // You can do it!
}

}  // namespace bustub
