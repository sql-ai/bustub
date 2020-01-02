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

#include <list>
#include <unordered_map>

namespace bustub {

BufferPoolManager::BufferPoolManager(
    size_t pool_size, 
    DiskManager *disk_manager, 
    LogManager *log_manager) : 
      pool_size_(pool_size), 
      disk_manager_(disk_manager), 
      log_manager_(log_manager) 
  {
  // We allocate a consecutive memory space for the buffer pool.
  pages_ = new Page[pool_size_];
  replacer_ = new ClockReplacer(pool_size);

  // Initially, every page is in the free list.
  for (size_t i = 0; i < pool_size_; ++i) 
  {
    free_list_.emplace_back(static_cast<int>(i));
  }
}

BufferPoolManager::~BufferPoolManager() 
{
  delete[] pages_;
  delete replacer_;
}

Page *BufferPoolManager::FetchPageImpl(page_id_t page_id) 
{
  // 1.     Search the page table for the requested page (P).
  // 1.1    If P exists, pin it and return it immediately.
  // 1.2    If P does not exist, find a replacement page (R) from either the free list or the replacer.
  //        Note that pages are always found from the free list first.
  // 2.     If R is dirty, write it back to the disk.
  // 3.     Delete R from the page table and insert P.
  // 4.     Update P's metadata, read in the page content from disk, and then return a pointer to P.  
  int fid;
  auto fid_iter = page_table_.find(page_id);
  if (fid_iter != page_table_.end())
  {
    fid = fid_iter->second;
    pages_[fid].pin_count_++;
    return &pages_[fid];
  }

  if (!free_list_.empty())
  {
    fid = free_list_.back();
    free_list_.pop_back();
  }
  else if (replacer_->Victim(&fid) == false)
  {
    return nullptr;
  }

  page_id_t old_page_id = pages_[fid].GetPageId();
  if (pages_[fid].IsDirty())
  {
    disk_manager_->WritePage(old_page_id,pages_[fid].GetData());
  }

  page_table_.erase(old_page_id);
  page_table_[page_id] = fid;
  disk_manager_->ReadPage(page_id, pages_[fid].GetData());
  pages_[fid].pin_count_ = 1;
  pages_[fid].page_id_ = page_id;
  return &pages_[fid];
}

bool BufferPoolManager::UnpinPageImpl(page_id_t page_id, bool is_dirty) 
{ 
  if (page_table_.find(page_id) == page_table_.end()) 
  {
    return false;
  }

  auto fid = page_table_[page_id];
  if (pages_[fid].GetPinCount() == 0)
  {
    return false;
  }

  pages_[fid].pin_count_--;
  if (pages_[fid].GetPinCount() == 0)
  {
    replacer_->Unpin(fid);
  }
  pages_[fid].is_dirty_ |= is_dirty;
  return true;
}

// Retrieve the Page object specified by the given page_id 
// and then use the DiskManager to write its contents out to disk. 
// Upon successful completion of that write operation, the function will return true. 
// Not remove the Page from the buffer pool. 
// Not update the Replacer for the Page. 
// If there is no entry in the page table for the given page_id, then return false.
bool BufferPoolManager::FlushPageImpl(page_id_t page_id) 
{
  BUSTUB_ASSERT(page_id != INVALID_PAGE_ID, "INVALID_PAGE_ID.");

  if (page_table_.find(page_id) == page_table_.end())
  {
    return false;
  }

  frame_id_t fid = page_table_[page_id];
  if (!pages_[fid].IsDirty())
  {
    return false;
  }

  disk_manager_->WritePage(page_id, pages_[fid].GetData());
  pages_[fid].is_dirty_ = false;
  return true;
}

Page *BufferPoolManager::NewPageImpl(page_id_t *page_id) 
{
  // 0.   Make sure you call DiskManager::AllocatePage!
  // 1.   If all the pages in the buffer pool are pinned, return nullptr.
  // 2.   Pick a victim page P from either the free list or the replacer. Always pick from the free list first.
  // 3.   Update P's metadata, zero out memory and add P to the page table.
  // 4.   Set the page ID output parameter. Return a pointer to P.
  int fid;
  if (!free_list_.empty())
  {
    fid = free_list_.back();
    free_list_.pop_back();
  }
  else
  {
    if (replacer_->Victim(&fid) == false)
    {
      return nullptr;
    } 

    page_id_t old_page_id = pages_[fid].GetPageId();
    if (pages_[fid].IsDirty())
    {
      disk_manager_->WritePage(old_page_id,pages_[fid].GetData());
    }
    page_table_.erase(old_page_id);
  }

  *page_id = disk_manager_->AllocatePage();
  page_table_[*page_id]=fid;
  pages_[fid].page_id_ = *page_id;
  pages_[fid].pin_count_ = 1;
  pages_[fid].ResetMemory();
  pages_[fid].is_dirty_ = true;
  return &pages_[fid];
}

bool BufferPoolManager::DeletePageImpl(page_id_t page_id) 
{
  // 0.   Make sure you call DiskManager::DeallocatePage!
  // 1.   Search the page table for the requested page (P).
  // 1.   If P does not exist, return true.
  // 2.   If P exists, but has a non-zero pin-count, return false. Someone is using the page.
  // 3.   Otherwise, P can be deleted. Remove P from the page table, reset its metadata and return it to the free list.
  if (page_table_.find(page_id) == page_table_.end())
  {
    return true;
  }

  frame_id_t fid = page_table_[page_id];
  if (pages_[fid].GetPinCount() > 0 )
  {
    return false;
  }

  page_table_.erase(page_id);
  pages_[fid].page_id_ = INVALID_PAGE_ID;
  free_list_.emplace_back(fid);
  disk_manager_->DeallocatePage(page_id);
  return true;
}

void BufferPoolManager::FlushAllPagesImpl() 
{
  for (auto page_frame : page_table_)
  {
    page_id_t page_id = page_frame.first;
    frame_id_t fid = page_frame.second;
    if (pages_[fid].IsDirty())
    {
        disk_manager_->WritePage(page_id, pages_[fid].GetData());
        pages_[fid].is_dirty_ = false;
    }
  }
}

}  // namespace bustub
