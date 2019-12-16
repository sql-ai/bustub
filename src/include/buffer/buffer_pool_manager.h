//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// buffer_pool_manager.h
//
// Identification: src/include/buffer/buffer_pool_manager.h
//
// Copyright (c) 2015-2019, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#pragma once

#include <list>
#include <mutex>  // NOLINT
#include <unordered_map>

#include "buffer/clock_replacer.h"
#include "recovery/log_manager.h"
#include "storage/disk/disk_manager.h"
#include "storage/page/page.h"

namespace bustub 
{
    
/**
 * BufferPoolManager reads disk pages to and from its internal buffer pool.
 * 
 * The BufferPoolManager is responsible for fetching database pages from the DiskManager 
 * and storing them in memory. 
 * 
 * The BufferPoolManager can also write dirty pages to disk when it is either explicitly 
 * instructed to do so or when it needs to evict a page to make space for a new page.
 * 
 * Code that actually reads and writes data to disk is implemented in the DiskManager.
 * 
 * All in-memory pages in the system are represented by Page objects. BufferPoolManager does not 
 * need to understand the contents of pages. But it is important to understand that Page objects 
 * are just containers for memory in the buffer pool and thus are not specific to a unique page. 
 * 
 * That is, each Page object contains a block of memory that the DiskManager will use as a location 
 * to copy the contents of a physical page that it reads from disk. 
 * 
 * The BufferPoolManager will reuse the same Page object to store data as it moves back and forth 
 * to disk. 
 * 
 * This means that the same Page object may contain a different physical page throughout the life of 
 * the system. The Page object's identifer (page_id) keeps track of what physical page it contains; 
 * if a Page object does not contain a physical page, then its page_id is set to INVALID_PAGE_ID.
 * 
 * Each Page object also maintains a counter for the number of threads that have "pinned" that page. 
 * BufferPoolManager is not allowed to free a Page that is pinned. Each Page object also keeps track 
 * of whether it is dirty or not. BufferPoolManager must knows whether a page was modified before it is unpinned. 
 * 
 * BufferPoolManager must write the contents of a dirty Page back to disk before that object can be reused.
 * 
 * This BufferPoolManager uses ClockReplacer to keep track of when Page objects are accessed so that it can 
 * decide which one to evict when it must free a frame to make room for copying a new physical page from disk.
 */
class BufferPoolManager 
{
 public:
  enum class CallbackType { BEFORE, AFTER };    
  using bufferpool_callback_fn = void (*)(enum CallbackType, const page_id_t page_id);

  /**
   * Creates a new BufferPoolManager.
   * @param pool_size the size of the buffer pool
   * @param disk_manager the disk manager
   * @param log_manager the log manager (for testing only: nullptr = disable logging)
   */
  BufferPoolManager(
    size_t pool_size, 
    DiskManager *disk_manager,
    LogManager *log_manager = nullptr);

  /**
   * Destroys an existing BufferPoolManager.
   */
  ~BufferPoolManager();

  /** Grading function. Do not modify! */
  Page *FetchPage(page_id_t page_id, bufferpool_callback_fn callback = nullptr) 
  {
    GradingCallback(callback, CallbackType::BEFORE, page_id);
    auto *result = FetchPageImpl(page_id);
    GradingCallback(callback, CallbackType::AFTER, page_id);
    return result;
  }

  /** Grading function. Do not modify! */
  bool UnpinPage(page_id_t page_id, bool is_dirty, bufferpool_callback_fn callback = nullptr) 
  {
    GradingCallback(callback, CallbackType::BEFORE, page_id);
    auto result = UnpinPageImpl(page_id, is_dirty);
    GradingCallback(callback, CallbackType::AFTER, page_id);
    return result;
  }

  /** Grading function. Do not modify! */
  bool FlushPage(page_id_t page_id, bufferpool_callback_fn callback = nullptr) 
  {
    GradingCallback(callback, CallbackType::BEFORE, page_id);
    auto result = FlushPageImpl(page_id);
    GradingCallback(callback, CallbackType::AFTER, page_id);
    return result;
  }

  /** Grading function. Do not modify! */
  Page *NewPage(page_id_t *page_id, bufferpool_callback_fn callback = nullptr) 
  {
    GradingCallback(callback, CallbackType::BEFORE, INVALID_PAGE_ID);
    auto *result = NewPageImpl(page_id);
    GradingCallback(callback, CallbackType::AFTER, *page_id);
    return result;
  }

  /** Grading function. Do not modify! */
  bool DeletePage(page_id_t page_id, bufferpool_callback_fn callback = nullptr) 
  {
    GradingCallback(callback, CallbackType::BEFORE, page_id);
    auto result = DeletePageImpl(page_id);
    GradingCallback(callback, CallbackType::AFTER, page_id);
    return result;
  }

  /** Grading function. Do not modify! */
  void FlushAllPages(bufferpool_callback_fn callback = nullptr) 
  {
    GradingCallback(callback, CallbackType::BEFORE, INVALID_PAGE_ID);
    FlushAllPagesImpl();
    GradingCallback(callback, CallbackType::AFTER, INVALID_PAGE_ID);
  }

  /** @return pointer to all the pages in the buffer pool */
  Page *GetPages() { return pages_; }

  /** @return size of the buffer pool */
  size_t GetPoolSize() { return pool_size_; }

 private:
  /**
   * Grading function. Do not modify!
   * Invokes the callback function if it is not null.
   * @param callback callback function to be invoked
   * @param callback_type BEFORE or AFTER
   * @param page_id the page id to invoke the callback with
   */
  void GradingCallback(bufferpool_callback_fn callback, CallbackType callback_type, page_id_t page_id) 
  {
    if (callback != nullptr) 
    {
      callback(callback_type, page_id);
    }
  }

  /**
   * Fetch the requested page from the buffer pool.
   * @param page_id id of page to be fetched
   * @return the requested page
   */
  Page *FetchPageImpl(page_id_t page_id);

  /**
   * Unpin the target page from the buffer pool.
   * @param page_id id of page to be unpinned
   * @param is_dirty true if the page should be marked as dirty, false otherwise
   * @return false if the page pin count is <= 0 before this call, true otherwise
   */
  bool UnpinPageImpl(page_id_t page_id, bool is_dirty);
  
  /**
   * Flushes the target page to disk.
   * @param page_id id of page to be flushed, cannot be INVALID_PAGE_ID
   * @return false if the page could not be found in the page table, true otherwise
   */
  bool FlushPageImpl(page_id_t page_id);

  /**
   * Creates a new page in the buffer pool.
   * @param[out] page_id id of created page
   * @return nullptr if no new pages could be created, otherwise pointer to new page
   */
  Page *NewPageImpl(page_id_t *page_id);

  /**
   * Deletes a page from the buffer pool.
   * @param page_id id of page to be deleted
   * @return false if the page exists but could not be deleted, true if the page didn't exist or deletion succeeded
   */
  bool DeletePageImpl(page_id_t page_id);

  /**
   * Flushes all the pages in the buffer pool to disk.
   */
  void FlushAllPagesImpl();

  /** Number of pages in the buffer pool. */
  size_t pool_size_;

  /** Array of buffer pool pages. */
  Page *pages_;
  
  /** Pointer to the disk manager. */
  DiskManager *disk_manager_ __attribute__((__unused__));
  
  /** Pointer to the log manager. */
  LogManager *log_manager_ __attribute__((__unused__));
  
  /** Page table for keeping track of buffer pool pages. */
  std::unordered_map<page_id_t, frame_id_t> page_table_;

  /** Replacer to find unpinned pages for replacement. */
  Replacer *replacer_;

  /** List of free pages. */
  std::list<frame_id_t> free_list_;

  /** This latch protects shared data structures. We recommend updating this comment to describe what it protects. */
  std::mutex latch_;
};
}  // namespace bustub
