//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// linear_probe_hash_table.cpp
//
// Identification: src/container/hash/linear_probe_hash_table.cpp
//
// Copyright (c) 2015-2019, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include <iostream>
#include <string>
#include <utility>
#include <vector>

#include "common/exception.h"
#include "common/logger.h"
#include "common/rid.h"
#include "container/hash/linear_probe_hash_table.h"

namespace bustub {

template <typename KeyType, typename ValueType, typename KeyComparator>
HASH_TABLE_TYPE::LinearProbeHashTable(
      const std::string &name, 
      BufferPoolManager *buffer_pool_manager,
      const KeyComparator &comparator, size_t num_buckets,
      HashFunction<KeyType> hash_fn)
    : buffer_pool_manager_(buffer_pool_manager),
      comparator_(comparator), 
      hash_fn_(std::move(hash_fn)),
      num_buckets_(num_buckets),
      size_(0) 
    {
      Page *page = buffer_pool_manager->NewPage(&header_page_id_);
      HashTableHeaderPage *header_page = reinterpret_cast<HashTableHeaderPage*>(page);
      header_page->SetSize(size_);
      // header_page->SetPageId(header_page_id_);

      num_blocks_ = (num_buckets_ - 1) / BLOCK_ARRAY_SIZE + 1;
      page_id_t block_page_id;
      for (size_t i = 0; i < num_blocks_; i++)
      {
        buffer_pool_manager->NewPage(&block_page_id);
        header_page->AddBlockPageId(block_page_id);
        buffer_pool_manager->UnpinPage(block_page_id, true);
      }
      buffer_pool_manager->UnpinPage(header_page_id_, true);
    }

/*****************************************************************************
 * SEARCH
 *****************************************************************************/
template <typename KeyType, typename ValueType, typename KeyComparator>
bool HASH_TABLE_TYPE::GetValue(
  Transaction *transaction, 
  const KeyType &key, 
  std::vector<ValueType> *result)
{
  size_t offset = hash_fn_.GetHash(key) % num_buckets_;
  HashTableHeaderPage *header_page = reinterpret_cast<HashTableHeaderPage*>(buffer_pool_manager_->FetchPage(header_page_id_));
  bool isKeyFound = false;
  for (size_t i = 0; i < num_buckets_; i++)
  {
    size_t block_id = offset / BLOCK_ARRAY_SIZE;
    size_t bucket_ind = offset % BLOCK_ARRAY_SIZE;
    
    page_id_t page_id = header_page->GetBlockPageId(block_id);
    Page* b_page = buffer_pool_manager_->FetchPage(page_id);
    HASH_TABLE_BLOCK_TYPE *block_page = reinterpret_cast<HASH_TABLE_BLOCK_TYPE *>(b_page);

    if (block_page->IsReadable(bucket_ind) && comparator_(key, block_page->KeyAt(bucket_ind)) == 0)
    {
      result->push_back(block_page->ValueAt(bucket_ind));
      isKeyFound = true;
    }
    if (!block_page->IsOccupied(bucket_ind))
    {
      buffer_pool_manager_->UnpinPage(page_id, false);
      break;
    }
    buffer_pool_manager_->UnpinPage(page_id, false);
    offset = (offset + 1) % num_buckets_;
  }

  buffer_pool_manager_->UnpinPage(header_page_id_, false);
  return isKeyFound;
}
/*****************************************************************************
 * INSERTION
 *****************************************************************************/
template <typename KeyType, typename ValueType, typename KeyComparator>
bool HASH_TABLE_TYPE::Insert(Transaction *transaction, const KeyType &key, const ValueType &value)
{
  size_t offset = hash_fn_.GetHash(key) % num_buckets_;
  HashTableHeaderPage *header_page = reinterpret_cast<HashTableHeaderPage*>(buffer_pool_manager_->FetchPage(header_page_id_));
  for (size_t i = 0; i < num_buckets_; i++)
  {
    size_t block_id = offset / BLOCK_ARRAY_SIZE;
    size_t bucket_ind = offset % BLOCK_ARRAY_SIZE;
    
    page_id_t page_id = header_page->GetBlockPageId(block_id);
    Page* b_page = buffer_pool_manager_->FetchPage(page_id);
    HASH_TABLE_BLOCK_TYPE *block_page = reinterpret_cast<HASH_TABLE_BLOCK_TYPE *>(b_page);

    if (block_page->IsReadable(bucket_ind) && 
        comparator_(key, block_page->KeyAt(bucket_ind)) == 0 && value == block_page->ValueAt(bucket_ind)) 
    {
      buffer_pool_manager_->UnpinPage(page_id, false);
      buffer_pool_manager_->UnpinPage(header_page_id_, false);
      return false;
    }
    if (!block_page->IsOccupied(bucket_ind))
    {
      block_page->Insert(bucket_ind, key, value);
      buffer_pool_manager_->UnpinPage(page_id, true);
      size_++;
      header_page->SetSize(size_);
      buffer_pool_manager_->UnpinPage(header_page_id_, true);
      return true;
    }
    buffer_pool_manager_->UnpinPage(page_id, false);
    offset = (offset + 1) % num_buckets_;
  }
  buffer_pool_manager_->UnpinPage(header_page_id_, false);
  return false;
}

/*****************************************************************************
 * REMOVE
 *****************************************************************************/
template <typename KeyType, typename ValueType, typename KeyComparator>
bool HASH_TABLE_TYPE::Remove(Transaction *transaction, const KeyType &key, const ValueType &value) 
{
  size_t offset = hash_fn_.GetHash(key) % num_buckets_;
  HashTableHeaderPage *header_page = reinterpret_cast<HashTableHeaderPage*>(buffer_pool_manager_->FetchPage(header_page_id_));
  for (size_t i = 0; i < num_buckets_; i++)
  {
    size_t block_id = offset / BLOCK_ARRAY_SIZE;
    size_t bucket_ind = offset % BLOCK_ARRAY_SIZE;
    
    page_id_t page_id = header_page->GetBlockPageId(block_id);
    Page* b_page = buffer_pool_manager_->FetchPage(page_id);
    HASH_TABLE_BLOCK_TYPE *block_page = reinterpret_cast<HASH_TABLE_BLOCK_TYPE *>(b_page);

    if (block_page->IsReadable(bucket_ind) && 
        comparator_(key, block_page->KeyAt(bucket_ind)) == 0 && value == block_page->ValueAt(bucket_ind)) 
    {
      block_page->Remove(bucket_ind);
      buffer_pool_manager_->UnpinPage(page_id, true);
      size_--;
      header_page->SetSize(size_);
      buffer_pool_manager_->UnpinPage(header_page_id_, true);
      return true;
    }
    if (!block_page->IsOccupied(bucket_ind))
    {
      buffer_pool_manager_->UnpinPage(page_id, false);
      buffer_pool_manager_->UnpinPage(header_page_id_, false);
      return false;
    }
    buffer_pool_manager_->UnpinPage(page_id, false);
    offset = (offset + 1) % num_buckets_;
  }
  buffer_pool_manager_->UnpinPage(header_page_id_, false);
  return false;
}

/*****************************************************************************
 * RESIZE
 * The linear probing hashing scheme uses a fized-size table. 
 * When the hash table is full, then any insert operation will get stuck in an infinite loop 
 * because the system will walk through the entire slot array and not find a free space. 
 * If hash table detects it is full, then it must resize itself to be twice the current size
 * (i.e., if currently has n slots, then the new size will be 2×n).
 * 
 * Since any write operation could lead to a change of header_page_id in hash table, 
 * update header_page_id in the header page (src/include/page/header_page.h) to ensure 
 * that the container is durable on disk.
 * 
 *****************************************************************************/
template <typename KeyType, typename ValueType, typename KeyComparator>
void HASH_TABLE_TYPE::Resize(size_t initial_size) 
{
  page_id_t old_header_page_id = header_page_id_;
  auto *old_header_page = reinterpret_cast<HashTableHeaderPage*>(buffer_pool_manager_->FetchPage(old_header_page_id));

  num_buckets_ = initial_size * 2;
  num_blocks_ = (num_buckets_ - 1) / BLOCK_ARRAY_SIZE + 1;

  auto header_page = reinterpret_cast<HashTableHeaderPage *>(buffer_pool_manager_->NewPage(&header_page_id_));

  page_id_t block_page_id;
  for (size_t i = 0; i < num_blocks_; i++)
  {
    buffer_pool_manager_->NewPage(&block_page_id);
    header_page->AddBlockPageId(block_page_id);    
    buffer_pool_manager_->UnpinPage(block_page_id, true);
  }
  buffer_pool_manager_->UnpinPage(header_page_id_, true);

  size_ = 0;
  for (size_t block_id = 0; block_id < old_header_page->NumBlocks(); block_id++)
  {
    auto old_block_page_id = old_header_page->GetBlockPageId(block_id);
    HASH_TABLE_BLOCK_TYPE *old_block_page = reinterpret_cast<HASH_TABLE_BLOCK_TYPE*>(
      buffer_pool_manager_->FetchPage(old_block_page_id));
    for (size_t bucket_id = 0; bucket_id < BLOCK_ARRAY_SIZE; bucket_id++)
    {
      if (old_block_page->IsReadable(bucket_id)) 
      {
        Insert(nullptr, old_block_page->KeyAt(bucket_id), old_block_page->ValueAt(bucket_id));
      }      
    }
    buffer_pool_manager_->UnpinPage(old_block_page_id, false);
    buffer_pool_manager_->DeletePage(old_block_page_id);
  }
  buffer_pool_manager_->UnpinPage(old_header_page_id, false);
  buffer_pool_manager_->DeletePage(old_header_page_id);
}

/*****************************************************************************
 * GETSIZE
 *****************************************************************************/
template <typename KeyType, typename ValueType, typename KeyComparator>
size_t HASH_TABLE_TYPE::GetSize() 
{
  return size_;
}

template class LinearProbeHashTable<int, int, IntComparator>;

template class LinearProbeHashTable<GenericKey<4>, RID, GenericComparator<4>>;
template class LinearProbeHashTable<GenericKey<8>, RID, GenericComparator<8>>;
template class LinearProbeHashTable<GenericKey<16>, RID, GenericComparator<16>>;
template class LinearProbeHashTable<GenericKey<32>, RID, GenericComparator<32>>;
template class LinearProbeHashTable<GenericKey<64>, RID, GenericComparator<64>>;

}  // namespace bustub
