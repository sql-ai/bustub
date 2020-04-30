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
 *****************************************************************************/
template <typename KeyType, typename ValueType, typename KeyComparator>
void HASH_TABLE_TYPE::Resize(size_t initial_size) {}

/*****************************************************************************
 * GETSIZE
 *****************************************************************************/
template <typename KeyType, typename ValueType, typename KeyComparator>
size_t HASH_TABLE_TYPE::GetSize() {
  return 0;
}

template class LinearProbeHashTable<int, int, IntComparator>;

template class LinearProbeHashTable<GenericKey<4>, RID, GenericComparator<4>>;
template class LinearProbeHashTable<GenericKey<8>, RID, GenericComparator<8>>;
template class LinearProbeHashTable<GenericKey<16>, RID, GenericComparator<16>>;
template class LinearProbeHashTable<GenericKey<32>, RID, GenericComparator<32>>;
template class LinearProbeHashTable<GenericKey<64>, RID, GenericComparator<64>>;

}  // namespace bustub
