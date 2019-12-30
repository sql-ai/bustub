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
HASH_TABLE_TYPE::LinearProbeHashTable
(
    const std::string &name,
    BufferPoolManager *buffer_pool_manager,
    const KeyComparator &comparator,
    size_t num_buckets,
    HashFunction<KeyType> hash_fn
) : buffer_pool_manager_(buffer_pool_manager),
    comparator_(comparator),
    sz_(0),
    num_buckets_(num_buckets),    
    hash_fn_(std::move(hash_fn))
{
  // TODO: Check below command
  // header_page_ = reinterpret_cast<HashTableHeaderPage *>(buffer_pool_manager_->NewPage(&header_page_id_));
  header_page_ = reinterpret_cast<HashTableHeaderPage *>(buffer_pool_manager_->NewPage(&header_page_id_)->GetData());
  header_page_->SetSize(num_buckets);
  num_pages_ = (num_buckets_ -1) / BLOCK_ARRAY_SIZE + 1; 
  page_id_t block_page_id;
  for (size_t i = 0; i < num_pages_; i++)
  {
    buffer_pool_manager_->NewPage(&block_page_id);
    header_page_->AddBlockPageId(block_page_id);
  }
}

/*****************************************************************************
* SEARCH
* 
* Performs a point query on the hash table.
* @param transaction the current transaction
* @param key the key to look up
* @param[out] result the value(s) associated with a given key
* @return the value(s) associated with the given key
*****************************************************************************/
template <typename KeyType, typename ValueType, typename KeyComparator>
bool HASH_TABLE_TYPE::GetValue(
  Transaction *transaction, 
  const KeyType &key, 
  std::vector<ValueType> *result)
{
  size_t global_bucket_idx = hash_fn_.GetHash(key) % num_buckets_;

  for (size_t i = 0; i < num_buckets_; i++)
  {
    auto block_id = global_bucket_idx / BLOCK_ARRAY_SIZE;
    auto slot_id  = global_bucket_idx % BLOCK_ARRAY_SIZE;
    page_id_t block_page_id = header_page_->GetBlockPageId(block_id);
    HASH_TABLE_BLOCK_TYPE *block_page = reinterpret_cast<HASH_TABLE_BLOCK_TYPE *>(
        buffer_pool_manager_->FetchPage(block_page_id));

    if (!block_page->IsOccupied(slot_id))
    {
      return (result->size() > 0);
    }

    if (block_page->IsReadable(slot_id) && comparator_(key, block_page->KeyAt(slot_id)) == 0)
    {
      result->push_back(block_page->ValueAt(slot_id));
    }
    global_bucket_idx = (global_bucket_idx + 1) % num_buckets_;
  }

  return (result->size() > 0);
}
/*****************************************************************************
 * INSERTION
 *****************************************************************************/
template <typename KeyType, typename ValueType, typename KeyComparator>
bool HASH_TABLE_TYPE::Insert(
  Transaction *transaction, 
  const KeyType &key, 
  const ValueType &value)
{
  size_t global_bucket_idx = hash_fn_.GetHash(key) % num_buckets_;
  for (size_t i = 0; i < num_buckets_; i++)
  {
    size_t block_id = global_bucket_idx / BLOCK_ARRAY_SIZE;
    slot_offset_t slot_id = global_bucket_idx % BLOCK_ARRAY_SIZE;

    page_id_t block_page_id = header_page_->GetBlockPageId(block_id);
    HASH_TABLE_BLOCK_TYPE *block_page = reinterpret_cast<HASH_TABLE_BLOCK_TYPE *>(
        buffer_pool_manager_->FetchPage(block_page_id));
    
    if (block_page->IsReadable(slot_id) 
        && comparator_(key, block_page->KeyAt(slot_id)) == 0
        && value == block_page->ValueAt(slot_id)) 
    {
      return false;
    }

    if (block_page->Insert(slot_id, key,value))
    {
      sz_++;
      return true;
    }
    global_bucket_idx = (global_bucket_idx + 1) % num_buckets_;
  }
  return false;
}

/*****************************************************************************
* REMOVE
* 
* Deletes the associated value for the given key.
* @param transaction the current transaction
* @param key the key to delete
* @param value the value to delete
* @return true if remove succeeded, false otherwise
*****************************************************************************/
template <typename KeyType, typename ValueType, typename KeyComparator>
bool HASH_TABLE_TYPE::Remove(
  Transaction *transaction, 
  const KeyType &key,
  const ValueType &value)
{
  size_t global_bucket_idx = hash_fn_.GetHash(key) % num_buckets_;
  for (size_t i = 0; i < num_buckets_; i++)
  {
    auto block_id = global_bucket_idx / BLOCK_ARRAY_SIZE;
    auto slot_id  = global_bucket_idx % BLOCK_ARRAY_SIZE;
    page_id_t block_page_id = header_page_->GetBlockPageId(block_id);
    HASH_TABLE_BLOCK_TYPE *block_page = reinterpret_cast<HASH_TABLE_BLOCK_TYPE *>(
        buffer_pool_manager_->FetchPage(block_page_id));

    if (!block_page->IsOccupied(slot_id))
    {
      return false;
    }

    if (block_page->IsReadable(slot_id) 
      && comparator_(key, block_page->KeyAt(slot_id)) == 0
      && value == block_page->ValueAt(slot_id))
    {
      block_page->Remove(slot_id);
      sz_--;
      return true;
    }

    global_bucket_idx = (global_bucket_idx + 1) % num_buckets_;
  }

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
  return sz_;
}

template class LinearProbeHashTable<int, int, IntComparator>;

template class LinearProbeHashTable<GenericKey<4>, RID, GenericComparator<4>>;
template class LinearProbeHashTable<GenericKey<8>, RID, GenericComparator<8>>;
template class LinearProbeHashTable<GenericKey<16>, RID, GenericComparator<16>>;
template class LinearProbeHashTable<GenericKey<32>, RID, GenericComparator<32>>;
template class LinearProbeHashTable<GenericKey<64>, RID, GenericComparator<64>>;

}  // namespace bustub
