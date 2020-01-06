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
  
  header_page_ = reinterpret_cast<HashTableHeaderPage *>(buffer_pool_manager_->NewPage(&header_page_id_)->GetData());
  header_page_->SetSize(num_buckets);
  num_pages_ = (num_buckets_ -1) / BLOCK_ARRAY_SIZE + 1; 
  page_id_t block_page_id;
  for (size_t i = 0; i < num_pages_; i++)
  {
    Page *newBlockPage = buffer_pool_manager_->NewPage(&block_page_id);
    if (newBlockPage == nullptr)
    {
      LOG_ERROR("Expected error. Buffer pool manager NOT able to allocate new page....");
    }
    buffer_pool_manager_->UnpinPage(block_page_id, /*dirty*/ true);
    header_page_->AddBlockPageId(block_page_id);
  }
}

template <typename KeyType, typename ValueType, typename KeyComparator>
HASH_TABLE_TYPE::LinearProbeHashTable
(
    BufferPoolManager *buffer_pool_manager,
    const KeyComparator &comparator,    
    size_t num_buckets,
    page_id_t header_page,
    HashFunction<KeyType> hash_fn
) : buffer_pool_manager_(buffer_pool_manager),    
    comparator_(comparator),
    num_buckets_(num_buckets),
    header_page_id_(header_page),    
    hash_fn_(std::move(hash_fn))
{
  header_page_ = reinterpret_cast<HashTableHeaderPage *>(buffer_pool_manager_->FetchPage(header_page_id_)->GetData());
  num_pages_ = header_page_->NumBlocks();  
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
    page_id_t block_page_id = header_page_->GetBlockPageId(block_id);

    Page *page = buffer_pool_manager_->FetchPage(block_page_id);
    HASH_TABLE_BLOCK_TYPE *block_page = reinterpret_cast<HASH_TABLE_BLOCK_TYPE *>(page);

    auto slot_id  = global_bucket_idx % BLOCK_ARRAY_SIZE;

    page->RLatch();
    if (!block_page->IsOccupied(slot_id))
    {
      buffer_pool_manager_->UnpinPage(block_page_id, false);
      page->RUnlatch();
      return (result->size() > 0);
    }

    if (block_page->IsReadable(slot_id) && comparator_(key, block_page->KeyAt(slot_id)) == 0)
    {
      result->push_back(block_page->ValueAt(slot_id));
    }
    global_bucket_idx = (global_bucket_idx + 1) % num_buckets_;
    buffer_pool_manager_->UnpinPage(block_page_id, false);
    page->RUnlatch();
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
    // printf("BLOCK_ARRAY_SIZE: %lu ", BLOCK_ARRAY_SIZE);
    // printf("hash_fn_.GetHash(key): %lu ", hash_fn_.GetHash(key));

    size_t block_id = global_bucket_idx / BLOCK_ARRAY_SIZE;
    page_id_t block_page_id = header_page_->GetBlockPageId(block_id);    
    Page *page = buffer_pool_manager_->FetchPage(block_page_id);
    if (page == nullptr) {
      LOG_ERROR("Cannot fetch page %d", block_page_id);
      return false;
    }
    HASH_TABLE_BLOCK_TYPE *block_page = reinterpret_cast<HASH_TABLE_BLOCK_TYPE *>(page);
    slot_offset_t slot_id = global_bucket_idx % BLOCK_ARRAY_SIZE;  
    page->WLatch();
    if (block_page->IsReadable(slot_id) 
        && comparator_(key, block_page->KeyAt(slot_id)) == 0
        && value == block_page->ValueAt(slot_id)) 
    {
      buffer_pool_manager_->UnpinPage(block_page_id, false);
      page->WUnlatch();      
      return false;
    }

    if (block_page->Insert(slot_id, key,value))
    {
      sz_++;
      buffer_pool_manager_->UnpinPage(block_page_id, true);
      page->WUnlatch();
      return true;
    }

    global_bucket_idx = (global_bucket_idx + 1) % num_buckets_;    
    buffer_pool_manager_->UnpinPage(block_page_id, false);
    page->WUnlatch();
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
    page_id_t block_page_id = header_page_->GetBlockPageId(block_id);
    Page *page = buffer_pool_manager_->FetchPage(block_page_id);

    HASH_TABLE_BLOCK_TYPE *block_page = reinterpret_cast<HASH_TABLE_BLOCK_TYPE *>(page);
    auto slot_id  = global_bucket_idx % BLOCK_ARRAY_SIZE;
    
    page->WLatch();
    if (!block_page->IsOccupied(slot_id))
    {
      buffer_pool_manager_->UnpinPage(block_page_id, false);
      page->WUnlatch();
      return false;
    }

    if (block_page->IsReadable(slot_id) 
      && comparator_(key, block_page->KeyAt(slot_id)) == 0
      && value == block_page->ValueAt(slot_id))
    {
      block_page->Remove(slot_id);
      sz_--;
      buffer_pool_manager_->UnpinPage(block_page_id, true);
      page->WUnlatch();      
      return true;
    }

    buffer_pool_manager_->UnpinPage(block_page_id, false);
    page->WUnlatch();
    global_bucket_idx = (global_bucket_idx + 1) % num_buckets_;
  }

  return false;  
}

/*****************************************************************************
 * RESIZE
 *****************************************************************************/
template <typename KeyType, typename ValueType, typename KeyComparator>
void HASH_TABLE_TYPE::Resize(size_t initial_size) 
{

}

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
