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
HASH_TABLE_TYPE::LinearProbeHashTable(const std::string &name, BufferPoolManager *buffer_pool_manager,
                                      const KeyComparator &comparator, size_t num_buckets,
                                      HashFunction<KeyType> hash_fn)
    : buffer_pool_manager_(buffer_pool_manager), comparator_(comparator), hash_fn_(std::move(hash_fn)), num_buckets_(num_buckets), size_(0)
{
  Page *page = buffer_pool_manager_->NewPage(&header_page_id_);
  HashTableHeaderPage* header_page = reinterpret_cast<HashTableHeaderPage *>(page);
  header_page->SetSize(size_);
  header_page->SetPageId(header_page_id_);

  num_blocks_ = (num_buckets-1) / BLOCK_ARRAY_SIZE + 1;
  
  page_id_t block_page_id;
  for (size_t i = 0; i < num_blocks_; i++) {
    buffer_pool_manager_->NewPage(&block_page_id);
    buffer_pool_manager_->UnpinPage(block_page_id, false);
    header_page->AddBlockPageId(block_page_id);
  }

  buffer_pool_manager_->UnpinPage(header_page_id_, true);
}

/*****************************************************************************
 * SEARCH
 *****************************************************************************/
template <typename KeyType, typename ValueType, typename KeyComparator>
bool HASH_TABLE_TYPE::GetValue(Transaction *transaction, const KeyType &key, std::vector<ValueType> *result) {
  uint64_t hash = hash_fn_.GetHash(key) % num_buckets_;
  HashTableHeaderPage* header_page = reinterpret_cast<HashTableHeaderPage *>(buffer_pool_manager_->FetchPage(header_page_id_));
  for(size_t i = 0; i < num_buckets_; i++)
  {
    size_t idx = hash+i%num_buckets_;
    size_t block_idx = idx / BLOCK_ARRAY_SIZE;
    size_t bucket_idx = idx % BLOCK_ARRAY_SIZE;
    Page* page = buffer_pool_manager_->FetchPage(header_page->GetBlockPageId(block_idx));
    auto block_page =  reinterpret_cast<HashTableBlockPage<KeyType,ValueType,KeyComparator> *>(page);

    if(!block_page->IsOccupied(bucket_idx))
    {
      buffer_pool_manager_->UnpinPage(page->GetPageId(), false);
      break;
    }

    if(block_page->IsReadable(bucket_idx))
    {
      if(comparator_(block_page->KeyAt((bucket_idx)), key) == 0)
      {
        result->push_back(block_page->ValueAt(bucket_idx));
        buffer_pool_manager_->UnpinPage(page->GetPageId(), false);
        continue;
      }
    }
    buffer_pool_manager_->UnpinPage(page->GetPageId(), false);
  }
  buffer_pool_manager_->UnpinPage(header_page_id_, false);
  if(result->size() > 0) return true;
  return false;
}
/*****************************************************************************
 * INSERTION
 *****************************************************************************/
template <typename KeyType, typename ValueType, typename KeyComparator>
bool HASH_TABLE_TYPE::Insert(Transaction *transaction, const KeyType &key, const ValueType &value) {

  if(size_ == num_buckets_) Resize(num_buckets_);

  std::vector<ValueType> vals{};
  GetValue(transaction, key, &vals);
  for(ValueType val: vals)
  {
    if(val == value) return false; // value already in hashtable
  }

  uint64_t hash = hash_fn_.GetHash(key) % num_buckets_;
  HashTableHeaderPage* header_page = reinterpret_cast<HashTableHeaderPage *>(buffer_pool_manager_->FetchPage(header_page_id_));

  for(size_t i = 0; i < num_buckets_; i++)
  {
    size_t idx = hash+i%num_buckets_;
    size_t block_idx = idx / BLOCK_ARRAY_SIZE;
    size_t bucket_idx = idx % BLOCK_ARRAY_SIZE;
    Page* page = buffer_pool_manager_->FetchPage(header_page->GetBlockPageId(block_idx));
    auto block_page =  reinterpret_cast<HashTableBlockPage<KeyType,ValueType,KeyComparator> *>(page);

    if(!block_page->IsReadable(bucket_idx))
    {
      block_page->Insert(bucket_idx, key, value);
      buffer_pool_manager_->UnpinPage(page->GetPageId(), true);
      buffer_pool_manager_->UnpinPage(header_page_id_, false);
      size_++;
      return true;
    }
    buffer_pool_manager_->UnpinPage(page->GetPageId(), false);
  }
  buffer_pool_manager_->UnpinPage(header_page_id_, false);
  return false;
}

/*****************************************************************************
 * REMOVE
 *****************************************************************************/
template <typename KeyType, typename ValueType, typename KeyComparator>
bool HASH_TABLE_TYPE::Remove(Transaction *transaction, const KeyType &key, const ValueType &value) {
  uint64_t hash = hash_fn_.GetHash(key) % num_buckets_;
  HashTableHeaderPage* header_page = reinterpret_cast<HashTableHeaderPage *>(buffer_pool_manager_->FetchPage(header_page_id_));

  for(size_t i = 0; i < num_buckets_; i++)
  {
    size_t idx = hash+i%num_buckets_;
    size_t block_idx = idx / BLOCK_ARRAY_SIZE;
    size_t bucket_idx = idx % BLOCK_ARRAY_SIZE;
    Page* page = buffer_pool_manager_->FetchPage(header_page->GetBlockPageId(block_idx));
    auto block_page =  reinterpret_cast<HashTableBlockPage<KeyType,ValueType,KeyComparator> *>(page);

    if(!block_page->IsOccupied(bucket_idx))
    {
      buffer_pool_manager_->UnpinPage(page->GetPageId(), false);
      break;
    }
    if(block_page->IsReadable(bucket_idx))
    {
      if(comparator_(block_page->KeyAt((bucket_idx)), key) == 0)
      {
        if(block_page->ValueAt(bucket_idx) == value)
        {
          block_page->Remove(bucket_idx);
          buffer_pool_manager_->UnpinPage(page->GetPageId(), true);
          buffer_pool_manager_->UnpinPage(header_page_id_, false);
          size_--;
          return true;
        }
      }
    }
    buffer_pool_manager_->UnpinPage(page->GetPageId(), false);
  }
  buffer_pool_manager_->UnpinPage(header_page_id_, false);
  return false;
}

/*****************************************************************************
 * RESIZE
 *****************************************************************************/
template <typename KeyType, typename ValueType, typename KeyComparator>
void HASH_TABLE_TYPE::Resize(size_t initial_size) 
{
  num_buckets_ = initial_size * 2;
  num_blocks_ = num_buckets_ / BUFFER_POOL_SIZE + 1;

  page_id_t old_page_id = header_page_id_;
  HashTableHeaderPage* old_header = reinterpret_cast<HashTableHeaderPage *>(buffer_pool_manager_->FetchPage(header_page_id_));

  Page *page = buffer_pool_manager_->NewPage(&header_page_id_);
  HashTableHeaderPage* header_page = reinterpret_cast<HashTableHeaderPage *>(page);
  header_page->SetSize(size_);
  header_page->SetPageId(header_page_id_);
  
  page_id_t block_page_id;
  for (size_t i = 0; i < num_blocks_; i++) {
    buffer_pool_manager_->NewPage(&block_page_id);
    buffer_pool_manager_->UnpinPage(block_page_id, true);
    header_page->AddBlockPageId(block_page_id);
  }

  buffer_pool_manager_->UnpinPage(header_page_id_, true);

  size_t old_blocks = initial_size / BUFFER_POOL_SIZE + 1;
  for(size_t block_idx = 0; block_idx < old_blocks; block_idx++)
  {
    Page* page = buffer_pool_manager_->FetchPage(old_header->GetBlockPageId(block_idx));
    auto block_page =  reinterpret_cast<HashTableBlockPage<KeyType,ValueType,KeyComparator> *>(page);

    for(size_t bucket_idx = 0; bucket_idx < BUFFER_POOL_SIZE; bucket_idx++)
    {
      Insert(nullptr, block_page->KeyAt(bucket_idx), block_page->ValueAt(bucket_idx));
    }

    buffer_pool_manager_->UnpinPage(page->GetPageId(), false);
    buffer_pool_manager_->DeletePage(page->GetPageId());
  }
  
  buffer_pool_manager_->DeletePage(old_page_id);
}

/*****************************************************************************
 * GETSIZE
 *****************************************************************************/
template <typename KeyType, typename ValueType, typename KeyComparator>
size_t HASH_TABLE_TYPE::GetSize() {
  return size_;
}

template class LinearProbeHashTable<int, int, IntComparator>;

template class LinearProbeHashTable<GenericKey<4>, RID, GenericComparator<4>>;
template class LinearProbeHashTable<GenericKey<8>, RID, GenericComparator<8>>;
template class LinearProbeHashTable<GenericKey<16>, RID, GenericComparator<16>>;
template class LinearProbeHashTable<GenericKey<32>, RID, GenericComparator<32>>;
template class LinearProbeHashTable<GenericKey<64>, RID, GenericComparator<64>>;

}  // namespace bustub
