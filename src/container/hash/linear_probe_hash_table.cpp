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
    : buffer_pool_manager_(buffer_pool_manager),
      comparator_(comparator),
      num_buckets_(num_buckets),
      size_(0),
      hash_fn_(std::move(hash_fn)) {
  auto header_page = reinterpret_cast<HashTableHeaderPage *>(buffer_pool_manager_->NewPage(&header_page_id_));
  header_page->SetSize(num_buckets);
  num_blocks_ = (num_buckets - 1) / BLOCK_ARRAY_SIZE + 1;
  page_id_t block_page_id;
  for (size_t i = 0; i < num_blocks_; i++) {
    buffer_pool_manager_->NewPage(&block_page_id);
    header_page->AddBlockPageId(block_page_id);
  }
}

/*****************************************************************************
 * SEARCH
 *****************************************************************************/
template <typename KeyType, typename ValueType, typename KeyComparator>
bool HASH_TABLE_TYPE::GetValue(Transaction *transaction, const KeyType &key, std::vector<ValueType> *result) {
  auto header_page = reinterpret_cast<HashTableHeaderPage *>(buffer_pool_manager_->FetchPage(header_page_id_));
  size_t offset = hash_fn_.GetHash(key) % num_buckets_;
  for (size_t i = 0; i < num_buckets_; i++) {
    size_t block_id = offset / BLOCK_ARRAY_SIZE;
    slot_offset_t bucket_ind = offset % BLOCK_ARRAY_SIZE;
    page_id_t current_page_id = header_page->GetBlockPageId(block_id);
    HASH_TABLE_BLOCK_TYPE *block_page =
        reinterpret_cast<HASH_TABLE_BLOCK_TYPE *>(buffer_pool_manager_->FetchPage(current_page_id));
    if (!block_page->IsOccupied(bucket_ind)) {
      break;
    }
    if (block_page->IsReadable(bucket_ind) && comparator_(block_page->KeyAt(bucket_ind), key) == 0) {
      result->push_back(block_page->ValueAt(bucket_ind));
    }

    offset = (offset + 1) % num_buckets_;
  }
  return true;
}

/*****************************************************************************
 * INSERTION
 *****************************************************************************/
template <typename KeyType, typename ValueType, typename KeyComparator>
bool HASH_TABLE_TYPE::Insert(Transaction *transaction, const KeyType &key, const ValueType &value) {
  if (size_ == num_buckets_) {
    Resize(size_);
  }

  auto header_page = reinterpret_cast<HashTableHeaderPage *>(buffer_pool_manager_->FetchPage(header_page_id_));
  size_t offset = hash_fn_.GetHash(key) % num_buckets_;
  for (size_t i = 0; i < num_buckets_; i++) {
    size_t block_id = offset / BLOCK_ARRAY_SIZE;
    slot_offset_t bucket_ind = offset % BLOCK_ARRAY_SIZE;
    page_id_t current_page_id = header_page->GetBlockPageId(block_id);
    HASH_TABLE_BLOCK_TYPE *block_page =
        reinterpret_cast<HASH_TABLE_BLOCK_TYPE *>(buffer_pool_manager_->FetchPage(current_page_id));

    // Do not allow duplicate values for the same key
    if (block_page->IsReadable(bucket_ind) && comparator_(block_page->KeyAt(bucket_ind), key) == 0) {
      if (block_page->ValueAt(bucket_ind) == value) {
        return false;
      }
    }
    if (!block_page->IsOccupied(bucket_ind)) {
      block_page->Insert(bucket_ind, key, value);
      size_++;
      return true;
    }

    offset = (offset + 1) % num_buckets_;
  }
  return false;
}

/*****************************************************************************
 * REMOVE
 *****************************************************************************/
template <typename KeyType, typename ValueType, typename KeyComparator>
bool HASH_TABLE_TYPE::Remove(Transaction *transaction, const KeyType &key, const ValueType &value) {
  auto header_page = reinterpret_cast<HashTableHeaderPage *>(buffer_pool_manager_->FetchPage(header_page_id_));
  size_t offset = hash_fn_.GetHash(key) % num_buckets_;
  for (size_t i = 0; i < num_buckets_; i++) {
    size_t block_id = offset / BLOCK_ARRAY_SIZE;
    slot_offset_t bucket_ind = offset % BLOCK_ARRAY_SIZE;
    page_id_t current_page_id = header_page->GetBlockPageId(block_id);
    HASH_TABLE_BLOCK_TYPE *block_page =
        reinterpret_cast<HASH_TABLE_BLOCK_TYPE *>(buffer_pool_manager_->FetchPage(current_page_id));
    if (!block_page->IsOccupied(bucket_ind)) {
      break;
    }
    if (block_page->IsReadable(bucket_ind) && comparator_(block_page->KeyAt(bucket_ind), key) == 0) {
      if (block_page->ValueAt(bucket_ind) == value) {
        block_page->Remove(bucket_ind);
        size_--;
        return true;
      }
    }

    offset = (offset + 1) % num_buckets_;
  }
  return false;
}

/*****************************************************************************
 * RESIZE
 *****************************************************************************/
template <typename KeyType, typename ValueType, typename KeyComparator>
void HASH_TABLE_TYPE::Resize(size_t initial_size) {
  page_id_t old_header_page_id = header_page_id_;
  auto old_header_page = reinterpret_cast<HashTableHeaderPage *>(buffer_pool_manager_->FetchPage(old_header_page_id));

  num_buckets_ = 2 * initial_size;
  num_blocks_ = (num_buckets_ - 1) / BLOCK_ARRAY_SIZE + 1;

  auto header_page = reinterpret_cast<HashTableHeaderPage *>(buffer_pool_manager_->NewPage(&header_page_id_));
  header_page->SetSize(num_buckets_);
  page_id_t block_page_id;
  for (size_t i = 0; i < num_blocks_; i++) {
    buffer_pool_manager_->NewPage(&block_page_id);
    header_page->AddBlockPageId(block_page_id);
  }

  for (size_t i = 0; i < old_header_page->NumBlocks(); i++) {
    HASH_TABLE_BLOCK_TYPE *old_block_page =
        reinterpret_cast<HASH_TABLE_BLOCK_TYPE *>(buffer_pool_manager_->FetchPage(old_header_page->GetBlockPageId(i)));
    for (slot_offset_t bucket_ind = 0; bucket_ind < BLOCK_ARRAY_SIZE; bucket_ind++) {
      if (old_block_page->IsReadable(bucket_ind)) {
        Insert(nullptr, old_block_page->KeyAt(bucket_ind), old_block_page->ValueAt(bucket_ind));
      }
    }
    buffer_pool_manager_->DeletePage(old_header_page->GetBlockPageId(i));
  }
  buffer_pool_manager_->DeletePage(old_header_page_id);
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
