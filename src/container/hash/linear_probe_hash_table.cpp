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
HASH_TABLE_TYPE::LinearProbeHashTable(const std::string &name, 
                                      BufferPoolManager *buffer_pool_manager,
                                      const KeyComparator &comparator, 
                                      size_t num_buckets,
                                      HashFunction<KeyType> hash_fn)
    : buffer_pool_manager_(buffer_pool_manager), 
      comparator_(comparator),
      num_buckets_(num_buckets),
      size_(0),
      hash_fn_(std::move(hash_fn)) {
  auto h_page = buffer_pool_manager->NewPage(&header_page_id_);
  //auto header_page = reinterpret_cast<HashTableHeaderPage *>(h_page);
  HashTableHeaderPage *header_page = reinterpret_cast<HashTableHeaderPage *>(h_page);
  header_page->SetSize(0);
  auto num_blocks = (num_buckets - 1) / BLOCK_ARRAY_SIZE + 1;
  page_id_t block_page_id;
  for (size_t i = 0; i < num_blocks; i++) {
    buffer_pool_manager_->NewPage(&block_page_id);
    buffer_pool_manager_->UnpinPage(block_page_id, true);
    header_page->AddBlockPageId(block_page_id);
  }
  buffer_pool_manager->UnpinPage(header_page_id_, true);
}

/*****************************************************************************
 * SEARCH
 *****************************************************************************/
template <typename KeyType, typename ValueType, typename KeyComparator>
bool HASH_TABLE_TYPE::GetValue(Transaction *transaction, const KeyType &key, std::vector<ValueType> *result) {
  auto h_page = buffer_pool_manager_->FetchPage(header_page_id_);
  HashTableHeaderPage *header_page = reinterpret_cast<HashTableHeaderPage *>(h_page);
  auto table_offset = hash_fn_.GetHash(key) % num_buckets_;
  auto search_result = false;
  for (size_t i = 0; i < num_buckets_; i++) {
    auto block_id = table_offset / BLOCK_ARRAY_SIZE;
    auto block_page_id = header_page->GetBlockPageId(block_id);
    auto b_page = buffer_pool_manager_->FetchPage(block_page_id);
    HASH_TABLE_BLOCK_TYPE *block_page = reinterpret_cast<HASH_TABLE_BLOCK_TYPE *>(b_page);
    auto bucket_ind_in_block = table_offset % BLOCK_ARRAY_SIZE;
    if (!block_page->IsOccupied(bucket_ind_in_block)) {
      buffer_pool_manager_->UnpinPage(block_page_id, false);
      break;
    }
    if (block_page->IsReadable(bucket_ind_in_block)
        && comparator_(block_page->KeyAt(bucket_ind_in_block), key) == 0) {
      result->push_back(block_page->ValueAt(bucket_ind_in_block));
      search_result = true;
    }
    buffer_pool_manager_->UnpinPage(block_page_id, false);
    table_offset = (table_offset + 1) % num_buckets_;
  }
  buffer_pool_manager_->UnpinPage(header_page_id_, false);
  return search_result;
}
/*****************************************************************************
 * INSERTION
 *****************************************************************************/
template <typename KeyType, typename ValueType, typename KeyComparator>
bool HASH_TABLE_TYPE::Insert(Transaction *transaction, const KeyType &key, const ValueType &value) {
  if (size_ == num_buckets_) {
    Resize(size_);
  }
  auto h_page = buffer_pool_manager_->FetchPage(header_page_id_);
  HashTableHeaderPage *header_page = reinterpret_cast<HashTableHeaderPage *>(h_page);
  auto table_offset = hash_fn_.GetHash(key) % num_buckets_;
  auto insert_result = false;
  for (size_t i = 0; i < num_buckets_; i++) {
    auto block_id = table_offset / BLOCK_ARRAY_SIZE;
    auto block_page_id = header_page->GetBlockPageId(block_id);
    auto b_page = buffer_pool_manager_-> FetchPage(block_page_id);
    //HASH_TABLE_BLOCK_TYPE *block_page = reinterpret_cast<HASH_TABLE_BLOCK_TYPE *>(b_page);
    HASH_TABLE_BLOCK_TYPE *block_page = reinterpret_cast<HASH_TABLE_BLOCK_TYPE *>(b_page->GetData());
    auto bucket_ind_in_block = table_offset % BLOCK_ARRAY_SIZE;
    // Disallow duplicate values for the same key
    if (block_page->IsReadable(bucket_ind_in_block) 
        && comparator_(block_page->KeyAt(bucket_ind_in_block), key) == 0
        && block_page->ValueAt(bucket_ind_in_block) == value) {
      buffer_pool_manager_->UnpinPage(block_page_id, false);
      break;
    }
    if (block_page->Insert(bucket_ind_in_block, key, value)) {
      insert_result = true;
      size_++;
      header_page->SetSize(size_);
      buffer_pool_manager_->UnpinPage(block_page_id, true);
      break;
    }
    buffer_pool_manager_->UnpinPage(block_page_id, false);
    table_offset = (table_offset + 1) % num_buckets_;
  }
  buffer_pool_manager_->UnpinPage(header_page_id_, insert_result);
  return insert_result;
}

/*****************************************************************************
 * REMOVE
 *****************************************************************************/
template <typename KeyType, typename ValueType, typename KeyComparator>
bool HASH_TABLE_TYPE::Remove(Transaction *transaction, const KeyType &key, const ValueType &value) {
  auto h_page = buffer_pool_manager_->FetchPage(header_page_id_);
  HashTableHeaderPage *header_page = reinterpret_cast<HashTableHeaderPage *>(h_page);
  auto table_offset = hash_fn_.GetHash(key) % num_buckets_;
  auto remove_result = false;
  for (size_t i = 0; i < num_buckets_; i++) {
    auto block_id = table_offset / BLOCK_ARRAY_SIZE;
    auto block_page_id = header_page->GetBlockPageId(block_id);
    auto b_page = buffer_pool_manager_->FetchPage(block_page_id);
    HASH_TABLE_BLOCK_TYPE *block_page = reinterpret_cast<HASH_TABLE_BLOCK_TYPE *>(b_page);
    auto bucket_ind_in_block = table_offset % BLOCK_ARRAY_SIZE;
    if (!block_page->IsOccupied(bucket_ind_in_block)) {
      buffer_pool_manager_->UnpinPage(block_page_id, false);
      break;
    }
    if (block_page->IsReadable(bucket_ind_in_block)
        && comparator_(block_page->KeyAt(bucket_ind_in_block), key) == 0
        && block_page->ValueAt(bucket_ind_in_block) == value) {
      block_page->Remove(bucket_ind_in_block);
      remove_result = true;
      size_--;
      header_page->SetSize(size_);
      break;
    }
    buffer_pool_manager_->UnpinPage(block_page_id, remove_result);
    table_offset = (table_offset + 1) % num_buckets_;
  }
  buffer_pool_manager_->UnpinPage(header_page_id_, remove_result);
  return remove_result;
}

/*****************************************************************************
 * RESIZE
 *****************************************************************************/
template <typename KeyType, typename ValueType, typename KeyComparator>
void HASH_TABLE_TYPE::Resize(size_t initial_size) {
  page_id_t new_header_page_id;
  auto new_h_page = buffer_pool_manager_->NewPage(&new_header_page_id);
  HashTableHeaderPage *new_header_page = reinterpret_cast<HashTableHeaderPage *>(new_h_page);
  num_buckets_ = 2 * initial_size;
  new_header_page->SetSize(num_buckets_);
  auto new_num_blocks = (num_buckets_ - 1) / BLOCK_ARRAY_SIZE + 1;
  page_id_t block_page_id;
  for (size_t i = 0; i < new_num_blocks; i++) {
    buffer_pool_manager_->NewPage(&block_page_id);
    new_header_page->AddBlockPageId(block_page_id);
  }
  auto h_page = buffer_pool_manager_->FetchPage(header_page_id_);
  HashTableHeaderPage *header_page = reinterpret_cast<HashTableHeaderPage *>(h_page);
  size_ = 0;
  auto num_blocks = header_page->NumBlocks();
  for (size_t block_id = 0; block_id < num_blocks; block_id++) {
    block_page_id = header_page->GetBlockPageId(block_id);
    auto b_page = buffer_pool_manager_->FetchPage(block_page_id);
    HASH_TABLE_BLOCK_TYPE *block_page = reinterpret_cast<HASH_TABLE_BLOCK_TYPE *>(b_page);
    for (size_t bucket_ind_in_block = 0; bucket_ind_in_block < BLOCK_ARRAY_SIZE; bucket_ind_in_block++) {
      if (block_page->IsReadable(bucket_ind_in_block)) {
        Insert(nullptr, block_page->KeyAt(bucket_ind_in_block), block_page->ValueAt(bucket_ind_in_block));
      }
    }
    buffer_pool_manager_->DeletePage(block_page_id);
  }
  buffer_pool_manager_->DeletePage(header_page_id_);
  header_page_id_ = new_header_page_id;
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
