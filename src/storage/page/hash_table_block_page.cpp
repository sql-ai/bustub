//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// hash_table_block_page.cpp
//
// Identification: src/storage/page/hash_table_block_page.cpp
//
// Copyright (c) 2015-2019, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "storage/page/hash_table_block_page.h"
#include "storage/index/generic_key.h"

namespace bustub {

template <typename KeyType, typename ValueType, typename KeyComparator>
KeyType HASH_TABLE_BLOCK_TYPE::KeyAt(slot_offset_t bucket_ind) const 
{
    return array_[bucket_ind].first;
}

/**
 * Gets the value at an index in the block.
 *
 * @param bucket_ind the index in the block to get the value at
 * @return value at index bucket_ind of the block
 */
template <typename KeyType, typename ValueType, typename KeyComparator>
ValueType HASH_TABLE_BLOCK_TYPE::ValueAt(slot_offset_t bucket_ind) const 
{
  return array_[bucket_ind].second;
}

/**
 * Attempts to insert a key and value into an index in the block.
 * The insert is thread safe. It uses compare and swap to claim the index,
 * and then writes the key and value into the index, and then marks the
 * index as readable.
 *
 * @param bucket_ind index to write the key and value to
 * @param key key to insert
 * @param value value to insert
 * @return If the value is inserted successfully, it returns true. If the
 * index is marked as occupied before the key and value can be inserted,
 * Insert returns false.
 */
template <typename KeyType, typename ValueType, typename KeyComparator>
bool HASH_TABLE_BLOCK_TYPE::Insert(
  slot_offset_t bucket_ind, 
  const KeyType &key, 
  const ValueType &value) 
{
  // TODO: Implement thread-safe version.
  int i = bucket_ind / 8;
  int k = bucket_ind % 8;

  // check if kth bit from ith byte is 1. 
  if (occupied_[i] & (1 << k))
  {
    return false;    
  }

  MappingType kv = std::make_pair(key, value);
  array_[bucket_ind] = kv;

  // Set kth bit from ith byte to 1. 
  occupied_[i] |= (1 << k);
  readable_[i] |= (1 << k);
  return true;
}

/**
 * Removes a key and value at index.
 *
 * @param bucket_ind ind to remove the value
 */
template <typename KeyType, typename ValueType, typename KeyComparator>
void HASH_TABLE_BLOCK_TYPE::Remove(slot_offset_t bucket_ind) 
{
  int i = bucket_ind / 8;
  int k = bucket_ind % 8;
  // set kth bit from ith byte to 0.
  readable_[i] &= ~(1 << k);
}

template <typename KeyType, typename ValueType, typename KeyComparator>
bool HASH_TABLE_BLOCK_TYPE::IsOccupied(slot_offset_t bucket_ind) const 
{
  int i = bucket_ind / 8;
  int k = bucket_ind % 8;

  // check if kth bit from ith byte is 1. 
  return (occupied_[i] & (1 << k));
}

template <typename KeyType, typename ValueType, typename KeyComparator>
bool HASH_TABLE_BLOCK_TYPE::IsReadable(slot_offset_t bucket_ind) const 
{
  int i = bucket_ind / 8;
  int k = bucket_ind % 8;

  // check if kth bit from ith byte is 1. 
  return (readable_[i] & (1 << k));
}

// DO NOT REMOVE ANYTHING BELOW THIS LINE
template class HashTableBlockPage<int, int, IntComparator>;
template class HashTableBlockPage<GenericKey<4>, RID, GenericComparator<4>>;
template class HashTableBlockPage<GenericKey<8>, RID, GenericComparator<8>>;
template class HashTableBlockPage<GenericKey<16>, RID, GenericComparator<16>>;
template class HashTableBlockPage<GenericKey<32>, RID, GenericComparator<32>>;
template class HashTableBlockPage<GenericKey<64>, RID, GenericComparator<64>>;

}  // namespace bustub
