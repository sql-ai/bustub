//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// hash_table_block_page.h
//
// Identification: src/include/storage/page/hash_table_block_page.h
//
// Copyright (c) 2015-2019, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#pragma once

#include <atomic>
#include <utility>
#include <vector>

#include "common/config.h"
#include "storage/index/int_comparator.h"
#include "storage/page/hash_table_page_defs.h"

namespace bustub {
/**
 * Store indexed key and and value together within block page. Supports
 * non-unique keys.
 *
 * Block page format (keys are stored in order):
 *  ----------------------------------------------------------------
 * | KEY(1) + VALUE(1) | KEY(2) + VALUE(2) | ... | KEY(n) + VALUE(n)
 *  ----------------------------------------------------------------
 *
 *  Here '+' means concatenation.
 *
 * The Hash Table Block Page holds three arrays:
 *    occupied_ : The ith bit of occupied_ is 1 if the ith index of array_ has ever been occupied.
 *    readable_ : The ith bit of readable_ is 1 if the ith index of array_ holds a readable value.
 *    array_ : The array that holds the key-value pairs.
 * 
 * The number of slots available in a Hash Table Block Page depends on the types of the keys and values being stored. 
 * Only support fixed-length keys and values. 
 * 
 * The size of keys/values will be the same within a single hash table instance, but NOT the same for all instances 
 *  e.g. hash table #1 can have 32-bit keys and hash table #2 can have 64-bit keys.
 * 
 * Each Hash Table Header page and each  Hash Table Block page corresponds to the content (i.e., the byte array data_) 
 * of a memory page fetched by buffer pool. 
 * 
 * Before trying to read or write a page, first fetch the page from buffer pool using its page_id, 
 * then reinterpret cast to either a header or a block page, and unpin the page after any writing or reading operations.
 */
template <typename KeyType, typename ValueType, typename KeyComparator>
class HashTableBlockPage {
 public:
  // Delete all constructor / destructor to ensure memory safety
  HashTableBlockPage() = delete;

  /**
   * Gets the key at an index in the block.
   *
   * @param bucket_ind the index in the block to get the key at
   * @return key at index bucket_ind of the block
   */
  KeyType KeyAt(slot_offset_t bucket_ind) const;

  /**
   * Gets the value at an index in the block.
   *
   * @param bucket_ind the index in the block to get the value at
   * @return value at index bucket_ind of the block
   */
  ValueType ValueAt(slot_offset_t bucket_ind) const;

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
  bool Insert(slot_offset_t bucket_ind, const KeyType &key, const ValueType &value);

  /**
   * Removes a key and value at index.
   *
   * @param bucket_ind ind to remove the value
   */
  void Remove(slot_offset_t bucket_ind);

  /**
   * Returns whether or not an index is occupied (key/value pair or tombstone)
   *
   * @param bucket_ind index to look at
   * @return true if the index is occupied, false otherwise
   */
  bool IsOccupied(slot_offset_t bucket_ind) const;

  /**
   * Returns whether or not an index is readable (valid key/value pair)
   *
   * @param bucket_ind index to look at
   * @return true if the index is readable, false otherwise
   */
  bool IsReadable(slot_offset_t bucket_ind) const;

 private:
  std::atomic_char occupied_[(BLOCK_ARRAY_SIZE - 1) / 8 + 1];
  std::atomic_char readable_[(BLOCK_ARRAY_SIZE - 1) / 8 + 1];
  MappingType array_[BLOCK_ARRAY_SIZE];
};

}  // namespace bustub
