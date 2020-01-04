//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// linear_probe_hash_table.h
//
// Identification: src/include/container/hash/linear_probe_hash_table.h
//
// Copyright (c) 2015-2019, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#pragma once

#include <queue>
#include <string>
#include <vector>

#include "buffer/buffer_pool_manager.h"
#include "concurrency/transaction.h"
#include "container/hash/hash_function.h"
#include "container/hash/hash_table.h"
#include "storage/page/hash_table_block_page.h"
#include "storage/page/hash_table_header_page.h"
#include "storage/page/hash_table_page_defs.h"

namespace bustub {

#define HASH_TABLE_TYPE LinearProbeHashTable<KeyType, ValueType, KeyComparator>

/**
 * Implementation of linear probing hash table that is backed by a buffer pool
 * manager. Non-unique keys are supported. 
 * 
 * Supports insertions (Insert), point search (GetValue), and deletions (Remove).
 * 
 * The table dynamically grows once full.
 * 
 * Support both unique and non-unique keys. Duplicate values for the same key are not allowed. 
 * I.e. (key_0, value_0) and (key_0, value_1) can exist in the same table, but not (key_0, value_0) and (key_0, value_0)
 * 
 * Support multiple threads reading/writing the table at the same time.
 * 
 * Have latches on each block so that when one thread is writing to a block other threads are not reading 
 * or modifying that index as well. 
 * 
 * Allow multiple readers to be reading the same block at the same time.
 * 
 *  A latch the whole hash table when need to resize. When resize is called, the size that 
 *  the table was when resize was called is passed in as an argument. 
 *  This is so that if the table was resized while a thread was waiting for the latch, 
 *  it can immediately give up the latch and attempt insertion again.
 * 
 *  Do not use a global scope latch to protect data structure for each operation. 
 *  Do not lock the whole container and only unlock the latch when operations are done.
 * 
 * Deps:
 *  KeyType: 
 *    The type of each key in the hash table. This will only be GenericKey, 
 *    the actual size of GenericKey is specified and instantiated with a template argument 
 *    and depends on the data type of indexed attribute.
 * 
 * ValueType: 
 *    The type of each value in the hash table. This will only be 64-bit RID.
 * 
 * KeyComparator: 
 *    The class used to compare whether two KeyType instances are less/greater-than each other. 
 *    These are included in the KeyType implementation files.
 */
template <typename KeyType, typename ValueType, typename KeyComparator>
class LinearProbeHashTable : public HashTable<KeyType, ValueType, KeyComparator> {
 public:
    // member variable
    page_id_t header_page_id_;

  /**
   * Creates a new LinearProbeHashTable
   *
   * @param buffer_pool_manager buffer pool manager to be used
   * @param comparator comparator for keys
   * @param num_buckets initial number of buckets contained by this hash table
   * @param hash_fn the hash function
   */
  explicit LinearProbeHashTable (
      const std::string &name, 
      BufferPoolManager *buffer_pool_manager,
      const KeyComparator &comparator, 
      size_t num_buckets,
      HashFunction<KeyType> hash_fn);

  /**
   * Creates a new LinearProbeHashTable
   *
   * @param buffer_pool_manager buffer pool manager to be used
   * @param comparator comparator for keys
   * @param header_page the page id of the header page
   * @param hash_fn the hash function
   */
  explicit LinearProbeHashTable (
      BufferPoolManager *buffer_pool_manager,
      const KeyComparator &comparator,
      page_id_t header_page,
      HashFunction<KeyType> hash_fn
  );


  /**
   * Inserts a key-value pair into the hash table.
   * @param transaction the current transaction
   * @param key the key to create
   * @param value the value to be associated with the key
   * @return true if insert succeeded, false otherwise
   */
  bool Insert(
        Transaction *transaction, 
        const KeyType &key, 
        const ValueType &value) override;

  /**
   * Deletes the associated value for the given key.
   * @param transaction the current transaction
   * @param key the key to delete
   * @param value the value to delete
   * @return true if remove succeeded, false otherwise
   */
  bool Remove(Transaction *transaction, 
    const KeyType &key, 
    const ValueType &value) override;

  /**
   * Performs a point query on the hash table.
   * @param transaction the current transaction
   * @param key the key to look up
   * @param[out] result the value(s) associated with a given key
   * @return the value(s) associated with the given key
   */
  bool GetValue(Transaction *transaction, 
    const KeyType &key, 
    std::vector<ValueType> *result) override;

  /**
   * Resizes the table to at least twice the initial size provided.
   * @param initial_size the initial size of the hash table
   */
  void Resize(size_t initial_size);

  /**
   * Gets the size of the hash table
   * @return current size of the hash table
   */
  size_t GetSize();

 private:

  HashTableHeaderPage *header_page_;
  BufferPoolManager *buffer_pool_manager_;
  KeyComparator comparator_;

  // Number of key/value pairs.
  size_t sz_;

  // Number of blocks
  size_t num_pages_;

  // Capacity, number of buckets 
  size_t num_buckets_;

  // Readers includes inserts and removes, writer is only resize
  ReaderWriterLatch table_latch_;

  // Hash function
  HashFunction<KeyType> hash_fn_;
};

}  // namespace bustub
