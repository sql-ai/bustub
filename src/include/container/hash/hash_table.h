//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// hash_table.h
//
// Identification: src/include/container/hash/hash_table.h
//
// Copyright (c) 2015-2019, Carnegie Mellon University Database Group
//
// Hash table is meant to be accessed through the DBMS's BufferPoolManager. 
// This means that everything must be stored in disk pages so that they can read/written from the DiskManager.
//
// If a hash tableis created, its pages are written to disk, and if the DBMS is restarted, the same hash table 
// is loaded back from disk after restarting.
//
// To support reading/writing hash table blocks on top of pages, two Page classes are implemented to store the data of hash table. 
// Hash table allocate memory from the BufferPoolManager as pages.
// 
//===----------------------------------------------------------------------===//

#pragma once

#include <queue>
#include <string>
#include <vector>

#include "concurrency/transaction.h"
#include "storage/page/hash_table_page_defs.h"

namespace bustub {

template <typename KeyType, typename ValueType, typename KeyComparator>
class HashTable {
 public:
  virtual ~HashTable() = default;

  /**
   * Inserts a key-value pair into the hash table.
   * @param transaction the current transaction
   * @param key the key to create
   * @param value the value to be associated with the key
   * @return true if insert succeeded, false otherwise
   */
  virtual bool Insert(Transaction *transaction, const KeyType &key, const ValueType &value) = 0;

  /**
   * Deletes the associated value for the given key.
   * @param transaction the current transaction
   * @param key the key to delete
   * @param value the value to delete
   * @return true if remove succeeded, false otherwise
   */
  virtual bool Remove(Transaction *transaction, const KeyType &key, const ValueType &value) = 0;

  /**
   * Performs a point query on the hash table.
   * @param transaction the current transaction
   * @param key the key to look up
   * @param[out] result the value(s) associated with a given key
   * @return the value(s) associated with the given key
   */
  virtual bool GetValue(Transaction *transaction, const KeyType &key, std::vector<ValueType> *result) = 0;
};

}  // namespace bustub
