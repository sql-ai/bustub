//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// hash_table_header_page.h
//
// Identification: src/include/storage/page/hash_table_header_page.h
//
// Copyright (c) 2015-2019, Carnegie Mellon University Database Group
//
// Hold all of the meta-data for the hash table.
// 
//===----------------------------------------------------------------------===//

#pragma once

#include <cassert>
#include <climits>
#include <cstdlib>
#include <string>

#include "storage/index/generic_key.h"
#include "storage/page/hash_table_page_defs.h"

namespace bustub {

/**
 *
 * Header Page for linear probing hash table.
 *
 * Header format (size in byte, 16 bytes in total):
 * -------------------------------------------------------------
 * | LSN (4) | Size (4) | PageId(4) | NextBlockIndex(4)
 * -------------------------------------------------------------
 */
class HashTableHeaderPage {
 public:
  /**
   * @return the number of buckets in the hash table;
   */
  size_t GetSize() const;

  /**
   * Sets the size field of the hash table to size
   *
   * @param size the size for the size field to be set to
   */
  void SetSize(size_t size);

  /**
   * @return the page ID of this page
   */
  page_id_t GetPageId() const;

  /**
   * Sets the page ID of this page
   *
   * @param page_id the page id for the page id field to be set to
   */
  void SetPageId(page_id_t page_id);

  /**
   * @return the lsn of this page
   */
  lsn_t GetLSN() const;

  /**
   * Sets the LSN of this page
   *
   * @param lsn the log sequence number for the lsn field to be set to
   */
  void SetLSN(lsn_t lsn);

  /**
   * Adds a block page_id to the end of header page
   *
   * @param page_id page_id to be added
   */
  void AddBlockPageId(page_id_t page_id);

  /**
   * Returns the page_id of the index-th block
   *
   * @param index the index of the block
   * @return the page_id for the block.
   */
  page_id_t GetBlockPageId(size_t index);

  /**
   * @return the number of blocks currently stored in the header page
   */
  size_t NumBlocks();

 private:
  // ================================================================================
  // Variable Name	 | Size       | Description
  // ================================================================================
  // Page_id_        | 4 bytes    | Self Page Id
  // size_           | 4 bytes    | Number of Key & Value pairs the hash table can hold
  // next_ind_       | 4 bytes    | The next index to add a new entry to block_page_ids_
  // lsn_            | 4 bytes    | Log sequence number (Used in Project 4)
  // block_page_ids_ | 4080 bytes | Array of block page_id_t
  // ================================================================================
  //
  // The block_page_ids_ array maps block ids to page_id_t ids. 
  // The ith element in block_page_ids_ is the page_id for the ith block.
  lsn_t lsn_;
  size_t size_;
  page_id_t page_id_;
  size_t next_ind_;
  page_id_t block_page_ids_[0];
};

}  // namespace bustub
