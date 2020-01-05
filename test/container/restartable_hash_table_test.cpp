//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// restartable_hash_table_test.cpp
//
// Identification: test/container/restartable_hash_table_test.cpp
//
// Copyright (c) 2015-2019, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include <thread>  // NOLINT
#include <vector>

#include "common/logger.h"
#include "container/hash/linear_probe_hash_table.h"
#include "gtest/gtest.h"
#include "murmur3/MurmurHash3.h"

constexpr size_t NUM_BUCKETS = 1100;

namespace bustub {

// NOLINTNEXTLINE
TEST(HashTableTest, EvictionTest) 
{
  auto *disk_manager = new DiskManager("EvictionTest.db");
  auto *bpm = new BufferPoolManager(2, disk_manager);

  LinearProbeHashTable<int, int, IntComparator> ht("blah", bpm, IntComparator(), NUM_BUCKETS, HashFunction<int>());  
  std::vector<int> keys;

  // insert a few values
  for (size_t i = 0; i < NUM_BUCKETS; i++) 
  {
    ht.Insert(nullptr, i, i);
    std::vector<int> res;
    ht.GetValue(nullptr, i, &res);
    EXPECT_EQ(1, res.size()) << "Failed to insert " << i << std::endl;
    EXPECT_EQ(i, res[0]);
  }

  // check if the inserted values are all there
  for (size_t i = 0; i < NUM_BUCKETS; i++) 
  {
    std::vector<int> res;
    ht.GetValue(nullptr, i, &res);
    EXPECT_EQ(1, res.size()) << "Failed to keep " << i << std::endl;
    EXPECT_EQ(i, res[0]);
  }
  
  bpm->FlushAllPages();  
  disk_manager->ShutDown();
  delete disk_manager;
  delete bpm;
}

// NOLINTNEXTLINE
TEST(HashTableTest, RestartTest) 
{
  auto *disk_manager = new DiskManager("EvictionTest.db");
  auto *bpm = new BufferPoolManager(2, disk_manager);
  page_id_t head_page = 0;
  LinearProbeHashTable<int, int, IntComparator> ht(bpm, IntComparator(), NUM_BUCKETS, head_page, HashFunction<int>());
  std::vector<int> keys;

  // check if hash-table is correct
  for (size_t i = 0; i < NUM_BUCKETS; i++) 
  {
    std::vector<int> res;
    ht.GetValue(nullptr, i, &res);
    EXPECT_EQ(1, res.size()) << "Failed to insert " << i << std::endl;
    EXPECT_EQ(i, res[0]);
  }

  disk_manager->ShutDown();
  delete disk_manager;
  delete bpm;
}

}  // namespace bustub
