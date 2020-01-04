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

namespace bustub {

// NOLINTNEXTLINE
TEST(HashTableTest, EvictionTest) 
{
  auto *disk_manager = new DiskManager("EvictionTest.db");
  auto *bpm = new BufferPoolManager(2, disk_manager);

  LinearProbeHashTable<int, int, IntComparator> ht("blah", bpm, IntComparator(), 1100, HashFunction<int>());  
  std::vector<int> keys;

  // insert a few values
  for (int i = 0; i < 1100; i++) 
  {
    ht.Insert(nullptr, i, i);
    std::vector<int> res;
    ht.GetValue(nullptr, i, &res);
    EXPECT_EQ(1, res.size()) << "Failed to insert " << i << std::endl;
    EXPECT_EQ(i, res[0]);
  }

  // check if the inserted values are all there
  for (int i = 0; i < 1100; i++) 
  {
    std::vector<int> res;
    ht.GetValue(nullptr, i, &res);
    EXPECT_EQ(1, res.size()) << "Failed to keep " << i << std::endl;
    EXPECT_EQ(i, res[0]);
  }
  
  disk_manager->ShutDown();
  delete disk_manager;
  delete bpm;
}

// NOLINTNEXTLINE
TEST(HashTableTest, DISABLED_RestartTest) 
{
  auto *disk_manager = new DiskManager("EvictionTest.db");
  auto *bpm = new BufferPoolManager(10, disk_manager);
  page_id_t head_page = 0;
  LinearProbeHashTable<int, int, IntComparator> ht(bpm, IntComparator(), head_page, HashFunction<int>());  
  std::vector<int> keys;

  // check if hash-table is correct
  for (int i = 0; i < 10000; i++) 
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
