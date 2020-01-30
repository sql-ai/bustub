//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// hash_table_test.cpp
//
// Identification: test/container/hash_table_test.cpp
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
TEST(HashTableTest, SampleTest) {
  auto *disk_manager = new DiskManager("test.db");
  auto *bpm = new BufferPoolManager(50, disk_manager);

  LinearProbeHashTable<int, int, IntComparator> ht("blah", bpm, IntComparator(), 1000, HashFunction<int>());
  EXPECT_EQ(0, ht.GetSize());

  // insert a few values
  for (int i = 0; i < 5; i++) {
    EXPECT_TRUE(ht.Insert(nullptr, i, i));
    EXPECT_EQ(i + 1, ht.GetSize());
    std::vector<int> res;
    EXPECT_TRUE(ht.GetValue(nullptr, i, &res));
    EXPECT_EQ(1, res.size()) << "Failed to insert " << i << std::endl;
    EXPECT_EQ(i, res[0]);
  }

  // check if the inserted values are all there
  for (int i = 0; i < 5; i++) {
    std::vector<int> res;
    EXPECT_TRUE(ht.GetValue(nullptr, i, &res));
    EXPECT_EQ(1, res.size()) << "Failed to keep " << i << std::endl;
    EXPECT_EQ(i, res[0]);
  }
  EXPECT_EQ(5, ht.GetSize());

  // insert one more value for each key
  for (int i = 0; i < 5; i++) {
    if (i == 0) {
      // duplicate values for the same key are not allowed
      EXPECT_FALSE(ht.Insert(nullptr, i, 2 * i));
      EXPECT_EQ(5, ht.GetSize());
    } else {
      EXPECT_TRUE(ht.Insert(nullptr, i, 2 * i));
      EXPECT_EQ(5 + i, ht.GetSize());
    }
    EXPECT_FALSE(ht.Insert(nullptr, i, 2 * i));
    std::vector<int> res;
    EXPECT_TRUE(ht.GetValue(nullptr, i, &res));
    if (i == 0) {
      // duplicate values for the same key are not allowed
      EXPECT_EQ(1, res.size());
      EXPECT_EQ(i, res[0]);
    } else {
      EXPECT_EQ(2, res.size());
      if (i == res[0]) {
        EXPECT_EQ(2 * i, res[1]);
      } else {
        EXPECT_EQ(2 * i, res[0]);
        EXPECT_EQ(i, res[1]);
      }
    }
  }
  EXPECT_EQ(9, ht.GetSize());

  // look for a key that does not exist
  std::vector<int> res;
  EXPECT_FALSE(ht.GetValue(nullptr, 20, &res));
  EXPECT_EQ(0, res.size());

  // delete some values
  for (int i = 0; i < 5; i++) {
    EXPECT_TRUE(ht.Remove(nullptr, i, i));
    EXPECT_EQ(9 - (i + 1), ht.GetSize());
    std::vector<int> res;
    if (i == 0) {
      // (0, 0) is the only pair with key 0
      EXPECT_FALSE(ht.GetValue(nullptr, i, &res));
      EXPECT_EQ(0, res.size());
    } else {
      EXPECT_TRUE(ht.GetValue(nullptr, i, &res));
      EXPECT_EQ(1, res.size());
      EXPECT_EQ(2 * i, res[0]);
    }
  }
  EXPECT_EQ(4, ht.GetSize());

  // delete all values
  for (int i = 0; i < 5; i++) {
    if (i == 0) {
      // (0, 0) has been deleted
      EXPECT_FALSE(ht.Remove(nullptr, i, 2 * i));
      EXPECT_EQ(4, ht.GetSize());
    } else {
      EXPECT_TRUE(ht.Remove(nullptr, i, 2 * i));
      EXPECT_EQ(4 - i, ht.GetSize());
    }
    std::vector<int> res;
    EXPECT_FALSE(ht.GetValue(nullptr, i, &res));
    EXPECT_EQ(0, res.size());
  }
  EXPECT_EQ(0, ht.GetSize());

  disk_manager->ShutDown();
  remove("test.db");
  delete disk_manager;
  delete bpm;
}

}  // namespace bustub
