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

// helper function to launch multiple threads
template <typename... Args>
void LaunchParallelTest(uint64_t num_threads, Args &&... args)
{
  std::vector<std::thread> thread_group;

  // Launch a group of threads
  for (uint64_t thread_itr = 0; thread_itr < num_threads; ++thread_itr)
  {
    thread_group.push_back(std::thread(args..., thread_itr));
  }

  // Join the threads with the main thread
  for (uint64_t thread_itr = 0; thread_itr < num_threads; ++thread_itr)
  {
    thread_group[thread_itr].join();
  }
}

// helper function to insert
void InsertHelper(LinearProbeHashTable<int, int, IntComparator> &ht,
                  const std::vector<int> &keys,
                  __attribute__((unused)) uint64_t thread_itr = 0)
{
  for (auto key : keys) {
    ht.Insert(nullptr, key, key * (thread_itr + 1));
  }
}


// NOLINTNEXTLINE
TEST(HashTableTest, SampleTest) {
  auto *disk_manager = new DiskManager("test.db");
  auto *bpm = new BufferPoolManager(2, disk_manager);

  LinearProbeHashTable<int, int, IntComparator> ht("blah", bpm, IntComparator(), 100000, HashFunction<int>());

  // insert a few values
  for (int i = 0; i < 5; i++) {
    ht.Insert(nullptr, i, i);
    std::vector<int> res;
    ht.GetValue(nullptr, i, &res);
    EXPECT_EQ(1, res.size()) << "Failed to insert " << i << std::endl;
    EXPECT_EQ(i, res[0]);
  }

  // check if the inserted values are all there
  for (int i = 0; i < 5; i++) {
    std::vector<int> res;
    ht.GetValue(nullptr, i, &res);
    EXPECT_EQ(1, res.size()) << "Failed to keep " << i << std::endl;
    EXPECT_EQ(i, res[0]);
  }

  // insert one more value for each key
  for (int i = 0; i < 5; i++) {
    if (i == 0) {
      // duplicate values for the same key are not allowed
      EXPECT_FALSE(ht.Insert(nullptr, i, 2 * i));
    } else {
      EXPECT_TRUE(ht.Insert(nullptr, i, 2 * i));
    }
    ht.Insert(nullptr, i, 2 * i);
    std::vector<int> res;
    ht.GetValue(nullptr, i, &res);
    if (i == 0) {
      // duplicate values for the same key are not allowed
      EXPECT_EQ(1, res.size());
      EXPECT_EQ(i, res[0]);
    } else {
      EXPECT_EQ(2, res.size());
      if (res[0] == i) {
        EXPECT_EQ(2 * i, res[1]);
      } else {
        EXPECT_EQ(2 * i, res[0]);
        EXPECT_EQ(i, res[1]);
      }
    }
  }

  // look for a key that does not exist
  std::vector<int> res;
  ht.GetValue(nullptr, 20, &res);
  EXPECT_EQ(0, res.size());

  // delete some values
  for (int i = 0; i < 5; i++) {
    EXPECT_TRUE(ht.Remove(nullptr, i, i));
    std::vector<int> res;
    ht.GetValue(nullptr, i, &res);
    if (i == 0) {
      // (0, 0) is the only pair with key 0
      EXPECT_EQ(0, res.size());
    } else {
      EXPECT_EQ(1, res.size());
      EXPECT_EQ(2 * i, res[0]);
    }
  }

  // delete all values
  for (int i = 0; i < 5; i++) {
    if (i == 0) {
      // (0, 0) has been deleted
      EXPECT_FALSE(ht.Remove(nullptr, i, 2 * i));
    } else {
      EXPECT_TRUE(ht.Remove(nullptr, i, 2 * i));
    }
  }
  disk_manager->ShutDown();
  remove("test.db");
  delete disk_manager;
  delete bpm;
}

TEST(HashTableTest, RestartTest) {
  auto *disk_manager = new DiskManager("test.db");
  auto *bpm = new BufferPoolManager(2, disk_manager);

  LinearProbeHashTable<int, int, IntComparator> ht("blah", bpm, IntComparator(), 100000, HashFunction<int>());

  // insert a few values
  for (int i = 0; i < 5; i++) {
    ht.Insert(nullptr, i, i);
    std::vector<int> res;
    ht.GetValue(nullptr, i, &res);
    EXPECT_EQ(1, res.size()) << "Failed to insert " << i << std::endl;
    EXPECT_EQ(i, res[0]);
  }

  // check if the inserted values are all there
  for (int i = 0; i < 5; i++) {
    std::vector<int> res;
    ht.GetValue(nullptr, i, &res);
    EXPECT_EQ(1, res.size()) << "Failed to keep " << i << std::endl;
    EXPECT_EQ(i, res[0]);
  }

  // insert one more value for each key
  for (int i = 0; i < 5; i++) {
    if (i == 0) {
      // duplicate values for the same key are not allowed
      EXPECT_FALSE(ht.Insert(nullptr, i, 2 * i));
    } else {
      EXPECT_TRUE(ht.Insert(nullptr, i, 2 * i));
    }
    ht.Insert(nullptr, i, 2 * i);
    std::vector<int> res;
    ht.GetValue(nullptr, i, &res);
    if (i == 0) {
      // duplicate values for the same key are not allowed
      EXPECT_EQ(1, res.size());
      EXPECT_EQ(i, res[0]);
    } else {
      EXPECT_EQ(2, res.size());
      if (res[0] == i) {
        EXPECT_EQ(2 * i, res[1]);
      } else {
        EXPECT_EQ(2 * i, res[0]);
        EXPECT_EQ(i, res[1]);
      }
    }
  }

  bpm->FlushAllPages();
  auto *bpm2 = new BufferPoolManager(2, disk_manager);

  LinearProbeHashTable<int, int, IntComparator> ht2("blah", bpm2, IntComparator(), 100000, HashFunction<int>(), 0);

  // look for a key that does not exist
  std::vector<int> res;
  ht2.GetValue(nullptr, 20, &res);
  EXPECT_EQ(0, res.size());

  // delete some values
  for (int i = 0; i < 5; i++) {
    EXPECT_TRUE(ht2.Remove(nullptr, i, i));
    std::vector<int> res;
    ht2.GetValue(nullptr, i, &res);
    if (i == 0) {
      // (0, 0) is the only pair with key 0
      EXPECT_EQ(0, res.size());
    } else {
      EXPECT_EQ(1, res.size());
      EXPECT_EQ(2 * i, res[0]);
    }
  }

  // delete all values
  for (int i = 0; i < 5; i++) {
    if (i == 0) {
      // (0, 0) has been deleted
      EXPECT_FALSE(ht2.Remove(nullptr, i, 2 * i));
    } else {
      EXPECT_TRUE(ht2.Remove(nullptr, i, 2 * i));
    }
  }
  disk_manager->ShutDown();
  remove("test.db");
  delete disk_manager;
  delete bpm;
  delete bpm2;
}

TEST(HashTableTest, ConcurrentTest) { 
  auto *disk_manager = new DiskManager("test.db");
  auto *bpm = new BufferPoolManager(3, disk_manager);

  LinearProbeHashTable<int, int, IntComparator> ht("blah", bpm, IntComparator(), 500, HashFunction<int>());

  int scale_factor = 111;
  int num_threads = 5;
  std::vector<int> keys;
  for (int i = 1; i <= scale_factor; i++) {
    keys.push_back(i);
  }

  LaunchParallelTest(num_threads, InsertHelper, std::ref(ht), keys);

  for (auto key : keys) {
    std::vector<int> res;
    ht.GetValue(nullptr, key, &res);
    EXPECT_EQ(num_threads, res.size()) << "Fail on key " << key << " (" << res.size() << " / " << num_threads << " found)\n";
    std::sort(res.begin(), res.end());
    for (size_t i = 0; i < res.size(); i++) {
      EXPECT_EQ(key * (i + 1), res[i]);
    }
  }


  disk_manager->ShutDown();
  remove("test.db");
  delete disk_manager;
  delete bpm;
}

}  // namespace bustub
