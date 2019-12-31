//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// concurrent_hash_table_test.cpp
//
// Identification: test/container/concurrent_hash_table_test.cpp
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
                  uint64_t thread_itr = 0)
{
  LOG_INFO("InsertHelper: Thread: %lu", thread_itr);
  for (size_t i = 0; i < keys.size(); i++)
  {
    int k = keys[i];
    int v = k*(thread_itr+1);
    ht.Insert(nullptr, k, v);
  }
}

// NOLINTNEXTLINE
TEST(ConcurrentHashTableTest, ConcurrentTest) 
{
  auto *disk_manager = new DiskManager("test.db");
  auto *bpm = new BufferPoolManager(100, disk_manager);

  LinearProbeHashTable<int, int, IntComparator> ht("blah", bpm, IntComparator(), 10000, HashFunction<int>());  
  std::vector<int> keys;

  for (int i = 0; i < 111; i++)
  {
    keys.push_back(i);
  }

  LaunchParallelTest(90, InsertHelper, std::ref(ht), keys);

  for (auto key: keys)
  {
    if (key > 0)
    {
      std::vector<int> res;
      ht.GetValue(nullptr, key, &res);
      std::string s;
      for (auto x : res)
      {
        s = s + ", " + std::to_string(x);
      }
      EXPECT_EQ(90, res.size()) << "Failed key  " << key << ", values " << s << std::endl;;
    }
  }
  
  disk_manager->ShutDown();
  remove("test.db");
  delete disk_manager;
  delete bpm;
}

}  // namespace bustub
