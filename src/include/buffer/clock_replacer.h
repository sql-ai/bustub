//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// clock_replacer.h
//
// Identification: src/include/buffer/clock_replacer.h
//
// Copyright (c) 2015-2019, Carnegie Mellon University Database Group
//

// This component is responsible for tracking page usage in the buffer pool.
// ClockReplacer, clock_replacer.h, and its corresponding implementation clock_replacer.cpp extends the abstract Replacer class 
// src/include/buffer/replacer.h, which contains the function specifications.

// The size of the ClockReplacer is the same as buffer pool since it contains placeholders for all of the frames in the BufferPoolManager.
// However, not all the frames are considered as in the ClockReplacer.

// The ClockReplacer is initialized to have no frame in it.
// Then, only the newly unpinned ones will be considered in the ClockReplacer.

// Adding or removing a frame from a replacer is implemented by changing a reference bit of a frame.
// The clock hand initially points to the placeholder of frame 0. For each frame, ClockReplacer tracks two things:
//    1. Is this frame currently in the ClockReplacer?
//    2. Has this frame recently been unpinned (ref flag)?
// In some scenarios, the two are the same. 
//  For example, when unpin a page, both of the above are true.
//  However, the frame stays in the ClockReplacer until it is pinned or victimized, but its ref flag is modified by the clock hand.

// This implementation is memory safe and thread-safe.

//===----------------------------------------------------------------------===//

#pragma once

#include <list>
#include <mutex>  // NOLINT
#include <vector>

#include "buffer/replacer.h"
#include "common/config.h"

namespace bustub {

/**
 * ClockReplacer implements the clock replacement policy, which approximates the Least Recently Used policy.
 */
class ClockReplacer : public Replacer {
 public:
  /**
   * Create a new ClockReplacer.
   * @param num_pages the maximum number of pages the ClockReplacer will be required to store.
   */
  explicit ClockReplacer(size_t num_pages);

  /**
   * Destroys the ClockReplacer.
   */
  ~ClockReplacer() override;

/**
 * Find the first frame that is both in the ClockReplacer and with its ref flag set to false.
 * @param[out] frame_id id of frame that was removed, nullptr if no victim was found
 * @return true if a victim frame was found, false otherwise
 * 
 * If a frame is in the ClockReplacer, but its ref flag is set to true, change it to false instead.
 * This is the only method that updates the clock hand.
 */
  bool Victim(frame_id_t *frame_id) override;

/** 
 * Pin(frame_id_t frame_id) is called after a page is pinned to a frame in the BufferPoolManager. 
 * It should remove the frame containing the pinned page from the ClockReplacer.
 * 
 */
  void Pin(frame_id_t frame_id) override;

/**
 * Unpin(frame_id_t frame_id) is called when the pin_count of a page becomes 0.
 * This method should add the frame containing the unpinned page to the ClockReplacer
 * 
 */
  void Unpin(frame_id_t frame_id) override;
  size_t Size() override;

 private:
  struct Bits
  {
    uint8_t in_replacer : 1;
    uint8_t ref : 1;
  };

  size_t size_;
  size_t hand_;
  std::vector<Bits> clock_;
};



}  // namespace bustub
