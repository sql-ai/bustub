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
 *
 * The size of ClockReplacer is the same as buffer pool since it contains placeholders for all of the frames in the
 * BufferPoolManager. However, not all the frames are considered as in the ClockReplacer. The ClockReplacer is
 * initialized to have no frame in it. Then, only the newly unpinnned ones will be considered in the ClockReplacer.
 * Adding a frame to or removing a frame from a replacer is implemented by changing a reference bit of frame. The clock
 * hand initially points to the placeholder of frame 0. For each frame, it keeps track of two thing: 1. Is this frame
 * currently in the ClockReplacer? 2. Has this frame recently been unpinned (ref flag)?
 */
class ClockReplacer : public Replacer {
 public:
  /**
   * @brief Construct a new Clock Replacer object
   * 
   * @param num_pages the maximum number of pages the ClockReplacer will be required to store
   */
  explicit ClockReplacer(size_t num_pages);

  /**
   * @brief Destroy the Clock Replacer object
   * 
   */
  ~ClockReplacer() override;

  /**
   * @brief Starting from the current position of the clock hand, find the first frame that is both in the ClockReplacer and
   * with its ref flag is set to false. If a frame is in the ClockReplacer, but its ref flag is set to true, change it
   * to false instead. This should be the only method that updates the clock hand.
   * 
   * @param[out] frame_id pointer the frame_id that is just victimized
   * @return true if it can find a frame id to victimized
   * @return false if it can't find a frame id to victimized
   */
  bool Victim(frame_id_t *frame_id) override;
  
  /**
   * @brief This method should be called after a page is pinned to a frame in the BufferPoolManager. It should remove the frame
   * containning the pinned page from the ClockReplacer.
   * 
   * @param frame_id the frame we need to pin
   */
  void Pin(frame_id_t frame_id) override;

  /**
   * @brief This method should be called when the pin_count of a page becomes 0. This method should add the frame containing
   * the unpinned page to the ClockReplacer.
   * 
   * @param frame_id 
   */
  void Unpin(frame_id_t frame_id) override;

  /**
   * @brief Return the number of frames that are currently in the ClockReplacer
   * 
   * @return size_t number of frames that are currently in the ClockReplacer
   */
  size_t Size() override;

 private:
  size_t max_size_;
  size_t size_;
  /** Position of the clock hand */
  size_t clock_hand_ = 0;
  /** Ref count of each frame. -1 indicates that frame is not in the replacer */
  int8_t *flags_;
};

}  // namespace bustub
