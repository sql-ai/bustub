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
// Responsible for tracking page usage in the buffer pool.
// ClockReplacer extends the abstract Replacer class, which contains the function specifications
//
// Size of the ClockReplacer is = buffer pool since it contains placeholders for all of the frames in the BufferPoolManager.
// Not all the frames are considered as in the ClockReplacer. 
//
// ClockReplacer is initialized to have no frame.
// Only the newly unpinned ones will be considered in the ClockReplacer. 
//
// Adding or removing a frame is implemented by changing a reference bit of a frame.
// The clock hand initially points to the placeholder of frame 0. For each frame, ClockReplacer tracks two things:
//   1. Is this frame currently in the ClockReplacer?
//   2. Has this frame recently been unpinned (ref flag)?
// In some scenarios, the two are the same. For example, page is unpinned, both of the above are true.
// However, the frame stays in the ClockReplacer until it is pinned or victimized, but its ref flag is modified by the clock hand.
//
// Implementation will not run out of memory + operations are thread-safe.
//
//===----------------------------------------------------------------------===//


#pragma once

#include <list>
#include <mutex>  // NOLINT
#include <vector>

#include "buffer/replacer.h"
#include "common/config.h"

namespace bustub {

struct Bits;

/**
 * ClockReplacer implements the clock replacement policy, which approximates the Least Recently Used policy.
 */
class ClockReplacer : public Replacer {
 public:
  /**
   * Create a new ClockReplacer.
   * @param num_pages the maximum number of pages the ClockReplacer will be required to store
   */
  explicit ClockReplacer(size_t num_pages);

  /**
   * Destroys the ClockReplacer.
   */
  ~ClockReplacer() override;

  /**
   * Removes the first frame that is in the `ClockReplacer` and its ref flag = false, starting from current position of clock.
   * If a frame is in the `ClockReplacer`, but ref flag = true, change it to false instead.
   * This should be the only method that updates the clock hand.
   * @param[out] frame_id id of frame that was removed, nullptr if no victim was found
   * @return true if a victim frame was found, false otherwise
   */
  bool Victim(frame_id_t *frame_id) override;


  /**
   * Pins a frame, indicating that it should not be victimized until it is unpinned.
   * Removes the frame containing the pinned page from the ClockReplacer.
   * Called after a page is pinned to a frame in the BufferPoolManager.
   * @param frame_id the id of the frame to pin
   */
  void Pin(frame_id_t frame_id) override;

  /**
   * Unpins a frame, indicating that it can now be victimized.
   * Adds the frame containing the unpinned page to the ClockReplacer.
   * Called when the pin_count of a page = 0.
   * @param frame_id the id of the frame to unpin
   */
  void Unpin(frame_id_t frame_id) override;

  /** @return the number of frames that are currently in the ClockReplacer. */
  size_t Size() override;

 private:
  struct Bits{
    uint8_t in_replacer : 1;
    uint8_t ref_flag    : 1;
    uint8_t              : 6;
  };

  size_t hand_;
  size_t size_;

  std::vector<Bits> clock_; 
};

}  // namespace bustub
