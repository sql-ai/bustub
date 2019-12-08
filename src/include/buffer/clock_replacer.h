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

typedef struct 
{   
    // b1 is the ref-flag
    unsigned char b1 : 1;
    // b2 indicates if the frame is in the replacer
    unsigned char b2 : 1;
} ClockBits;

/**
 * ClockReplacer is responsible for tracking page usage in the buffer pool.
 * It implements the clock replacement policy, which approximates the Least Recently Used policy.
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

  // Starting from the current position of clock hand, 
  //  find the first frame that is both in the `ClockReplacer` and 
  //  with its ref flag set to false. 
  //
  //  If a frame is in the `ClockReplacer`, but its ref flag is set to true, 
  //  change it to false instead. 
  //
  //  This should be the only method that updates the clock hand.
  bool Victim(frame_id_t *frame_id) override;

  // This method should be called after a page is pinned to a frame in the BufferPoolManager. 
  // It should remove the frame containing the pinned page from the ClockReplacer. 
  void Pin(frame_id_t frame_id) override;

  // This method should be called when the pin_count of a page becomes 0. 
  // This method should add the frame containing the unpinned page to the ClockReplacer.
  void Unpin(frame_id_t frame_id) override;

  size_t Size() override;

 private:
    size_t sz;
    std::vector<ClockBits> bits;
    uint32_t hand;
    int inc_hand() 
    {
        hand++;
        if (hand == bits.size()) 
        {
            hand = 0;
        }
        return hand;
    }
};

}  // namespace bustub
