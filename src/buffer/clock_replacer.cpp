//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// clock_replacer.cpp
//
// Identification: src/buffer/clock_replacer.cpp
//
// Copyright (c) 2015-2019, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "buffer/clock_replacer.h"

namespace bustub {

ClockReplacer::ClockReplacer(size_t num_pages) : size_(0), hand_(0), clock_(std::vector<Bits>(num_pages)) {}

ClockReplacer::~ClockReplacer() = default;

bool ClockReplacer::Victim(frame_id_t *frame_id)
{
    for (size_t i = 0; i < 2*clock_.size(); i++)
    {
        if (clock_[hand_].in_replacer && !clock_[hand_].ref)
        {
            *frame_id = hand_;
            clock_[hand_].in_replacer =0;
            size_--;
            return true;
        }
        else if (clock_[hand_].in_replacer && clock_[hand_].ref)
        {
            clock_[hand_].ref = 0;
        }
        hand_++;
        hand_ = hand_%clock_.size();
    }
    frame_id = nullptr;
    return false;
}

  /**
   * Pins a frame, indicating that it should not be victimized until it is unpinned.
   * @param frame_id the id of the frame to pin
   */

void ClockReplacer::Pin(frame_id_t frame_id) 
{
    if (clock_[frame_id].in_replacer)
    {
        clock_[frame_id].in_replacer = 0;
        size_--;
    }
}

void ClockReplacer::Unpin(frame_id_t frame_id) 
{
    if (!clock_[frame_id].in_replacer)
    {
        clock_[frame_id].in_replacer = 1;
        clock_[frame_id].ref = 1;
        size_++;
    }
}

size_t ClockReplacer::Size() 
{ 
    return size_; 
}

}  // namespace bustub
