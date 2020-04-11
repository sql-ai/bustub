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
#include "common/logger.h"

namespace bustub {

ClockReplacer::ClockReplacer(size_t num_pages): hand_(0), size_(0),  clock_(std::vector<Bits>(num_pages)) {}

ClockReplacer::~ClockReplacer() = default;

/**
 * Starting from current position of clock, find the first frame that is in the `ClockReplacer` 
 * and its ref flag = false. If a frame is in the `ClockReplacer`, but ref flag = true, change it to false instead.
 * This should be the only method that updates the clock hand.
 */
bool ClockReplacer::Victim(frame_id_t *frame_id) 
{
    for(size_t i = 0; i < clock_.size()*2; i++)
    {
        if(clock_[hand_].in_replacer && !clock_[hand_].ref_flag) 
        {
            *frame_id = hand_;
            clock_[hand_].in_replacer = 0;
            size_--;
            return true;
        }
        else if (clock_[hand_].in_replacer && clock_[hand_].ref_flag)
        {
            clock_[hand_].ref_flag = 0;
        }
        hand_ = (hand_+1) % clock_.size();
    }
    frame_id = nullptr;
    return false;
}

/**
 * Called after a page is pinned to a frame in the BufferPoolManager.
 * It should remove the frame containing the pinned page from the ClockReplacer.
 */
void ClockReplacer::Pin(frame_id_t frame_id) 
{
    if(clock_[frame_id].in_replacer)
    {
        clock_[frame_id].in_replacer = 0;
        size_--;
    }
}

/**
 * Called when the pin_count of a page = 0. Add the frame containing the unpinned page to the ClockReplacer.
 */
void ClockReplacer::Unpin(frame_id_t frame_id) 
{
    if(!clock_[frame_id].in_replacer)
    {
        size_++;
        clock_[frame_id].in_replacer = 1;
        clock_[frame_id].ref_flag = 1;
    }
}

/**
 * Returns the number of frames that are currently in the ClockReplacer.
 */
size_t ClockReplacer::Size() 
{
    return size_; 
}

}  // namespace bustub
