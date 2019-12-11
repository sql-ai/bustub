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
//
// This component is responsible for tracking page usage in the buffer pool. 
// It is implemented as a new sub-class called ClockReplacer
//      1. src/include/buffer/clock_replacer.h 
//      2. src/buffer/clock_replacer.cpp. 

// ClockReplacer extends the abstract Replacer class (src/include/buffer/replacer.h), 
//  which contains the function specifications.

// The ClockReplacer is initialized to contain placeholders for all of the frames in the BufferPoolManager. 
//  The clock hand initially points to the placeholder of frame 0. 
//  For each frame, ClockReplacer tracks two things: 
//      1. Is this frame currently in the ClockReplacer? 
//      2. Has this frame recently been unpinned (ref flag)?

// In some scenarios, the two are the same. For example, when page is unpined, both of the above are true. 
// However, a frame stays in the ClockReplacer until it is pinned or victimized, 
// but its ref flag is modified by the clock hand.
//===----------------------------------------------------------------------===//

#include "buffer/clock_replacer.h"

namespace bustub {

using std::vector;
ClockReplacer::ClockReplacer(size_t num_pages) : 
    sz_(0), bits_(vector<ClockBits>(num_pages)), hand_(0)
{
    // TODO: do we need this?
    for (size_t i = 0; i < num_pages; i++)
    {
        this->bits_[i].ref = 0;
        this->bits_[i].in_replacer = 0;
    }
}

ClockReplacer::~ClockReplacer() = default;

// Starting from the current position of clock hand, find the first frame 
// that is both in the ClockReplacer and with its ref flag set to false.
// If a frame is in ClockReplacer, but its ref flag is set to true, change it to false instead.
// This is the only method that updates the clock hand.
bool ClockReplacer::Victim(frame_id_t *frame_id) 
{ 
    for (size_t i = 0; i <= 2*bits_.size(); i++)
    {
        inc_hand();
        // Find the frame that is in the Replacer (bits[hand].ref == 0)
        // and with its ref flag set to false.
        if (bits_[hand_].in_replacer == 1 && bits_[hand_].ref == 0)
        {
            *frame_id = hand_;
            bits_[hand_].ref = 1;
            bits_[hand_].in_replacer = 0;
            sz_--;
            return true;
        } 
        else if (bits_[hand_].ref == 1)
        {
            bits_[hand_].ref = 0;
        }
    }    
    return false;
}

// This method should be called after a page is pinned to a frame in the BufferPoolManager. 
// It removes the frame containing the pinned page from the ClockReplacer.
void ClockReplacer::Pin(frame_id_t frame_id) 
{
    if (this->bits_[frame_id].in_replacer == 0) 
    {
        LOG_ERROR("Trying to pin on frame %d that is not in the replacer", frame_id);
        return;
    }
    this->bits_[frame_id].in_replacer = 0;
    --sz_;
}

// This method should be called when the pin_count of a page becomes 0. 
// It adds the frame containing the unpinned page to the ClockReplacer. 
void ClockReplacer::Unpin(frame_id_t frame_id) 
{
    if (this->bits_[frame_id].in_replacer == 1) 
    {
        LOG_ERROR("Trying to unpin on frame %d that is already in the replacer", frame_id);
        return;
    }
    this->bits_[frame_id].in_replacer = 1;
    ++sz_;
}

size_t ClockReplacer::Size() 
{ 
    return sz_; 
}

}  // namespace bustub
