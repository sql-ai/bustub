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
#include <cstring>

namespace bustub {

ClockReplacer::ClockReplacer(size_t num_pages) : hand_(0), num_pages_(num_pages), sz_(0) {
    in_replacer_ = new bool[num_pages];
    memset(in_replacer_, 0, num_pages * sizeof(bool));
    ref_ = new bool[num_pages];
    memset(ref_, 0, num_pages * sizeof(bool));
}

ClockReplacer::~ClockReplacer() {
    delete[] in_replacer_;
    delete[] ref_;
};

bool ClockReplacer::Victim(frame_id_t *frame_id) {
    if (0 == sz_) {
        return false;
    }

    /*
    Starting from the current position of clock hand, find the first frame that is both in the `ClockReplacer` and with its ref flag set to false.
    If a frame is in the `ClockReplacer`, but its ref flag is set to true, change it to false instead.
    This should be the only method that updates the clock hand.
    */
    const auto iteration_logic = [&]() {
        if (in_replacer_[hand_]) {
            if (!ref_[hand_]) {
                // Pins the frame 'hand_'
                in_replacer_[hand_] = false;
                --sz_;

                *frame_id = hand_;
                return true;
            }
            ref_[hand_] = false;
        }
        hand_ = (hand_ + 1) % num_pages_;
        return false;
    };
    const auto max_num_iterations = (num_pages_ - sz_ + 1) + num_pages_;
    for (size_t i = 0; i < max_num_iterations; i++) {
        if (iteration_logic()) {
            return true;
        }
    }
    // Checks and handles 'size_t' overflow
    if (max_num_iterations < num_pages_) {
        for (size_t i = 0; i < SIZE_MAX; i++) {
            if (iteration_logic()) {
                return true;
            }
        }
    }

    return false;
}

void ClockReplacer::Pin(frame_id_t frame_id) {
    if (!in_replacer_[frame_id]) {
        LOG_ERROR("Attempt to pin frame %d that is currently not in the replacer", frame_id);
        return;
    }

    in_replacer_[frame_id] = false;
    --sz_;
}

void ClockReplacer::Unpin(frame_id_t frame_id) {
    if (in_replacer_[frame_id]) {
        LOG_ERROR("Attempt to unpin frame %d that is already in the replacer", frame_id);
        return;
    }

    in_replacer_[frame_id] = true;
    ref_[frame_id] = true;
    ++sz_;
}

size_t ClockReplacer::Size() {
    return sz_;
}

}  // namespace bustub
