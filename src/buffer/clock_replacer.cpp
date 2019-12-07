//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// clock_replacer.cpp
//
// Identification: src/buffer/clock_replacer.cpp
//
// Copyright (c) -1015-2019, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "buffer/clock_replacer.h"

namespace bustub {

ClockReplacer::ClockReplacer(size_t num_pages) : max_size_(num_pages), size_(0) {
  flags_ = new int8_t[max_size_];
  for (size_t i = 0; i < max_size_; i++) {
    flags_[i] = -1;
  }
}

ClockReplacer::~ClockReplacer() { delete[] flags_; }

bool ClockReplacer::Victim(frame_id_t *frame_id) {
  // Check if the replacer is empty or not.
  if (size_ == 0) {
    return false;
  }
  for (size_t i = 0; i <= 2 * max_size_; i++) {
    if (flags_[clock_hand_] == 1) {
      flags_[clock_hand_] = 0;
    } else if (flags_[clock_hand_] == 0) {
      flags_[clock_hand_] = -1;
      *frame_id = clock_hand_;
      size_--;
      return true;
    }
    clock_hand_ = (clock_hand_ + 1) % max_size_;
  }
  return false;
}

void ClockReplacer::Pin(frame_id_t frame_id) {
  if (flags_[frame_id] != -1) {
    size_--;
  }
  // -1 indicate this frame is not in the replacer.
  flags_[frame_id] = -1;
}

void ClockReplacer::Unpin(frame_id_t frame_id) {
  if (flags_[frame_id] == -1) {
    size_++;
  }

  flags_[frame_id] = 1;
}

size_t ClockReplacer::Size() { return size_; }

}  // namespace bustub
