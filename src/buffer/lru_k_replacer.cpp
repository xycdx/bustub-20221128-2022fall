//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// lru_k_replacer.cpp
//
// Identification: src/buffer/lru_k_replacer.cpp
//
// Copyright (c) 2015-2022, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "buffer/lru_k_replacer.h"
#include <iostream>
namespace bustub {

LRUKReplacer::LRUKReplacer(size_t num_frames, size_t k) : replacer_size_(num_frames), k_(k) {}

auto LRUKReplacer::Evict(frame_id_t *frame_id) -> bool {
  std::lock_guard<std::mutex> locker(latch_);
  bool flag = false;
  size_t max_dst = 0;
  for (auto &it : frame_ref_) {
    if (is_evictable_[it.first]) {
      if (it.second.size() < k_) {
        flag = true;
        if (current_timestamp_ - it.second.front() >= max_dst) {
          max_dst = current_timestamp_ - it.second.front();
          *frame_id = it.first;
        }
      }
    }
  }
  if (flag) {
    curr_size_--;
    frame_ref_.erase(*frame_id);
    is_evictable_.erase(*frame_id);
  } else {
    for (auto &it : frame_ref_) {
      if (is_evictable_[it.first]) {
        flag = true;
        if (current_timestamp_ - it.second.front() >= max_dst) {
          max_dst = current_timestamp_ - it.second.front();
          *frame_id = it.first;
        }
      }
    }
    if (flag) {
      curr_size_--;
      frame_ref_.erase(*frame_id);
      is_evictable_.erase(*frame_id);
    }
  }
  return flag;
}

void LRUKReplacer::RecordAccess(frame_id_t frame_id) {
  std::lock_guard<std::mutex> locker(latch_);
  if (frame_ref_.count(frame_id) == 0U) {
    BUSTUB_ASSERT(curr_size_ != replacer_size_, "out of replacer's size");
  }
  frame_ref_[frame_id].push_back(++current_timestamp_);
  if (frame_ref_[frame_id].size() > k_) {
    frame_ref_[frame_id].pop_front();
  }
}

void LRUKReplacer::SetEvictable(frame_id_t frame_id, bool set_evictable) {
  std::lock_guard<std::mutex> locker(latch_);
  if (frame_ref_.count(frame_id) == 0U) {
    return;
  }
  if (is_evictable_[frame_id] && !set_evictable) {
    curr_size_--;
  } else if (!is_evictable_[frame_id] && set_evictable) {
    curr_size_++;
  }
  is_evictable_[frame_id] = set_evictable;
}

void LRUKReplacer::Remove(frame_id_t frame_id) {
  std::lock_guard<std::mutex> locker(latch_);
  if (frame_ref_.count(frame_id) == 0U) {
    return;
  }
  BUSTUB_ASSERT(is_evictable_[frame_id], "this frame canot be removed");

  curr_size_--;
  frame_ref_.erase(frame_id);
  is_evictable_.erase(frame_id);
}

auto LRUKReplacer::Size() -> size_t { return curr_size_; }

}  // namespace bustub
