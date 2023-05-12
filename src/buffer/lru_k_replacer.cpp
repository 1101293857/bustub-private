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
#include "common/exception.h"
namespace bustub {

LRUKReplacer::LRUKReplacer(size_t num_frames, size_t k) : replacer_size_(num_frames), k_(k) {}

auto LRUKReplacer::Evict(frame_id_t *frame_id) -> bool {
  std::lock_guard<std::mutex> lock(latch_);
  if (now_knode_.empty()) {
    return false;
  }
  // for(std::vector<int>::size_type i=0;i<now_knode_.size();i++){
  //     std::cout<<now_knode_[i];
  // }
  // std::cout<<std::endl;
  std::vector<frame_id_t> less_k;
  std::vector<size_t> less_k_time;
  std::vector<frame_id_t> more_k;
  std::vector<size_t> more_k_time;
  for (auto n : now_knode_) {
    if (node_store_[n].history_.size() < k_) {
      less_k.push_back(n);
      less_k_time.push_back(node_store_[n].history_[0]);
    } else {
      more_k.push_back(n);
      more_k_time.push_back(node_store_[n].history_[node_store_[n].history_.size() - k_]);
    }
  }

  // frame_id_t move_id_;
  size_t min = 200000000;
  if (!less_k.empty()) {
    for (std::vector<int>::size_type i = 0; i < less_k.size(); i++) {
      if (less_k_time[i] < min) {
        min = less_k_time[i];
        move_id_ = less_k[i];
      }
    }
  } else {
    for (std::vector<int>::size_type i = 0; i < more_k.size(); i++) {
      if (more_k_time[i] < min) {
        min = more_k_time[i];
        move_id_ = more_k[i];
      }
    }
  }
  *frame_id = move_id_;
  for (auto ii = now_knode_.begin(); ii != now_knode_.end(); ii++) {
    if ((*ii) == move_id_) {
      now_knode_.erase(ii);
      break;
    }
  }
  // remove(now_knode_.begin(),now_knode_.end(),move_id_);
  for (auto ii = now_knode_map_.begin(); ii != now_knode_map_.end(); ii++) {
    if (ii->first == move_id_) {
      now_knode_map_.erase(ii);
      break;
    }
  }
  for (auto ii = node_store_.begin(); ii != node_store_.end(); ii++) {
    if (ii->first == move_id_) {
      node_store_.erase(ii);
      break;
    }
  }
  frame_id_t *nihao = frame_id;
  move_id_ = *nihao;

  // remove(now_knode_map_.begin(),now_knode_map_.end(),move_id_);
  return true;
}

void LRUKReplacer::RecordAccess(frame_id_t frame_id, [[maybe_unused]] AccessType access_type) {
  std::lock_guard<std::mutex> lock(latch_);
  if (node_store_.count(frame_id) == 0 && now_knode_.empty() && node_store_.size() == replacer_size_) {
    return;
  }
  if (node_store_.count(frame_id) == 0) {  // 不在map中  创建
    frame_id_t ff;
    if (node_store_.size() == replacer_size_) {
      Evict(&ff);
    }
    LRUKNode node;
    node.fid_ = frame_id;
    node.k_ = k_;
    node_store_[frame_id] = node;
  }
  LRUKNode &node = node_store_[frame_id];
  current_timestamp_++;
  node.history_.push_back(current_timestamp_);
}

void LRUKReplacer::SetEvictable(frame_id_t frame_id, bool set_evictable) {
  std::lock_guard<std::mutex> lock(latch_);
  if (node_store_.count(frame_id) == 0) {
    return;
  }
  // for(std::vector<int>::size_type i=0;i<now_knode_.size();i++){
  //     std::cout<<now_knode_[i];
  // }
  // std::cout<<std::endl;
  LRUKNode &node = node_store_[frame_id];
  if (!set_evictable) {
    if (!node.is_evictable_) {
      return;
    }  // 换出来一个需要换回去
    node.is_evictable_ = false;
    for (auto ii = now_knode_.begin(); ii != now_knode_.end(); ii++) {
      if ((*ii) == frame_id) {
        now_knode_.erase(ii);
        break;
      }
    }
    // remove(now_knode_.begin(),now_knode_.end(),move_id_);
    for (auto ii = now_knode_map_.begin(); ii != now_knode_map_.end(); ii++) {
      if (ii->first == frame_id) {
        now_knode_map_.erase(ii);
        break;
      }
    }

  } else {
    if (node.is_evictable_) {
      return;
    }
    node.is_evictable_ = true;
    now_knode_.push_back(frame_id);
    now_knode_map_[frame_id] = 1;
  }
  // for(std::vector<int>::size_type i=0;i<now_knode_.size();i++){
  //     std::cout<<now_knode_[i];
  // }
  // std::cout<<std::endl;
}

void LRUKReplacer::Remove(frame_id_t frame_id) {
  std::lock_guard<std::mutex> lock(latch_);
  if (node_store_.count(frame_id) == 0) {
    return;
  }
  for (auto ii = node_store_.begin(); ii != node_store_.end(); ii++) {
    if (ii->first == frame_id) {
      node_store_.erase(ii);
      break;
    }
  }
  if (now_knode_map_.count(frame_id) == 1) {
    for (auto ii = now_knode_map_.begin(); ii != now_knode_map_.end(); ii++) {
      if (ii->first == frame_id) {
        now_knode_map_.erase(ii);
        break;
      }
    }
    for (auto ii = now_knode_.begin(); ii != now_knode_.end(); ii++) {
      if ((*ii) == frame_id) {
        now_knode_.erase(ii);
        break;
      }
    }
  }
}

auto LRUKReplacer::Size() -> size_t {
  std::lock_guard<std::mutex> lock(latch_);
  return now_knode_.size();
}

}  // namespace bustub
