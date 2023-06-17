//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// extendible_hash_table.cpp
//
// Identification: src/container/hash/extendible_hash_table.cpp
//
// Copyright (c) 2022, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include <cassert>
#include <cstdlib>
#include <functional>
#include <list>
#include <utility>

#include "container/hash/extendible_hash_table.h"
#include "storage/page/page.h"

namespace bustub {

template <typename K, typename V>
ExtendibleHashTable<K, V>::ExtendibleHashTable(size_t bucket_size)
    : global_depth_(0), bucket_size_(bucket_size), num_buckets_(1) {
  dir_.push_back(std::shared_ptr<Bucket>(new Bucket(bucket_size)));
}

template <typename K, typename V>
auto ExtendibleHashTable<K, V>::IndexOf(const K &key) -> size_t {
  int mask = (1 << global_depth_) - 1;
  return std::hash<K>()(key) & mask;
}

template <typename K, typename V>
auto ExtendibleHashTable<K, V>::GetGlobalDepth() const -> int {
  std::scoped_lock<std::mutex> lock(latch_);
  return GetGlobalDepthInternal();
}

template <typename K, typename V>
auto ExtendibleHashTable<K, V>::GetGlobalDepthInternal() const -> int {
  return global_depth_;
}

template <typename K, typename V>
auto ExtendibleHashTable<K, V>::GetLocalDepth(int dir_index) const -> int {
  std::scoped_lock<std::mutex> lock(latch_);
  return GetLocalDepthInternal(dir_index);
}

template <typename K, typename V>
auto ExtendibleHashTable<K, V>::GetLocalDepthInternal(int dir_index) const -> int {
  return dir_[dir_index]->GetDepth();
}

template <typename K, typename V>
auto ExtendibleHashTable<K, V>::GetNumBuckets() const -> int {
  std::scoped_lock<std::mutex> lock(latch_);
  return GetNumBucketsInternal();
}

template <typename K, typename V>
auto ExtendibleHashTable<K, V>::GetNumBucketsInternal() const -> int {
  return num_buckets_;
}

template <typename K, typename V>
auto ExtendibleHashTable<K, V>::Find(const K &key, V &value) -> bool {
  // std::cout << "-----------find" << std::endl;
  std::scoped_lock<std::mutex> lock(latch_);
  size_t idx = IndexOf(key);
  auto ret = dir_[idx]->Find(key, value);
  return ret;
}

template <typename K, typename V>
auto ExtendibleHashTable<K, V>::Remove(const K &key) -> bool {
  // std::cout << "-----------remove" << std::endl;
  std::scoped_lock<std::mutex> lock(latch_);
  size_t idx = IndexOf(key);
  auto ret = dir_[idx]->Remove(key);
  return ret;
}

// template <typename K, typename V>
// void ExtendibleHashTable<K, V>::Mprint() {
//   // std::cout << "global_depth_:" << global_depth_ << std::endl;
//   for (const auto &i : dir_) {
//     std::cout << "localdepth:" << i->GetDepth() << std::endl;
//     auto list = i->GetItems();
//     for (const auto &p : list) {
//       std::cout << p.first << " " << p.second << std::endl;
//     }
//   }
// }

template <typename K, typename V>
void ExtendibleHashTable<K, V>::Insert(const K &key, const V &value) {
  std::scoped_lock<std::mutex> lock(latch_);
  InsertInternal(key, value);
}

template <typename K, typename V>
void ExtendibleHashTable<K, V>::InsertInternal(const K &key, const V &value) {
  // std::cout << "-----------insert" << key << std::endl;
  size_t idx = IndexOf(key);
  // std::cout << "---insert---" << key << " " << value << " " << IndexOf(key) << " " << dir_[idx]->GetItems().size()
  //           << std::endl;

  if (!dir_[idx]->IsFull()) {
    // std::cout << "notfull" << std::endl;
    // std::cout << "---bucket insert---" << key << " " << value << " " << idx << " " << dir_[idx]->GetItems().size()
    //           << std::endl;
    dir_[idx]->Insert(key, value);
  } else {
    do {
      auto old_bucket = dir_[idx];
      if (dir_[idx]->GetDepth() == global_depth_) {
        // std::cout << "--------------------case 1" << std::endl;
        size_t sz = dir_.size();
        dir_.resize(dir_.size() * 2);
        for (size_t i = sz; i < dir_.size(); i++) {
          dir_[i] = dir_[i - sz];
        }
        dir_[idx]->IncrementDepth();
        dir_[(1 << global_depth_) + idx] = std::shared_ptr<Bucket>(new Bucket(bucket_size_, dir_[idx]->GetDepth()));
        num_buckets_++;
        global_depth_++;
      } else {
        // std::cout << "--------------------case 2" << std::endl;
        dir_[idx]->IncrementDepth();
        auto new_bucket = std::shared_ptr<Bucket>(new Bucket(bucket_size_, dir_[idx]->GetDepth()));
        num_buckets_++;
        // std::cout << "size:" << dir_.size() << std::endl;
        // std::cout << "size:" << global_depth_ << " " << dir_[idx]->GetDepth() << " " << idx << std::endl;
        for (int i = 0; i <= (1 << (global_depth_ - dir_[idx]->GetDepth())) - 1; i++) {
          dir_[(i << dir_[idx]->GetDepth()) + (idx & ((1 << dir_[idx]->GetDepth()) - 1))] = new_bucket;
        }
      }
      // std::cout<<"redis------"<<idx<<std::endl;
      RedistributeBucket(old_bucket);
      idx = IndexOf(key);
    } while (dir_[idx]->IsFull());
    dir_[idx]->Insert(key, value);
    // std::cout << "---bucket insert---" << key << " " << value << " " << idx << " " << dir_[idx]->GetItems().size()
    //           << std::endl;
  }
}

template <typename K, typename V>
auto ExtendibleHashTable<K, V>::RedistributeBucket(std::shared_ptr<Bucket> bucket) -> void {
  auto &list = bucket->GetItems();
  size_t sz = list.size();
  while ((sz--) != 0) {
    K key = list.front().first;
    V value = list.front().second;
    list.pop_front();
    InsertInternal(key, value);
  }
}

//===--------------------------------------------------------------------===//
// Bucket
//===--------------------------------------------------------------------===//
template <typename K, typename V>
ExtendibleHashTable<K, V>::Bucket::Bucket(size_t array_size, int depth) : size_(array_size), depth_(depth) {
  list_.clear();
}

template <typename K, typename V>
auto ExtendibleHashTable<K, V>::Bucket::Find(const K &key, V &value) -> bool {
  for (const auto &p : list_) {
    if (p.first == key) {
      value = p.second;
      return true;
    }
  }
  return false;
}

template <typename K, typename V>
auto ExtendibleHashTable<K, V>::Bucket::Remove(const K &key) -> bool {
  for (auto it = list_.begin(); it != list_.end(); it++) {
    if (it->first == key) {
      list_.erase(it);
      return true;
    }
  }
  return false;
}

template <typename K, typename V>
auto ExtendibleHashTable<K, V>::Bucket::Insert(const K &key, const V &value) -> bool {
  if (IsFull()) {
    return false;
  }
  for (auto &p : list_) {
    if (p.first == key) {
      p.second = value;
      return true;
    }
  }
  list_.push_back(std::make_pair(key, value));
  return true;
}

template class ExtendibleHashTable<page_id_t, Page *>;
template class ExtendibleHashTable<Page *, std::list<Page *>::iterator>;
template class ExtendibleHashTable<int, int>;
// test purpose
template class ExtendibleHashTable<int, std::string>;
template class ExtendibleHashTable<int, std::list<int>::iterator>;

}  // namespace bustub
