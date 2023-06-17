//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// buffer_pool_manager_instance.cpp
//
// Identification: src/buffer/buffer_pool_manager.cpp
//
// Copyright (c) 2015-2021, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "buffer/buffer_pool_manager_instance.h"

#include "common/exception.h"
#include "common/macros.h"

namespace bustub {

BufferPoolManagerInstance::BufferPoolManagerInstance(size_t pool_size, DiskManager *disk_manager, size_t replacer_k,
                                                     LogManager *log_manager)
    : pool_size_(pool_size), disk_manager_(disk_manager), log_manager_(log_manager) {
  // we allocate a consecutive memory space for the buffer pool
  pages_ = new Page[pool_size_];
  page_table_ = new ExtendibleHashTable<page_id_t, frame_id_t>(bucket_size_);
  replacer_ = new LRUKReplacer(pool_size, replacer_k);

  // Initially, every page is in the free list.
  for (size_t i = 0; i < pool_size_; ++i) {
    free_list_.emplace_back(static_cast<int>(i));
  }
}

BufferPoolManagerInstance::~BufferPoolManagerInstance() {
  delete[] pages_;
  delete page_table_;
  delete replacer_;
}

auto BufferPoolManagerInstance::NewPgImp(page_id_t *page_id) -> Page * {
  std::lock_guard<std::mutex> lock_guard(latch_);
  frame_id_t frame_id;
  Page *page = nullptr;
  if (!free_list_.empty()) {
    frame_id = free_list_.front();
    free_list_.pop_front();
    page = &pages_[frame_id];
  } else if (replacer_->Evict(&frame_id)) {
    page = &pages_[frame_id];
    if (page->is_dirty_) {
      disk_manager_->WritePage(page->GetPageId(), page->GetData());
    }
    page_table_->Remove(page->GetPageId());
  } else {
    return nullptr;
  }
  page->page_id_ = AllocatePage();
  page->is_dirty_ = false;
  page->pin_count_ = 1;
  page->ResetMemory();
  page_table_->Insert(page->GetPageId(), frame_id);
  *page_id = page->GetPageId();
  replacer_->RecordAccess(frame_id);
  replacer_->SetEvictable(frame_id, false);
  return page;
}

auto BufferPoolManagerInstance::FetchPgImp(page_id_t page_id) -> Page * {
  std::lock_guard<std::mutex> lock_guard(latch_);
  frame_id_t frame_id;
  if (page_table_->Find(page_id, frame_id)) {
    pages_[frame_id].pin_count_++;
    replacer_->RecordAccess(frame_id);
    replacer_->SetEvictable(frame_id, false);
    return &pages_[frame_id];
  }
  Page *page;
  if (!free_list_.empty()) {
    frame_id = free_list_.front();
    free_list_.pop_front();
    page = &pages_[frame_id];
  } else if (replacer_->Evict(&frame_id)) {
    page = &pages_[frame_id];
    if (page->IsDirty()) {
      disk_manager_->WritePage(page->GetPageId(), page->GetData());
    }
    page_table_->Remove(page->GetPageId());
  } else {
    return nullptr;
  }
  page->page_id_ = page_id;
  page->pin_count_ = 1;
  page->is_dirty_ = false;
  page->ResetMemory();
  page_table_->Insert(page->GetPageId(), frame_id);
  disk_manager_->ReadPage(page->page_id_, page->GetData());
  replacer_->RecordAccess(frame_id);
  replacer_->SetEvictable(frame_id, false);
  return page;
}

auto BufferPoolManagerInstance::UnpinPgImp(page_id_t page_id, bool is_dirty) -> bool {
  std::lock_guard<std::mutex> lock_guard(latch_);
  frame_id_t frame_id;
  if (!page_table_->Find(page_id, frame_id)) {
    return false;
  }
  if (pages_[frame_id].pin_count_ == 0) {
    return false;
  }
  if (is_dirty) {
    pages_[frame_id].is_dirty_ = true;
  }
  pages_[frame_id].pin_count_--;
  if (pages_[frame_id].pin_count_ == 0) {
    replacer_->SetEvictable(frame_id, true);
  }
  return true;
}

auto BufferPoolManagerInstance::FlushPgImp(page_id_t page_id) -> bool {
  std::lock_guard<std::mutex> lock_guard(latch_);
  frame_id_t frame_id;
  if (!page_table_->Find(page_id, frame_id)) {
    return false;
  }
  disk_manager_->WritePage(pages_[frame_id].GetPageId(), pages_[frame_id].GetData());
  pages_[frame_id].is_dirty_ = false;
  return true;
}

void BufferPoolManagerInstance::FlushAllPgsImp() {
  for (size_t i = 0; i < pool_size_; i++) {
    FlushPgImp(pages_[i].GetPageId());
  }
}

auto BufferPoolManagerInstance::DeletePgImp(page_id_t page_id) -> bool {
  std::lock_guard<std::mutex> lock_guard(latch_);
  frame_id_t frame_id;
  if (!page_table_->Find(page_id, frame_id)) {
    return true;
  }
  if (pages_[frame_id].pin_count_ > 0) {
    return false;
  }
  page_table_->Remove(page_id);
  replacer_->SetEvictable(frame_id, true);
  free_list_.emplace_back(frame_id);
  pages_[frame_id].ResetMemory();
  return true;
}

auto BufferPoolManagerInstance::AllocatePage() -> page_id_t { return next_page_id_++; }

}  // namespace bustub
