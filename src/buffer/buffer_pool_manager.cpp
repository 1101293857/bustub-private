//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// buffer_pool_manager.cpp
//
// Identification: src/buffer/buffer_pool_manager.cpp
//
// Copyright (c) 2015-2021, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "buffer/buffer_pool_manager.h"

#include <iostream>
#include "common/exception.h"
#include "common/macros.h"
#include "storage/page/page_guard.h"
namespace bustub {

BufferPoolManager::BufferPoolManager(size_t pool_size, DiskManager *disk_manager, size_t replacer_k,
                                     LogManager *log_manager)
    : pool_size_(pool_size), disk_manager_(disk_manager), log_manager_(log_manager) {
  // TODO(students): remove this line after you have implemented the buffer pool manager
  // throw NotImplementedException(
  //     "BufferPoolManager is not implemented yet. If you have finished implementing BPM, please remove the throw "
  //     "exception line in `buffer_pool_manager.cpp`.");
  // std::cout << pool_size << "    " << replacer_k << std::endl;
  // we allocate a consecutive memory space for the buffer pool
  pages_ = new Page[pool_size_];
  replacer_ = std::make_unique<LRUKReplacer>(pool_size, replacer_k);

  // Initially, every page is in the free list.
  for (size_t i = 0; i < pool_size_; ++i) {
    free_list_.emplace_back(static_cast<int>(i));
  }
}
void BufferPoolManager::PrintPage() {
  for (auto ii : page_table_) {
    std::cout << ii.first << "   " << ii.second << "   ";
  }
  std::cout << std::endl;
}
BufferPoolManager::~BufferPoolManager() { delete[] pages_; }

auto BufferPoolManager::NewPage(page_id_t *page_id) -> Page * {
  std::lock_guard<std::mutex> lock(latch_);
  // std::cout << "NewPage";
  if (free_list_.empty() && replacer_->now_knode_.empty()) {
    // throw std::logic_error("50  error");
    return nullptr;
  }

  frame_id_t new_frame_id;
  if (!free_list_.empty()) {
    new_frame_id = free_list_.front();
    free_list_.pop_front();

  } else if (replacer_->Evict(&new_frame_id)) {
    Page *vic = &pages_[new_frame_id];

    if (vic->IsDirty()) {
      disk_manager_->WritePage(vic->GetPageId(), vic->GetData());
    }
    page_table_.erase(vic->GetPageId());
  } else {
    // throw std::logic_error("67  error");
    return nullptr;
  }
  *page_id = AllocatePage();
  // std::cout << *page_id << std::endl;
  Page *new_page = &pages_[new_frame_id];

  new_page->ResetMemory();  // 提供好的成员函数，直接调用就行

  new_page->is_dirty_ = false;

  new_page->page_id_ = *page_id;

  new_page->pin_count_++;

  replacer_->RecordAccess(new_frame_id);

  page_table_.emplace(new_page->GetPageId(), new_frame_id);
  // PrintPage();
  // std::cout << std::this_thread::get_id() << "NewPage   " << replacer_->node_store_.size() << "  "
  //           << replacer_->now_knode_.size() << std::endl;
  // replacer_->now_knode_.size() << std::endl;
  return new_page;
}

auto BufferPoolManager::FetchPage(page_id_t page_id, [[maybe_unused]] AccessType access_type) -> Page * {
  std::lock_guard<std::mutex> lock(latch_);
  // Page *new_page = nullptr;
  // std::cout << std::this_thread::get_id() << "FetchPage" << page_id << std::endl;
  if (page_table_.count(page_id) != 0) {
    frame_id_t frame_id = page_table_[page_id];
    replacer_->RecordAccess(frame_id);
    Page *p = &pages_[frame_id];
    if (p->pin_count_ == 0) {  // 注意因为由unpin减为0时 需要将其Evictable 设为false
      replacer_->SetEvictable(frame_id, false);
    }
    p->pin_count_++;
    // PrintPage();
    // std::cout << p->pin_count_ << std::endl;
    return p;
  }
  if (free_list_.empty() && replacer_->now_knode_.empty()) {
    // PrintPage();
    // throw std::logic_error("108  error");
    return nullptr;
  }
  // for (auto i : replacer_->now_knode_) {
  //   std::cout << i << "   ";
  // }
  // std::cout << std::endl;
  frame_id_t new_frame_id;
  if (!free_list_.empty()) {
    new_frame_id = free_list_.front();
    free_list_.pop_front();
  } else {
    replacer_->Evict(&new_frame_id);
    Page *vic = &pages_[new_frame_id];

    if (vic->IsDirty()) {
      disk_manager_->WritePage(vic->GetPageId(), vic->GetData());
    }
    page_table_.erase(vic->GetPageId());
  }
  // frame_id_t new_frame_id;  //接受返回的那个页框号

  // if (replacer_->Evict(&new_frame_id)) {

  //   new_page = &pages_[new_frame_id];

  // } else if (!free_list_.empty()) {

  //   new_frame_id = free_list_.front();

  //   free_list_.pop_front();

  //   new_page = &pages_[new_frame_id];

  // } else {

  //   return nullptr;

  // }
  // if (new_page->IsDirty()) {

  //   disk_manager_->WritePage(new_page->GetPageId(), new_page->GetData());

  // }

  // page_table_.erase(new_page->GetPageId());
  Page *new_page = &pages_[new_frame_id];

  new_page->ResetMemory();  // 提供好的成员函数，直接调用就行

  new_page->is_dirty_ = false;

  new_page->page_id_ = page_id;

  new_page->pin_count_++;

  replacer_->RecordAccess(new_frame_id);

  page_table_.emplace(new_page->GetPageId(), new_frame_id);
  disk_manager_->ReadPage(page_id, new_page->data_);
  // PrintPage();
  // std::cout << std::this_thread::get_id() << "Fetct Page   " << replacer_->node_store_.size() << "  "
  //           << replacer_->now_knode_.size() << std::endl;
  // replacer_->now_knode_.size() << std::endl;
  return new_page;
}

auto BufferPoolManager::UnpinPage(page_id_t page_id, bool is_dirty, [[maybe_unused]] AccessType access_type) -> bool {
  std::lock_guard<std::mutex> lock(latch_);
  // std::cout << "UnpinPage" << page_id << "   " << is_dirty << std::endl;
  if (page_table_.count(page_id) == 0) {
    // PrintPage();
    return false;
  }
  Page &page = pages_[page_table_[page_id]];
  if (page.pin_count_ == 0) {
    // PrintPage();
    return false;
  }
  // replacer_->RecordAccess(page_table_[page_id]);
  page.is_dirty_ = page.IsDirty() || is_dirty;
  page.pin_count_--;
  if (page.pin_count_ == 0) {
    replacer_->SetEvictable(page_table_[page_id], true);
  }
  // PrintPage();
  // std::cout << std::this_thread::get_id() << "UnpinPage   " << replacer_->node_store_.size() << "  "
  //       << replacer_->now_knode_.size() << std::endl;
  // std::cout << std::this_thread::get_id() << "Unpin finish" << std::endl;
  return true;
}

auto BufferPoolManager::FlushPage(page_id_t page_id) -> bool {
  std::lock_guard<std::mutex> lock(latch_);
  if (page_table_.count(page_id) == 0 || page_id == INVALID_PAGE_ID) {
    return false;
  }
  // std::cout << "FlushPage"
  //           << "  " << page_id << std::endl;
  // replacer_->RecordAccess(page_table_[page_id]);
  Page &page = pages_[page_table_[page_id]];
  if (page.IsDirty()) {
    disk_manager_->WritePage(page.GetPageId(), page.GetData());
  }
  page.is_dirty_ = false;
  return true;
}

void BufferPoolManager::FlushAllPages() {
  std::lock_guard<std::mutex> lock(latch_);
  // std::cout << "FlushAllPages" << std::endl;
  for (auto nn : page_table_) {
    frame_id_t frame_id = nn.second;
    // replacer_->RecordAccess(frame_id);
    Page &page = pages_[frame_id];
    if (page.IsDirty()) {
      disk_manager_->WritePage(page.GetPageId(), page.GetData());
    }
    page.is_dirty_ = false;
  }
}

auto BufferPoolManager::DeletePage(page_id_t page_id) -> bool {
  std::lock_guard<std::mutex> lock(latch_);
  // std::cout << "DeletePage" << page_id << std::endl;
  if (page_table_.count(page_id) == 0) {
    return true;
  }

  Page *page = &pages_[page_table_[page_id]];

  if (page->pin_count_ != 0) {
    return false;
  }

  if (page->IsDirty()) {
    disk_manager_->WritePage(page->GetPageId(), page->GetData());
  }

  page->ResetMemory();

  page->is_dirty_ = false;

  page->pin_count_ = 0;
  page->page_id_ = INVALID_PAGE_ID;
  free_list_.push_back(page_table_[page_id]);

  replacer_->Remove(page_table_[page_id]);

  page_table_.erase(page_id);
  DeallocatePage(page_id);

  return true;
}

auto BufferPoolManager::AllocatePage() -> page_id_t {
  // std::lock_guard<std::mutex> lock(latch_);
  return next_page_id_++;
}

auto BufferPoolManager::FetchPageBasic(page_id_t page_id) -> BasicPageGuard {
  // std::lock_guard<std::mutex> lock(latch_);
  // std::cout << "FetchPageBasic"
  //           << "   " << page_id << std::endl;
  // auto page = FetchPage(page_id);
  // page->RLatch();
  return {this, FetchPage(page_id)};
}

auto BufferPoolManager::FetchPageRead(page_id_t page_id) -> ReadPageGuard {
  // std::lock_guard<std::mutex> lock(latch_);
  // std::cout << "FetchPageRead"
  //           << "   " << page_id << std::endl;
  auto page = FetchPage(page_id);
  // std::cout << "RLock:" << page_id << std::endl;
  page->RLatch();
  return {this, page};
}

auto BufferPoolManager::FetchPageWrite(page_id_t page_id) -> WritePageGuard {
  // std::lock_guard<std::mutex> lock(latch_);
  // std::cout << "FetchPageWrite"
  //           << "   " << page_id << std::endl;
  auto page = FetchPage(page_id);
  // std::cout << "WLock:" << page_id << std::endl;
  page->WLatch();
  // std::cout << "jjWLock:" << page_id << std::endl;
  return {this, page};
}

auto BufferPoolManager::NewPageGuarded(page_id_t *page_id) -> BasicPageGuard {
  // std::lock_guard<std::mutex> lock(latch_);
  // std::cout << "NewPageGuard" << std::endl;
  return {this, NewPage(page_id)};
}

}  // namespace bustub
