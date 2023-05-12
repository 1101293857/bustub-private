#include "storage/page/page_guard.h"
#include <iostream>
#include "buffer/buffer_pool_manager.h"

namespace bustub {

BasicPageGuard::BasicPageGuard(BasicPageGuard &&that) noexcept
    : bpm_(that.bpm_), page_(that.page_), is_dirty_(that.is_dirty_) {
  // std::cout << "BasicPageGuard&&" << std::endl;
  that.bpm_ = nullptr;
  that.page_ = nullptr;
  that.is_dirty_ = false;
}

void BasicPageGuard::Drop() {
  // std::lock_guard<std::mutex> lock(latch_);
  // std::cout << "Drop" << std::endl;
  if (page_ == nullptr) {
    return;
  }
  // std::cout << page_->GetPageId() << std::endl;
  bpm_->UnpinPage(page_->GetPageId(), is_dirty_);

  page_ = nullptr;

  // bpm_->DeletePage(page_->GetPageId());
}

auto BasicPageGuard::operator=(BasicPageGuard &&that) noexcept -> BasicPageGuard & {
  // std::lock_guard<std::mutex> lock(latch_);
  // std::cout << "operator=" << std::endl;
  // if (this->page_ != nullptr) {
  //   if (this->page_->GetPinCount() > 0) {
  //     bpm_->UnpinPage(page_->GetPageId(), false);
  //   }
  // }
  Drop();
  if (&that != this) {
    this->page_ = that.page_;
    that.page_ = nullptr;
    this->bpm_ = that.bpm_;
    // that.bpm_ = nullptr;
    this->is_dirty_ = that.is_dirty_;
    // that.is_dirty_ = false;
  }
  return *this;
}

BasicPageGuard::~BasicPageGuard() {
  // std::lock_guard<std::mutex> lock(latch_);
  // std::cout << "~BasicPageGuard" << std::endl;
  if (page_ != nullptr) {
    // std::cout << "ahsdsa" << std::endl;
    bpm_->UnpinPage(page_->GetPageId(), is_dirty_);
  }
  // std::cout << "finish ~BasicPageGuard" << std::endl;
};  // NOLINT

ReadPageGuard::ReadPageGuard(ReadPageGuard &&that) noexcept : guard_(std::move(that.guard_)) {
  // std::lock_guard<std::mutex> lock(latch_); this << "   " << &that << std::endl;
  // std::cout << "ReadPageGuard&&" << this << "   " << &that << std::endl;
}

auto ReadPageGuard::operator=(ReadPageGuard &&that) noexcept -> ReadPageGuard & {
  // std::lock_guard<std::mutex> lock(latch_);
  // std::cout << "ReadPage operator=" << this << "   " << &that << std::endl;
  if (guard_.page_ != nullptr) {
    Drop();
  }
  if (&that != this) {
    guard_ = std::move(that.guard_);
  }
  return *this;
}

void ReadPageGuard::Drop() {
  // std::lock_guard<std::mutex> lock(latch_);
  // std::cout << "ReadPageGuard Drop" << std::endl;
  // std::lock_guard<std::mutex> lock(latch_);
  if (guard_.page_ != nullptr) {
    // std::cout << "RUnLock:" << guard_.PageId() << std::endl;
    guard_.page_->RUnlatch();
    guard_.Drop();
  }
  // std::cout << "finish ReadPageGuard Drop" << std::endl;
}

ReadPageGuard::~ReadPageGuard() {
  // std::lock_guard<std::mutex> lock(latch_);
  // std::cout << "~ReadPageGuard" << std::endl;
  if (guard_.page_ != nullptr) {
    // std::cout << "RUnLock:" << guard_.PageId() << std::endl;
    guard_.page_->RUnlatch();
  }
}  // NOLINT

WritePageGuard::WritePageGuard(WritePageGuard &&that) noexcept : guard_(std::move(that.guard_)) {
  // std::lock_guard<std::mutex> lock(latch_);
  // std::cout << "WritePageGuard" << this << "   " << &that << std::endl;
  // if (guard_.page_ != nullptr) {
  // guard_.page_->RUnlatch();
  // std::cout << "RUnlatch" << std::endl;
  // }
}

auto WritePageGuard::operator=(WritePageGuard &&that) noexcept -> WritePageGuard & {
  // std::lock_guard<std::mutex> lock(latch_);
  // std::cout << "WritePageGuard operator=" << this << "   " << &that << std::endl;
  if (guard_.page_ != nullptr) {
    // std::cout << 1 << std::endl;
    Drop();
  }
  if (&that != this) {
    // std::cout << 2 << std::endl;
    guard_ = std::move(that.guard_);
  }
  return *this;
}

void WritePageGuard::Drop() {
  // std::lock_guard<std::mutex> lock(latch_);
  // std::cout << "WritePageGuard Drop" << std::endl;
  // std::lock_guard<std::mutex> lock(latch_);

  if (guard_.page_ != nullptr) {
    // std::cout << "WUnLock:" << guard_.PageId() << std::endl;
    guard_.page_->WUnlatch();
    guard_.Drop();
  }
  // std::cout << "finish WritePageGuard Drop" << std::endl;
}

WritePageGuard::~WritePageGuard() {
  // std::lock_guard<std::mutex> lock(latch_);
  // std::cout << "~WritePageGuard" << std::endl;
  if (guard_.page_ != nullptr) {
    // std::cout << "WUnLock:" << guard_.PageId() << std::endl;
    guard_.page_->WUnlatch();
  }
  // std::cout << guard_.page_->GetPinCount() << std::endl;
}  // NOLINT

}  // namespace bustub
