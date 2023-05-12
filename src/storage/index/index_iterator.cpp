/**
 * index_iterator.cpp
 */
#include <cassert>

#include "storage/index/index_iterator.h"

namespace bustub {

/*
 * NOTE: you can change the destructor/constructor method here
 * set your own input parameters
 */
INDEX_TEMPLATE_ARGUMENTS
INDEXITERATOR_TYPE::IndexIterator() = default;

INDEX_TEMPLATE_ARGUMENTS
INDEXITERATOR_TYPE::~IndexIterator() = default;  // NOLINT

INDEX_TEMPLATE_ARGUMENTS
auto INDEXITERATOR_TYPE::IsEnd() -> bool {
  std::cout << page_id_ << std::endl;
  return page_id_ == INVALID_PAGE_ID;
}

INDEX_TEMPLATE_ARGUMENTS
auto INDEXITERATOR_TYPE::operator*() -> const MappingType & {
  // std::cout << leaf_page_->GetIndex(page_index_).first << std::endl;
  MappingType &mapp = leaf_page_->GetIndex(page_index_);
  return mapp;

  // return MappingType();
}

INDEX_TEMPLATE_ARGUMENTS
auto INDEXITERATOR_TYPE::operator++() -> INDEXITERATOR_TYPE & {
  if (page_id_ == INVALID_PAGE_ID) {
    // write_page_.Drop();
    // std::cout << "asddsasda" << std::endl;
    // write_page_.Drop();
    return *this;
  }
  if (page_index_ < leaf_page_->GetSize() - 1) {
    page_index_++;
  } else {
    page_index_ = 0;
    page_id_ = leaf_page_->GetNextPageId();
    if (page_id_ == INVALID_PAGE_ID) {
      leaf_page_ = nullptr;
      // write_page_.Drop();
    } else {
      auto write_page = bpm_->FetchPageBasic(page_id_);
      leaf_page_ = write_page.AsMut<B_PLUS_TREE_LEAF_PAGE_TYPE>();
    }
  }
  return *this;
}

template class IndexIterator<GenericKey<4>, RID, GenericComparator<4>>;

template class IndexIterator<GenericKey<8>, RID, GenericComparator<8>>;

template class IndexIterator<GenericKey<16>, RID, GenericComparator<16>>;

template class IndexIterator<GenericKey<32>, RID, GenericComparator<32>>;

template class IndexIterator<GenericKey<64>, RID, GenericComparator<64>>;

}  // namespace bustub
