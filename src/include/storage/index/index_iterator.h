//===----------------------------------------------------------------------===//
//
//                         CMU-DB Project (15-445/645)
//                         ***DO NO SHARE PUBLICLY***
//
// Identification: src/include/index/index_iterator.h
//
// Copyright (c) 2018, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//
/**
 * index_iterator.h
 * For range scan of b+ tree
 */
#pragma once
#include "storage/page/b_plus_tree_leaf_page.h"

namespace bustub {

#define INDEXITERATOR_TYPE IndexIterator<KeyType, ValueType, KeyComparator>

INDEX_TEMPLATE_ARGUMENTS
class IndexIterator {
 public:
  // you may define your own constructor based on your member variables
  IndexIterator();
  ~IndexIterator();  // NOLINT
  IndexIterator(page_id_t page_id, page_id_t page_index, BufferPoolManager *bpm, B_PLUS_TREE_LEAF_PAGE_TYPE *leaf_page)
      : page_id_(page_id), leaf_page_(leaf_page), page_index_(page_index), bpm_(bpm) {
    // if(page_id != INVALID_PAGE_ID){
    //   page_ = bpm->FetchPageWrite(page_id);
    //   std::cout << "index FetchPage Write" << std::endl;
    //   std::cout << std::endl;
    //   leaf_page_ = page_.AsMut<B_PLUS_TREE_LEAF_PAGE_TYPE>();
    // }
    // std::cout << "indexiterator" << std::endl;
    // std::cout << std::endl;
    // if (page_id != INVALID_PAGE_ID) {
    //   std::cout << page_id << "   " << INVALID_PAGE_ID << std::endl;
    //   // write_page_ = bpm->FetchPageWrite(page_id);

    // } else {
    //   std::cout << "dashabi" << std::endl;
    // }
    // // std::cout << write_page_.PageId() << std::endl;
    // std::cout << std::endl;
  }

  auto IsEnd() -> bool;

  auto operator*() -> const MappingType &;

  auto operator++() -> IndexIterator &;

  auto operator==(const IndexIterator &itr) const -> bool {
    return page_id_ == itr.page_id_ && page_index_ == itr.page_index_;
  }

  auto operator!=(const IndexIterator &itr) const -> bool {
    return page_id_ != itr.page_id_ || page_index_ != itr.page_index_;
  }

 public:
  // add your own private member variables here
  page_id_t page_id_ = INVALID_PAGE_ID;
  B_PLUS_TREE_LEAF_PAGE_TYPE *leaf_page_ = nullptr;
  int page_index_ = 0;
  BufferPoolManager *bpm_ = nullptr;
};

}  // namespace bustub
