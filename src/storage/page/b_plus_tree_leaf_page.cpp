//===----------------------------------------------------------------------===//
//
//                         CMU-DB Project (15-445/645)
//                         ***DO NO SHARE PUBLICLY***
//
// Identification: src/page/b_plus_tree_leaf_page.cpp
//
// Copyright (c) 2018, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include <sstream>

#include "common/exception.h"
#include "common/rid.h"
#include "storage/page/b_plus_tree_leaf_page.h"

namespace bustub {

/*****************************************************************************
 * HELPER METHODS AND UTILITIES
 *****************************************************************************/

/**
 * Init method after creating a new leaf page
 * Including set page type, set current size to zero, set next page id and set max size
 */
INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_LEAF_PAGE_TYPE::Init(int max_size) {
  SetMaxSize(max_size);
  SetPageType(IndexPageType::LEAF_PAGE);
  SetSize(0);
}

/**
 * Helper methods to set/get next page id
 */
INDEX_TEMPLATE_ARGUMENTS
auto B_PLUS_TREE_LEAF_PAGE_TYPE::GetNextPageId() const -> page_id_t { return next_page_id_; }

INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_LEAF_PAGE_TYPE::SetNextPageId(page_id_t next_page_id) { next_page_id_ = next_page_id; }

INDEX_TEMPLATE_ARGUMENTS
auto B_PLUS_TREE_LEAF_PAGE_TYPE::ValueAt(int index) const -> ValueType { return array_[index].second; }
/*
 * Helper method to find and return the key associated with input "index"(a.k.a
 * array offset)
 */
INDEX_TEMPLATE_ARGUMENTS
auto B_PLUS_TREE_LEAF_PAGE_TYPE::KeyAt(int index) const -> KeyType {
  // replace with your own code
  return array_[index].first;
}

INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_LEAF_PAGE_TYPE::SetKeyValueAt(int index, const KeyType &key, const ValueType &value) {
  array_[index].first = key;
  array_[index].second = value;
}

INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_LEAF_PAGE_TYPE::Insert(const KeyType &key, const ValueType &value, const KeyComparator &comp) {
  int size = GetSize();
  if (size >= GetMaxSize()) {
    throw std::logic_error("already reached max size");
  }
  int ins_at = 0;
  while (ins_at < size && comp(array_[ins_at].first, key) < 0) {
    ins_at++;
  }

  for (int i = size; i > ins_at; i--) {
    array_[i] = array_[i - 1];
  }
  IncreaseSize(1);
  SetKeyValueAt(ins_at, key, value);
}

INDEX_TEMPLATE_ARGUMENTS
auto B_PLUS_TREE_LEAF_PAGE_TYPE::Remove(const KeyType &key, const KeyComparator &comp) -> bool {
  int size = GetSize();
  int ins_at = 0;
  while (ins_at < size && comp(array_[ins_at].first, key) != 0) {
    ins_at++;
  }
  if (ins_at == size) {
    return false;
  }
  for (int i = ins_at; i < size - 1; i++) {
    array_[i] = array_[i + 1];
  }
  IncreaseSize(-1);
  return true;
}

INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_LEAF_PAGE_TYPE::RemoveAt(int index) {
  int size = GetSize();
  for (int i = index; i < size - 1; i++) {
    array_[i] = array_[i + 1];
  }
  IncreaseSize(-1);
}
// INDEX_TEMPLATE_ARGUMENTS
// auto B_PLUS_TREE_LEAF_PAGE_TYPE::LeafBeginKey(const KeyType &key,const KeyComparator &comp) -> INDEXITERATOR_TYPE{

//   for(int i=0;i<GetSize();i++){
//     if(comp(array_[i],key)==0){
//       return array_+i;
//     }
//   }
//   return array_+GetSize();
// }

// INDEX_TEMPLATE_ARGUMENTS
// auto B_PLUS_TREE_LEAF_PAGE_TYPE::GetIndexEnd(int index) -> INDEXITERATOR_TYPE{
//   return array_+index;
// }
INDEX_TEMPLATE_ARGUMENTS
auto B_PLUS_TREE_LEAF_PAGE_TYPE::GetIndex(int index) -> MappingType & { return array_[index]; }

INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_LEAF_PAGE_TYPE::MoveData(B_PLUS_TREE_LEAF_PAGE_TYPE *new_page, int count) {
  int size = GetSize();
  int start = size - count;
  for (int i = start, j = 0; i < size; i++, j++) {
    new_page->SetKeyValueAt(j, array_[i].first, array_[i].second);
  }
  new_page->IncreaseSize(count);
  SetSize(size - count);
}

// INDEX_TEMPLATE_ARGUMENTS
// auto B_PLUS_TREE_LEAF_PAGE_TYPE::Find(const KeyType &Key, const KeyComparator &keyComparator, ValueType &value)  ->
// bool{
//   auto result = std::lower_bound(array_,array_ + GetSize(), Key, [&keyComparator](const MappingType &pair_,KeyType
//   key){
//     return keyComparator(pair_.first,key)<0;
//   });

//   if(keyComparator(result->first,key)==0){
//     value = result->second;
//     return true;
//   }
//   return false;
// }
template class BPlusTreeLeafPage<GenericKey<4>, RID, GenericComparator<4>>;
template class BPlusTreeLeafPage<GenericKey<8>, RID, GenericComparator<8>>;
template class BPlusTreeLeafPage<GenericKey<16>, RID, GenericComparator<16>>;
template class BPlusTreeLeafPage<GenericKey<32>, RID, GenericComparator<32>>;
template class BPlusTreeLeafPage<GenericKey<64>, RID, GenericComparator<64>>;
}  // namespace bustub
