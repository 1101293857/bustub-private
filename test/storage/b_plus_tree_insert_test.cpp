//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// b_plus_tree_insert_test.cpp
//
// Identification: test/storage/b_plus_tree_insert_test.cpp
//
// Copyright (c) 2015-2021, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include <algorithm>
#include <cstdio>
#include <iostream>
#include "buffer/buffer_pool_manager.h"
#include "gtest/gtest.h"
#include "storage/disk/disk_manager_memory.h"
#include "storage/index/b_plus_tree.h"
#include "test_util.h"  // NOLINT

namespace bustub {

using bustub::DiskManagerUnlimitedMemory;

TEST(BPlusTreeTests, DISABLED_InsertTest1) {  // DISABLED_
  // create KeyComparator and index schema
  auto key_schema = ParseCreateStatement("a bigint");
  GenericComparator<8> comparator(key_schema.get());

  auto disk_manager = std::make_unique<DiskManagerUnlimitedMemory>();
  auto *bpm = new BufferPoolManager(50, disk_manager.get());
  // create and fetch header_page
  page_id_t page_id;
  auto header_page = bpm->NewPage(&page_id);
  ASSERT_EQ(page_id, HEADER_PAGE_ID);
  // create b+ tree
  BPlusTree<GenericKey<8>, RID, GenericComparator<8>> tree("foo_pk", header_page->GetPageId(), bpm, comparator);
  GenericKey<8> index_key;
  RID rid;
  // create transaction
  auto *transaction = new Transaction(0);

  int64_t key = 0;
  int64_t value = key & 0xFFFFFFFF;
  rid.Set(static_cast<int32_t>(key), value);
  index_key.SetFromInteger(key);

  tree.Insert(index_key, rid, transaction);
  std::cout << "test" << std::endl;
  auto root_page_id = tree.GetRootPageId();
  auto root_page = reinterpret_cast<BPlusTreePage *>(bpm->FetchPage(root_page_id)->GetData());
  ASSERT_NE(root_page, nullptr);
  ASSERT_TRUE(root_page->IsLeafPage());

  auto root_as_leaf = reinterpret_cast<BPlusTreeLeafPage<GenericKey<8>, RID, GenericComparator<8>> *>(root_page);
  ASSERT_EQ(root_as_leaf->GetSize(), 1);
  ASSERT_EQ(comparator(root_as_leaf->KeyAt(0), index_key), 0);

  bpm->UnpinPage(root_page_id, false);
  bpm->UnpinPage(HEADER_PAGE_ID, true);
  delete transaction;
  delete bpm;
}

TEST(BPlusTreeTests, InsertTest2) {
  // create KeyComparator and index schema
  auto key_schema = ParseCreateStatement("a bigint");
  GenericComparator<8> comparator(key_schema.get());

  auto disk_manager = std::make_unique<DiskManagerUnlimitedMemory>();
  auto *bpm = new BufferPoolManager(50, disk_manager.get());
  // create and fetch header_page
  page_id_t page_id;
  auto header_page = bpm->NewPage(&page_id);
  // create b+ tree
  BPlusTree<GenericKey<8>, RID, GenericComparator<8>> tree("foo_pk", header_page->GetPageId(), bpm, comparator, 2, 3);
  GenericKey<8> index_key;
  RID rid;
  // create transaction
  auto *transaction = new Transaction(0);

  // std::vector<int64_t> keys = {8160,8026,166,5113,5925,3038,4968,2047,6350,6525,6864,5584,5098,6566,9172,
  //                             3774,748,8099,9195,3213,4559,1854,9240,1475,6897,9004,98,133,966,4408};
  std::vector<int64_t> keys;
  for (int64_t i = 1; i < 1000; i++) {
    keys.push_back(i);
  }
  for (auto key : keys) {
    int64_t value = key & 0xFFFFFFFF;
    rid.Set(static_cast<int32_t>(key >> 32), value);
    index_key.SetFromInteger(key);
    tree.Insert(index_key, rid, transaction);
    // std::cout << tree.DrawBPlusTree() << std::endl;
  }

  std::vector<RID> rids;
  for (auto key : keys) {
    rids.clear();
    index_key.SetFromInteger(key);
    tree.GetValue(index_key, &rids);
    // std::cout << "rids.size():" << rids.size() << std::endl;
    // std::cout << std::endl;
    EXPECT_EQ(rids.size(), 1);

    int64_t value = key & 0xFFFFFFFF;
    EXPECT_EQ(rids[0].GetSlotNum(), value);
  }

  int64_t size = 0;
  bool is_present;

  for (auto key : keys) {
    rids.clear();
    index_key.SetFromInteger(key);
    is_present = tree.GetValue(index_key, &rids);

    EXPECT_EQ(is_present, true);
    EXPECT_EQ(rids.size(), 1);
    EXPECT_EQ(rids[0].GetPageId(), 0);
    EXPECT_EQ(rids[0].GetSlotNum(), key);
    size = size + 1;
  }

  EXPECT_EQ(size, keys.size());

  bpm->UnpinPage(HEADER_PAGE_ID, true);
  for (auto i : bpm->replacer_->node_store_) {
    std::cout << i.second.is_evictable_ << std::endl;
  }
  delete transaction;
  delete bpm;
}

TEST(BPlusTreeTests, DISABLED_InsertTest3) {
  // create KeyComparator and index schema
  auto key_schema = ParseCreateStatement("a bigint");
  GenericComparator<8> comparator(key_schema.get());

  auto disk_manager = std::make_unique<DiskManagerUnlimitedMemory>();
  auto *bpm = new BufferPoolManager(50, disk_manager.get());
  // create and fetch header_page
  page_id_t page_id;
  auto header_page = bpm->NewPage(&page_id);
  ASSERT_EQ(page_id, HEADER_PAGE_ID);

  // create b+ tree
  BPlusTree<GenericKey<8>, RID, GenericComparator<8>> tree("foo_pk", header_page->GetPageId(), bpm, comparator, 3, 5);
  GenericKey<8> index_key;
  RID rid;
  // create transaction
  auto *transaction = new Transaction(0);
  std::vector<int64_t> keys;

  for (int64_t i = 1; i < 1000; i++) {
    keys.push_back(i);
  }
  for (auto key : keys) {
    int64_t value = key & 0xFFFFFFFF;
    rid.Set(static_cast<int32_t>(key >> 32), value);
    index_key.SetFromInteger(key);
    tree.Insert(index_key, rid, transaction);
    std::cout << tree.DrawBPlusTree() << std::endl;
  }
  // std::cout << tree.DrawBPlusTree() << std::endl;
  // std::cout << "finish" << std::endl;
  // std::vector<RID> rids;
  // std::vector<int64_t> keys1;
  // for(int64_t i=1000000;i<100000;i++){
  //   keys1.push_back(i);
  // }
  // for (auto key : keys1) {
  //   int64_t value = key & 0xFFFFFFFF;
  //   rid.Set(static_cast<int32_t>(key >> 32), value);
  //   index_key.SetFromInteger(key);
  //   tree.Insert(index_key, rid, transaction);
  // }

  // rids.clear();
  // std::vector<int64_t> keys2={20920,25570};

  // for (auto key : keys2) {
  //   rids.clear();
  //   index_key.SetFromInteger(key);
  //   tree.GetValue(index_key, &rids);
  // }

  // std::vector<int64_t> keys3={82130};

  // for (auto key : keys1) {
  //   int64_t value = key & 0xFFFFFFFF;
  //   rid.Set(static_cast<int32_t>(key >> 32), value);
  //   index_key.SetFromInteger(key);
  //   tree.Insert(index_key, rid, transaction);
  // }

  // std::vector<int64_t> remove_keys = {82131};
  // // std::vector<int64_t> remove_keys = {1, 5, 3, 4};
  for (auto key : keys) {
    index_key.SetFromInteger(key);
    tree.Remove(index_key, transaction);
    std::cout << tree.DrawBPlusTree() << std::endl;
  }
  // for (auto key : keys) {
  //   rids.clear();
  //   index_key.SetFromInteger(key);
  //   tree.GetValue(index_key, &rids);
  //   EXPECT_EQ(rids.size(), 1);

  //   int64_t value = key & 0xFFFFFFFF;
  //   EXPECT_EQ(rids[0].GetSlotNum(), value);
  // }
  // std::cout << "finish getvalue" << std::endl;
  // // while (1) {
  // // }
  // int64_t start_key = 1;
  // int64_t current_key = start_key;
  // index_key.SetFromInteger(start_key);
  // for (auto iterator = tree.Begin(index_key); iterator != tree.End(); ++iterator) {
  //   auto location = (*iterator).second;
  //   EXPECT_EQ(location.GetPageId(), 0);
  //   // EXPECT_EQ(location.GetSlotNum(), current_key);
  //   std::cout << current_key << std::endl;
  //   current_key = current_key + 1;
  //   // std::cout << "asjdghasdsa" << std::endl;
  // }

  // EXPECT_EQ(current_key, keys.size() + 1);
  // std::cout << "laiyuhua shidawag1" << std::endl;
  // start_key = 3;
  // current_key = start_key;
  // index_key.SetFromInteger(start_key);
  // for (auto iterator = tree.Begin(index_key); iterator != tree.End(); ++iterator) {
  //   std::cout << "nihao" << std::endl;
  //   auto location = (*iterator).second;
  //   EXPECT_EQ(location.GetPageId(), 0);
  //   // EXPECT_EQ(location.GetSlotNum(), current_key);
  //   current_key = current_key + 1;
  // }

  bpm->UnpinPage(HEADER_PAGE_ID, true);

  delete transaction;
  delete bpm;
}
}  // namespace bustub
