#include <iostream>
#include <sstream>
#include <string>

#include "common/exception.h"
#include "common/logger.h"
#include "common/rid.h"
#include "storage/index/b_plus_tree.h"

namespace bustub {

INDEX_TEMPLATE_ARGUMENTS
BPLUSTREE_TYPE::BPlusTree(std::string name, page_id_t header_page_id, BufferPoolManager *buffer_pool_manager,
                          const KeyComparator &comparator, int leaf_max_size, int internal_max_size)
    : index_name_(std::move(name)),
      bpm_(buffer_pool_manager),
      comparator_(std::move(comparator)),
      leaf_max_size_(leaf_max_size),
      internal_max_size_(internal_max_size),
      header_page_id_(header_page_id) {
  std::cout << "leaf_max_size:" << leaf_max_size_ << "    "
            << "internal_max_size:" << internal_max_size_ << std::endl;
  root_latch_.WLock();
  auto guard = bpm_->FetchPageBasic(header_page_id_);
  auto root_page = guard.AsMut<BPlusTreeHeaderPage>();
  root_page->root_page_id_ = INVALID_PAGE_ID;
  root_latch_.WUnlock();
}

INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::IsPageSafe(BPlusTreePage *tree_page, Operation op, bool IsRootPage) -> bool {
  if (op == Operation::Read) {
    return true;
  }
  if (op == Operation::Insert) {
    if (tree_page->IsLeafPage()) {
      return tree_page->GetSize() < tree_page->GetMaxSize() - 1;
    }
    return tree_page->GetSize() < tree_page->GetMaxSize();
  }
  if (op == Operation::Remove) {
    if (IsRootPage) {
      if (tree_page->IsLeafPage()) {
        return tree_page->GetSize() > 1;
      }
      return tree_page->GetSize() > 2;
    }
    return tree_page->GetSize() > tree_page->GetMinSize();
  }
  return false;
}

// INDEX_TEMPLATE_ARGUMENTS
// void  BPLUSTREE_TYPE::ReleaseWpages(std::deque<Page *> &depage){
//   // if(depage == nullptr){
//   //   return;
//   // }
//   while(!depage.empty()){
//     Page *page = depage.front();
//     depage.pop_front();
//     if(page == nullptr){
//       std::cout << "root_latch  WUnlock" << std::endl;
//       root_latch_.WUnlock();
//     }else{
//       std::cout << "page   " << page->GetPageId() << "  WUnlock" << std::endl;
//       page->WUnlatch();
//     }
//   }
// }
INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::ReleasePages(std::deque<Page *> &transaction) {
  while (!transaction.empty()) {
    Page *page = transaction.front();
    transaction.pop_front();
    if (page == nullptr) {
      // std::cout << std::this_thread::get_id() << "root_latch  WUnlock" << std::endl;
      root_latch_.WUnlock();
    } else {
      // std::cout << std::this_thread::get_id() << "page  " << page->GetPageId() << "  WUnlock" << std::endl;
      page->WUnlatch();
      bpm_->UnpinPage(page->GetPageId(), true);
    }
  }
}
/*
 * Helper function to decide whether current b+tree is empty
 */
INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::IsEmpty() const -> bool {
  BasicPageGuard guard = bpm_->FetchPageBasic(header_page_id_);
  auto root_page = guard.AsMut<BPlusTreeHeaderPage>();
  guard.is_dirty_ = false;
  bool found = false;
  if (root_page->root_page_id_ == INVALID_PAGE_ID) {
    found = true;
  }
  return found;
}
/*****************************************************************************
 * SEARCH
 *****************************************************************************/
/*
 * Return the only value that associated with input key
 * This method is used for point query
 * @return : true means key exists
 */
INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::GetValue(const KeyType &key, std::vector<ValueType> *result, Transaction *txn) -> bool {
  // std::lock_guard<std::mutex> lock(latch_1_);
  // Declaration of context instance.
  bool found = false;
  // {
  //   std::lock_guard<std::mutex> lock(latch_);
  //   std::cout << std::this_thread::get_id() << "GetValue"
  //             << "  " << key << std::endl;
  // }
  // std::cout << "root_latch Rlock" << std::endl;
  root_latch_.RLock();
  if (IsEmpty()) {
    root_latch_.RUnlock();
    // {
    //   std::lock_guard<std::mutex> lock(latch_);
    //   std::cout << std::this_thread::get_id() << "finish GetValue"
    //             << "  " << key << std::endl;
    // }
    return false;
  }
  // std::cout << this->DrawBPlusTree() << std::endl;
  std::deque<Page *> depage;
  std::deque<page_id_t> nihaoa;
  BasicPageGuard page = GetLeafPage(key, depage, Operation::Read, nihaoa);
  // if(page.guard_ == nullptr){
  //   return false;
  // }
  // auto leaf_page1 = reinterpret_cast<BPlusTreePage *>(page.GetData());
  // auto leaf_page1 = page.As<BPlusTreePage>();
  auto *leaf_page = page.AsMut<LeafPage>();
  // std::cout << leaf_page->KeyAt(0) << std::endl;
  for (int i = 0; i < leaf_page->GetSize(); i++) {
    if (comparator_(leaf_page->KeyAt(i), key) == 0) {
      found = true;
      result->push_back(leaf_page->ValueAt(i));
    }
  }
  page.page_->RUnlatch();
  // {
  //   std::lock_guard<std::mutex> lock(latch_);
  //   std::cout << std::this_thread::get_id() << "finish GetValue"
  //             << "  " << key << std::endl;
  // }
  // std::cout << "page " << page.PageId() << " Runlock" << std::endl;
  return found;
}

// INDEX_TEMPLATE_ARGUMENTS
// auto BPLUSTREE_TYPE::GetLeafPage(const KeyType &key) -> BasicPageGuard {  // 获得该key所在的页页面
//   // std::cout << "start GetLeafPage" << std::endl;

//   auto next_page_id = header_page_id_;
//   if (IsEmpty()) {
//     return {nullptr, nullptr};
//   }

//   while (true) {
//     // auto ReadGuard = bpm_->FetchPageRead(next_page_id);

//     // // 要将页面的数据强行转换为 b+树页面才能获取数据
//     // // auto page = reinterpret_cast<BPlusTreePage *>(ReadGuard.GetData());
//     // auto page = ReadGuard.As<BPlusTreePage>();
//     // std::cout << "start 1 Getleafpage" << std::endl;
//     BasicPageGuard write_guard = bpm_->FetchPageBasic(next_page_id);
//     auto page = write_guard.AsMut<BPlusTreePage>();
//     if (page->IsLeafPage()) {
//       return write_guard;
//     }

//     // auto internal_page = static_cast<InternalPage *>(page);
//     auto internal_page = write_guard.AsMut<InternalPage>();
//     next_page_id = internal_page->ValueAt(internal_page->GetSize() - 1);
//     for (int i = 1; i < internal_page->GetSize(); i++) {
//       // std::cout << internal_page->KeyAt(i) << "   " << key << std::endl;
//       if (comparator_(internal_page->KeyAt(i), key) > 0) {
//         next_page_id = internal_page->ValueAt(i - 1);
//         break;
//         // std::cout << "internal_page" << i-1 << std::endl;
//       }
//     }
//   }
// }

INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::GetLeafPage(const KeyType &key, std::deque<Page *> &transaction, Operation op,
                                 std::deque<page_id_t> &parent_page_id) -> BasicPageGuard {  // 获得该key所在的页页面
  // std::cout << "start GetLeafPage" << std::endl;
  // std::cout << "GetLeafPage1" << std::endl;
  if (op != Operation::Read) {
    // std::cout << "GetLeafPage2" << std::endl;
    transaction.push_back(nullptr);
  }
  // root_latch_.WLock();
  // if (IsEmpty()) {
  //   return {nullptr, nullptr};
  // }
  // std::cout << "GetLeafPage3" << std::endl;
  bool flag = true;
  auto next_page_id = header_page_id_;
  Page *prev_page = nullptr;
  while (true) {
    // auto ReadGuard = bpm_->FetchPageRead(next_page_id);

    // // 要将页面的数据强行转换为 b+树页面才能获取数据
    // // auto page = reinterpret_cast<BPlusTreePage *>(ReadGuard.GetData());
    // auto page = ReadGuard.As<BPlusTreePage>();
    // std::cout << "start 1 Getleafpage" << std::endl;
    BasicPageGuard write_guard = bpm_->FetchPageBasic(next_page_id);
    auto curr_page = write_guard.page_;
    auto page = write_guard.AsMut<BPlusTreePage>();
    if (op == Operation::Read) {
      // std::cout << "page " << curr_page->GetPageId() << "Rlock " << std::endl;
      curr_page->RLatch();
      if (prev_page == nullptr) {
        root_latch_.RUnlock();
        // std::cout << "root_latch RUnlock " << std::endl;
      } else {
        prev_page->RUnlatch();
        // std::cout << "page RUnlock " << prev_page->GetPageId() << std::endl;
      }
      prev_page = curr_page;
    } else {
      // std::cout << std::this_thread::get_id() << "page  " << write_guard.PageId() << std::endl;
      curr_page->WLatch();
      // std::cout << std::this_thread::get_id() << "page  " << write_guard.PageId() << "  Wlock" << std::endl;
      if (IsPageSafe(page, op, flag)) {
        ReleasePages(transaction);
      }
      transaction.push_back(bpm_->FetchPage(next_page_id));
    }
    if (page->IsLeafPage()) {
      return write_guard;
    }
    parent_page_id.push_back(write_guard.PageId());

    // auto internal_page = static_cast<InternalPage *>(page);
    auto internal_page = write_guard.AsMut<InternalPage>();
    if (IsPageSafe(page, op, flag)) {
      write_guard.is_dirty_ = false;
    }
    flag = false;
    next_page_id = internal_page->ValueAt(internal_page->GetSize() - 1);
    for (int i = 1; i < internal_page->GetSize(); i++) {
      // std::cout << internal_page->KeyAt(i) << "   " << key << std::endl;
      if (comparator_(internal_page->KeyAt(i), key) > 0) {
        next_page_id = internal_page->ValueAt(i - 1);
        break;
        // std::cout << "internal_page" << i-1 << std::endl;
      }
    }
  }
}

INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::ReleaseOnePages(std::deque<Page *> &transaction) {
  if (!transaction.empty()) {
    Page *page = transaction.back();
    transaction.pop_back();
    if (page == nullptr) {
      // std::cout << std::this_thread::get_id() << "root_latch  WUnlock" << std::endl;
      root_latch_.WUnlock();
    } else {
      // std::cout << std::this_thread::get_id() << "page  " << page->GetPageId() << "  WUnlock" << std::endl;
      page->WUnlatch();
      bpm_->UnpinPage(page->GetPageId(), true);
    }
  }
}
/*****************************************************************************
 * INSERTION
 *****************************************************************************/
/*
 * Insert constant key & value pair into b+ tree
 * if current tree is empty, start new tree, update root page id and insert
 * entry, otherwise insert into leaf page.
 * @return: since we only support unique key, if user try to insert duplicate
 * keys return false, otherwise return true.
 */
INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::Insert(const KeyType &key, const ValueType &value, Transaction *txn) -> bool {
  // Declaration of context instance.
  // 当前B+树为空 需要新建树
  // std::cout << "nihao" << std::endl;
  // std::lock_guard<std::mutex> lock(latch_1_);
  // std::cout << std::this_thread::get_id() << "root_latch_" << std::endl;
  // {
  //   // std::lock_guard<std::mutex> lock(latch_);
  //   std::cout << std::this_thread::get_id() << "Insert"
  //             << "  " << key << std::endl;
  // }
  // std::cout << std::this_thread::get_id() << "root_latch" << std::endl;
  root_latch_.WLock();
  // std::cout << std::this_thread::get_id() << "root_latch_  Wlock" << std::endl;
  if (IsEmpty()) {
    // while(1){
    // root_latch_.RUnlock();
    // root_latch_.WLock();
    // }
    // std::cout << "empty" << std::endl;
    // if(IsEmpty()){
    // auto leaf_page1 = reinterpret_cast<BPlusTreePage *>(page.GetData());
    // auto leaf_page1 = page.As<BPlusTreePage>();
    auto page_header = bpm_->FetchPageBasic(header_page_id_);
    auto root_page = page_header.AsMut<BPlusTreeHeaderPage>();
    root_page->root_page_id_ = header_page_id_;
    auto *leaf_page = page_header.AsMut<LeafPage>();

    leaf_page->Init(leaf_max_size_);
    leaf_page->SetNextPageId(INVALID_PAGE_ID);
    leaf_page->IncreaseSize(1);
    leaf_page->SetPageType(IndexPageType::LEAF_PAGE);
    leaf_page->SetKeyValueAt(0, key, value);
    // page_header.Drop();
    // std::cout << this->DrawBPlusTree() << std::endl;
    root_latch_.WUnlock();
    // std::cout << std::this_thread::get_id() << "root_latch_  Wunlock" << std::endl;
    return true;
    // }
    // root_latch_.WUnlock();
    // root_latch_.RLock();
  }
  // std::cout << this->DrawBPlusTree() << std::endl;
  // std::cout << "finish empty" << std::endl;
  // std::deque<Page *> depage;
  std::deque<Page *> depage;
  std::deque<page_id_t> nihaoa;
  BasicPageGuard page = GetLeafPage(key, depage, Operation::Insert, nihaoa);
  // auto leaf_page2 = reinterpret_cast<BPlusTreePage *>(page.GetData());
  // auto leaf_page2 = page.As<BPlusTreePage>();
  auto *leaf_page = page.AsMut<LeafPage>();

  // 在叶节点中找到该元素  直接返回false
  for (int i = 0; i < leaf_page->GetSize(); i++) {
    if (comparator_(leaf_page->KeyAt(i), key) == 0) {
      ReleasePages(depage);
      // {
      //   // std::lock_guard<std::mutex> lock(latch_);
      //   std::cout << std::this_thread::get_id() << "finish insert" << key << std::endl;
      // }
      return false;
    }
  }

  // 叶节点是达到最大值就分裂   内部节点是要达到最大值+1 才分裂
  leaf_page->Insert(key, value, comparator_);
  if (leaf_page->GetSize() < leaf_max_size_) {
    ReleasePages(depage);
    // {
    //   // std::lock_guard<std::mutex> lock(latch_);
    //   std::cout << std::this_thread::get_id() << "finish insert" << key << std::endl;
    // }
    return true;
  }
  // 分裂  将一半数据  移到新页面上
  page_id_t new_page_id;
  auto new_page = bpm_->NewPageGuarded(&new_page_id);
  // auto new_leaf_page2 = reinterpret_cast<BPlusTreePage *>(new_page.GetData());
  // auto new_leaf_page2 = new_page.As<BPlusTreePage>();
  auto new_leaf_page = new_page.AsMut<LeafPage>();

  new_leaf_page->Init(leaf_max_size_);
  new_leaf_page->SetNextPageId(leaf_page->GetNextPageId());
  new_leaf_page->SetPageType(IndexPageType::LEAF_PAGE);

  leaf_page->SetNextPageId(new_page_id);
  leaf_page->MoveData(new_leaf_page, (leaf_max_size_ + 1) / 2);

  // BPlusTreePage *old_tree_page = leaf_page;
  // BPlusTreePage *new_tree_page = new_leaf_page;
  KeyType split_key = new_leaf_page->KeyAt(0);
  page_id_t new_tree_page_id = new_page.PageId();
  page_id_t old_tree_page_id = page.PageId();

  // std::deque<page_id_t> key;
  // std::deque<page_id_t> value;

  new_page.Drop();
  page.Drop();
  while (true) {
    if (nihaoa.empty()) {  // root  节点分裂
      page_id_t page_id1;
      auto new_page1 = bpm_->NewPageGuarded(&page_id1);
      // auto new_root_page3 = reinterpret_cast<BPlusTreePage *>(new_page1.GetData());
      // auto new_root_page3 = new_page1.As<BPlusTreePage>();
      auto *new_root_page = new_page1.AsMut<InternalPage>();
      new_root_page->Init(internal_max_size_);
      new_root_page->SetKeyValueAt(0, split_key, old_tree_page_id);
      new_root_page->SetKeyValueAt(1, split_key, new_tree_page_id);
      new_root_page->IncreaseSize(2);
      new_root_page->SetPageType(IndexPageType::INTERNAL_PAGE);
      header_page_id_ = page_id1;
      // SetRootPageId();
      break;
    }

    auto old_tree_page_parent = bpm_->FetchPageBasic(nihaoa.back());
    nihaoa.pop_back();

    // auto internal_old_tree_page_parent1 = reinterpret_cast<BPlusTreePage *>(old_tree_page_parent.GetData());
    // auto internal_old_tree_page_parent1 = old_tree_page_parent.As<BPlusTreePage>();
    auto internal_old_tree_page_parent = old_tree_page_parent.AsMut<InternalPage>();
    // std::cout << "Before size" << internal_old_tree_page_parent->GetSize() << std::endl;
    internal_old_tree_page_parent->Insert(split_key, new_tree_page_id, comparator_);
    // std::cout << "After size" << internal_old_tree_page_parent->GetSize() << std::endl;
    // 内部节点要达到最大值加1才分裂

    if (internal_old_tree_page_parent->GetSize() <= internal_max_size_) {
      break;
    }

    page_id_t new_tree_page_parent_id;
    auto new_tree_page_parent = bpm_->NewPageGuarded(&new_tree_page_parent_id);
    // auto internal_new_tree_page_parent1 = reinterpret_cast<BPlusTreePage*>(new_tree_page_parent.GetData());
    // auto internal_new_tree_page_parent1 = new_tree_page_parent.As<BPlusTreePage>();
    auto internal_new_tree_page_parent = new_tree_page_parent.AsMut<InternalPage>();
    internal_new_tree_page_parent->Init(internal_max_size_);

    internal_new_tree_page_parent->SetPageType(IndexPageType::INTERNAL_PAGE);
    int new_parent_page_size = (internal_max_size_ + 1) / 2;
    // 移动数据
    auto start = internal_old_tree_page_parent->GetSize() - new_parent_page_size;
    // 将内部页面的数据一半移动到新内部页面中  可以放到0位置上  因为0位置上的key是要向上传递的  不在新内部页面保留
    for (auto i = start, j = 0; i < internal_old_tree_page_parent->GetSize(); i++, j++) {
      internal_new_tree_page_parent->SetKeyValueAt(j, internal_old_tree_page_parent->KeyAt(i),
                                                   internal_old_tree_page_parent->ValueAt(i));
    }
    internal_old_tree_page_parent->SetSize(internal_max_size_ - new_parent_page_size + 1);
    internal_new_tree_page_parent->SetSize(new_parent_page_size);

    old_tree_page_id = old_tree_page_parent.PageId();
    new_tree_page_id = new_tree_page_parent.PageId();
    split_key = internal_new_tree_page_parent->KeyAt(0);
    // ReleasePages(depage);
  }

  ReleasePages(depage);

  // {
  //   // std::lock_guard<std::mutex> lock(latch_);
  //   std::cout << std::this_thread::get_id() << "finish insert" << key << std::endl;
  // }
  // std::cout << this->DrawBPlusTree() << std::endl;
  return true;
}

INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::GetSiblings(page_id_t page_id, page_id_t &left_sibling_id, page_id_t &right_sibling_id,
                                 Transaction *transaction, std::deque<page_id_t> &re_parent_page_id) {
  if (re_parent_page_id.empty()) {
    throw std::invalid_argument("cannot get sibling of the root page");
  }
  BasicPageGuard write_guard = bpm_->FetchPageBasic(re_parent_page_id.back());

  auto parent_node = write_guard.AsMut<InternalPage>();
  int index = parent_node->FindValue(page_id);
  // std::cout << index << std::endl;
  if (index == -1) {
    throw std::logic_error("Child page is not in parent's value");
  }

  left_sibling_id = right_sibling_id = INVALID_PAGE_ID;

  if (index != 0) {
    left_sibling_id = parent_node->ValueAt(index - 1);
  }

  if (index != parent_node->GetSize() - 1) {
    right_sibling_id = parent_node->ValueAt(index + 1);
  }
  write_guard.is_dirty_ = false;
  // std::cout << left_sibling_id << "   " << right_sibling_id << std::endl;
  // if (left_sibling_id != INVALID_PAGE_ID) {
  //   // std::cout << "llpage " << left_sibling_id << " Wlock" << std::endl;
  //   auto left_page = bpm_->FetchPage(left_sibling_id);
  //   left_page->WLatch();
  // }
  // if (right_sibling_id != INVALID_PAGE_ID) {
  //   // std::cout << "rrpage " << right_sibling_id << " Wlock" << std::endl;
  //   auto right_page = bpm_->FetchPage(right_sibling_id);
  //   right_page->WLatch();
  // }
}

INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::TryBorrow(page_id_t page_id, page_id_t sibling_page_id, InternalPage *parent_page,
                               bool sibling_at_left) -> bool {
  if (sibling_page_id == INVALID_PAGE_ID) {
    return false;
  }
  BasicPageGuard sibling_guard = bpm_->FetchPageBasic(sibling_page_id);
  sibling_guard.page_->WLatch();  //++
  auto sibling_page = sibling_guard.AsMut<BPlusTreePage>();
  if (sibling_page->GetSize() <= sibling_page->GetMinSize()) {
    sibling_guard.page_->WUnlatch();  //++
    sibling_guard.is_dirty_ = false;
    return false;
  }

  BasicPageGuard page_guard = bpm_->FetchPageBasic(page_id);
  auto page = page_guard.AsMut<BPlusTreePage>();
  // 从兄弟节点中借的位置
  int sibling_borrow_at = sibling_at_left ? sibling_page->GetSize() - 1 : (page->IsLeafPage() ? 0 : 1);
  // 如果向左兄弟借  那么需要将当前index换成第一个值  如果向右兄弟借 那么需要更新右兄弟父节点的位置  即+1
  int parent_update_at = parent_page->FindValue(page_id) + (sibling_at_left ? 0 : 1);
  KeyType update_key;

  if (page->IsLeafPage()) {
    // WritePageGuard page_guard = bpm_->FetchPageWrite(page_id);

    auto leaf_page = page_guard.AsMut<LeafPage>();
    auto leaf_sibling_page = sibling_guard.AsMut<LeafPage>();
    leaf_page->Insert(leaf_sibling_page->KeyAt(sibling_borrow_at), leaf_sibling_page->ValueAt(sibling_borrow_at),
                      comparator_);
    leaf_sibling_page->RemoveAt(sibling_borrow_at);
    // 如果使用的是左兄弟节点  则将当前节点的第0个key传递给父节点  若是使用的是右兄弟节点
    // 则使用右兄弟的第0个传递给父节点
    update_key = sibling_at_left ? leaf_page->KeyAt(0) : leaf_sibling_page->KeyAt(0);
  } else {
    // 如果是内部节点
    // WritePageGuard page_guard = bpm_->FetchPageWrite(page_id);
    auto internal_page = page_guard.AsMut<InternalPage>();
    auto internal_sibling_page = sibling_guard.AsMut<InternalPage>();
    // 父节点需要更新的节点  有点类似平衡二叉树  要先将节点传到父节点  再把原先父节点的key传下来
    update_key = internal_sibling_page->KeyAt(sibling_borrow_at);
    if (sibling_at_left) {
      // key是父节点的key  value是兄弟节点的value
      internal_page->Insert(parent_page->KeyAt(parent_update_at), internal_page->ValueAt(0), comparator_);
      internal_page->SetValueAt(0, internal_sibling_page->ValueAt(sibling_borrow_at));
    } else {
      internal_page->SetKeyValueAt(internal_page->GetSize(), parent_page->KeyAt(parent_update_at),
                                   internal_sibling_page->ValueAt(0));
      internal_page->IncreaseSize(1);
      internal_sibling_page->SetValueAt(0, internal_sibling_page->ValueAt(1));
    }
    internal_sibling_page->RemoveAt(sibling_borrow_at);
  }
  parent_page->SetKeyAt(parent_update_at, update_key);
  sibling_guard.page_->WUnlatch();  //++
  return true;
}

INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::MergePage(page_id_t left_page_id, page_id_t right_page_id, InternalPage *parent_page,
                               Transaction *transaction, bool flag) {
  // std::cout << "Mergepage" << std::endl;
  BasicPageGuard left_page_guard = bpm_->FetchPageBasic(left_page_id);
  BasicPageGuard right_page_guard = bpm_->FetchPageBasic(right_page_id);
  // if (flag) {
  //   right_page_guard.page_->WLatch();  //++
  // } else {
  //   left_page_guard.page_->WLatch();
  // }
  // {
  //   std::lock_guard<std::mutex> lock(latch_);
  //   std::cout << std::this_thread::get_id() << "574  " << std::endl;
  // }
  // transaction->AddIntoDeletedPageSet(right_page_guard.PageId());
  // {
  //   std::lock_guard<std::mutex> lock(latch_);
  //   std::cout << std::this_thread::get_id() << "578  " << std::endl;
  // }
  auto left_page = left_page_guard.AsMut<BPlusTreePage>();
  // {
  //   std::lock_guard<std::mutex> lock(latch_);
  //   std::cout << std::this_thread::get_id() << "584  " << std::endl;
  // }
  // auto right_page = right_page_guard.AsMut<BPlusTreePage>();
  if (left_page->IsLeafPage()) {
    // {
    //   std::lock_guard<std::mutex> lock(latch_);
    //   std::cout << std::this_thread::get_id() << "582  " << std::endl;
    // }
    auto leaf_left_page = left_page_guard.AsMut<LeafPage>();
    auto leaf_right_page = right_page_guard.AsMut<LeafPage>();
    // 把右边节点的值送到左边节点
    for (int i = 0; i < leaf_right_page->GetSize(); i++) {
      leaf_left_page->Insert(leaf_right_page->KeyAt(i), leaf_right_page->ValueAt(i), comparator_);
    }
    // 设置新的下一个节点
    leaf_left_page->SetNextPageId(leaf_right_page->GetNextPageId());
    // 在父节点中移除该值
    parent_page->RemoveAt(parent_page->FindValue(right_page_id));
  } else {
    // {
    //   std::lock_guard<std::mutex> lock(latch_);
    //   std::cout << std::this_thread::get_id() << "597  " << std::endl;
    // }
    auto left_internal_page = left_page_guard.AsMut<InternalPage>();
    auto right_internal_page = right_page_guard.AsMut<InternalPage>();
    left_internal_page->Insert(parent_page->KeyAt(parent_page->FindValue(right_page_id)),
                               right_internal_page->ValueAt(0), comparator_);

    parent_page->RemoveAt(parent_page->FindValue(right_page_id));
    for (int i = 1; i < right_internal_page->GetSize(); i++) {
      left_internal_page->Insert(right_internal_page->KeyAt(i), right_internal_page->ValueAt(i), comparator_);
    }
  }
  // {
  //   std::lock_guard<std::mutex> lock(latch_);
  //   std::cout << std::this_thread::get_id() << "618  " << std::endl;
  // }
  // if (flag) {
  //   right_page_guard.page_->WUnlatch();  //++
  // } else {
  //   left_page_guard.page_->WUnlatch();
  // }
}

INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::HandleUnderflow(page_id_t page_id, Transaction *tranction,
                                     std::deque<page_id_t> &re_parent_page_id) {
  if (re_parent_page_id.empty()) {
    // 递归到根节点  若根节点是叶子节点  不管直接返回 若根节点size大于1（此时根节点是内部节点）才能直接返回
    BasicPageGuard page_guard = bpm_->FetchPageBasic(page_id);
    auto page = page_guard.AsMut<BPlusTreePage>();
    if (page->IsLeafPage() || page->GetSize() > 1) {
      return;
    }

    auto old_root_page = page_guard.AsMut<InternalPage>();
    header_page_id_ = old_root_page->ValueAt(0);

    BasicPageGuard write_page = bpm_->FetchPageBasic(header_page_id_);
    auto leaf_page = write_page.AsMut<LeafPage>();
    if (leaf_page->GetSize() == 0) {
      auto root_page = write_page.AsMut<BPlusTreeHeaderPage>();
      root_page->root_page_id_ = INVALID_PAGE_ID;
    }
    // WritePageGuard write_page = bpm_->FetchPageWrite(header_page_id_);
    // auto root_page = write_page.AsMut<BPlusTreeHeaderPage>();
    // root_page->root_page_id_ = header_page_id_;
    // std::cout << "header_id" << header_page_id_ << std::endl;
    return;
  }
  // {
  //   std::lock_guard<std::mutex> lock(latch_);
  //   std::cout << std::this_thread::get_id() << "646  " << std::endl;
  // }
  page_id_t left_sibling_id;
  page_id_t right_sibling_id;
  GetSiblings(page_id, left_sibling_id, right_sibling_id, tranction, re_parent_page_id);
  // std::cout << "left_sibling" << left_sibling_id << std::endl;

  if (left_sibling_id == INVALID_PAGE_ID && right_sibling_id == INVALID_PAGE_ID) {
    throw std::logic_error("Child page is not in parent's value");
  }

  // BPlusTreePage* left_sibling_page = nullptr;
  // BPlusTreePage* right_sibling_page = nullptr;

  // if(left_sibling_id!=INVALID_PAGE_ID){
  //   WritePageGuard write_guard = bpm_->FetchPageWrite(left_sibling_id);
  //   left_sibling_page = write_guard.AsMut<BPlusTreePage>();
  // }

  // if(right_sibling_page!=INVALID_PAGE_ID){
  //   WritePageGuard write_guard = bpm_->FetchPageWrite(right_sibling_id);
  //   right_sibling_page = write_guard.AsMut<BPlusTreePage>();
  // }

  BasicPageGuard write_guard = bpm_->FetchPageBasic(re_parent_page_id.back());

  auto parent_page = write_guard.AsMut<InternalPage>();
  re_parent_page_id.pop_back();
  // 能够借到兄弟节点的值  就借

  if (TryBorrow(page_id, left_sibling_id, parent_page, true) ||
      TryBorrow(page_id, right_sibling_id, parent_page, false)) {
    // std::cout << "try Borrow" << std::endl;
    // if (left_sibling_id != INVALID_PAGE_ID) {
    //   // std::cout << "page " << left_sibling_id << "  Wunlock" << std::endl;
    //   auto left_sibling_page = bpm_->FetchPage(left_sibling_id);
    //   left_sibling_page->WUnlatch();
    //   bpm_->UnpinPage(left_sibling_id, true);
    // }
    // if (right_sibling_id != INVALID_PAGE_ID) {
    //   // std::cout << "page " << right_sibling_id << "  Wunlock" << std::endl;
    //   auto right_sibling_page = bpm_->FetchPage(right_sibling_id);
    //   right_sibling_page->WUnlatch();
    //   bpm_->UnpinPage(right_sibling_id, true);
    // }
    return;
  }
  // std::cout << "nihao" << std::endl;
  // 借不到就合并
  page_id_t left_page;
  page_id_t right_page;
  bool flag = false;
  if (left_sibling_id != INVALID_PAGE_ID) {
    left_page = left_sibling_id;
    right_page = page_id;
  } else {
    flag = true;
    left_page = page_id;
    right_page = right_sibling_id;
  }
  // {
  //   std::lock_guard<std::mutex> lock(latch_);
  //   std::cout << std::this_thread::get_id() << "714  " << std::endl;
  // }
  MergePage(left_page, right_page, parent_page, tranction, flag);
  // std::cout << this->DrawBPlusTree() << std::endl;
  // std::cout << "nihao" << std::endl;
  page_id_t write_guard_id = write_guard.PageId();
  // write_guard.Drop();
  // std::cout << left_sibling_id << "   " << right_sibling_id << std::endl;
  // if (left_sibling_id != INVALID_PAGE_ID) {
  //   // std::cout << "page " << left_sibling_id << "  Wunlock" << std::endl;
  //   auto left_sbling_page = bpm_->FetchPage(left_sibling_id);
  //   left_sbling_page->WUnlatch();
  //   bpm_->UnpinPage(left_sibling_id, true);
  // }
  // if (right_sibling_id != INVALID_PAGE_ID) {
  //   // std::cout << "page " << right_sibling_id << "  Wunlock" << std::endl;
  //   auto right_sibling_page = bpm_->FetchPage(right_sibling_id);
  //   right_sibling_page->WUnlatch();
  //   bpm_->UnpinPage(right_sibling_id, true);
  // }
  // std::cout << this->DrawBPlusTree() << std::endl;
  // {
  //   std::lock_guard<std::mutex> lock(latch_);
  //   std::cout << std::this_thread::get_id() << "737  " << std::endl;
  // }
  if (parent_page->GetSize() < parent_page->GetMinSize()) {
    HandleUnderflow(write_guard_id, tranction, re_parent_page_id);
    // std::cout << "finish hundleunderflow" << std::endl;
  }
  // {
  //   std::lock_guard<std::mutex> lock(latch_);
  //   std::cout << std::this_thread::get_id() << "742  " << std::endl;
  // }
}
/*****************************************************************************
 * REMOVE
 *****************************************************************************/
/*
 * Delete key & value pair associated with input key
 * If current tree is empty, return immediately.
 * If not, User needs to first find the right leaf page as deletion target, then
 * delete entry from leaf page. Remember to deal with redistribute or merge if
 * necessary.
 */
INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::Remove(const KeyType &key, Transaction *txn) {
  // std::lock_guard<std::mutex> lock(latch_1_);
  // std::cout << "root_latch" << std::endl;
  // {
  //   std::lock_guard<std::mutex> lock(latch_);
  //   std::cout << std::this_thread::get_id() << "Remove  " << key << std::endl;
  // }
  root_latch_.WLock();
  // std::cout << "root_latch Wlock" << std::endl;
  // Declaration of context instance.
  if (IsEmpty()) {
    root_latch_.WUnlock();
    // {
    //   std::lock_guard<std::mutex> lock(latch_);
    //   std::cout << std::this_thread::get_id() << "finish Remove  " << key << std::endl;
    // }
    // std::cout << "root_latch Wlock" << std::endl;
    return;
  }
  // std::cout << this->DrawBPlusTree() << std::endl;
  // std::cout << "Remove"
  // << "  " << key << std::endl;
  std::deque<Page *> depage;
  std::deque<page_id_t> nihaoa;
  BasicPageGuard guard = GetLeafPage(key, depage, Operation::Remove, nihaoa);

  auto leaf_page = guard.AsMut<LeafPage>();
  // std::cout << key << "   " << leaf_page->KeyAt(0) << std::endl;

  // std::cout << guard.PageId() << std::endl;
  leaf_page->Remove(key, comparator_);

  if (nihaoa.empty()) {
    // std::cout << "remove empty" << std::endl;
    ReleasePages(depage);
    // {
    //   std::lock_guard<std::mutex> lock(latch_);
    //   std::cout << std::this_thread::get_id() << "finish Remove  " << key << std::endl;
    // }
    return;
  }
  // std::cout << guard.PageId() << std::endl;
  page_id_t guard_id = guard.PageId();
  // guard.Drop();
  // {
  //   std::lock_guard<std::mutex> lock(latch_);
  //   std::cout << std::this_thread::get_id() << "803  " << key << std::endl;
  // }
  if (leaf_page->GetSize() < leaf_page->GetMinSize()) {
    HandleUnderflow(guard_id, txn, nihaoa);
  }
  // {
  //   std::lock_guard<std::mutex> lock(latch_);
  //   std::cout << std::this_thread::get_id() << "810  " << key << std::endl;
  // }
  ReleasePages(depage);
  // auto deleted_page = txn->GetDeletedPageSet();
  // {
  //   std::lock_guard<std::mutex> lock(latch_);
  //   std::cout << std::this_thread::get_id() << "816  " << key << std::endl;
  // }
  // for (auto &pid : *deleted_page) {
  //   bpm_->DeletePage(pid);
  // }
  // {
  //   std::lock_guard<std::mutex> lock(latch_);
  //   std::cout << std::this_thread::get_id() << "823  " << key << std::endl;
  // }
  // deleted_page->clear();
  // {
  //   std::lock_guard<std::mutex> lock(latch_);
  //   std::cout << std::this_thread::get_id() << "finish Remove  " << key << std::endl;
  // }
  // std::cout << this->DrawBPlusTree() << std::endl;
}

/*****************************************************************************
 * INDEX ITERATOR
 *****************************************************************************/
/*
 * Input parameter is void, find the leftmost leaf page first, then construct
 * index iterator
 * @return : index iterator
 */
INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::Begin() -> INDEXITERATOR_TYPE {
  // std::cout << "Begin()" << std::endl;
  // std::cout << "root_latch Rlock" << std::endl;
  root_latch_.RLock();
  if (IsEmpty()) {
    // std::cout << "root_latch RUnlock" << std::endl;
    root_latch_.RUnlock();
    return End();
  }
  page_id_t next_page_id = header_page_id_;
  while (true) {
    BasicPageGuard guard = bpm_->FetchPageBasic(next_page_id);
    auto page = guard.AsMut<BPlusTreePage>();

    if (page->IsLeafPage()) {
      auto page1 = guard.AsMut<LeafPage>();
      page_id_t page_id = guard.PageId();
      // guard.Drop();
      // std::cout << "Begin dad" << std::endl;
      root_latch_.RUnlock();
      // std::cout << "root_latch RUnlock" << std::endl;
      return INDEXITERATOR_TYPE(page_id, 0, bpm_, page1);
    }
    auto internal_page = guard.As<InternalPage>();
    guard.is_dirty_ = false;
    next_page_id = internal_page->ValueAt(0);
  }
  root_latch_.RUnlock();
  // std::cout << "root_latch RUnlock" << std::endl;
  return INDEXITERATOR_TYPE();
}

/*
 * Input parameter is low key, find the leaf page that contains the input key
 * first, then construct index iterator
 * @return : index iterator
 */
INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::Begin(const KeyType &key) -> INDEXITERATOR_TYPE {
  // std::cout << "Begin()"
  //           << "  " << key << std::endl;
  root_latch_.RLock();

  // std::cout << "Begin()" << std::endl;
  // std::deque<Page *> depage;
  std::deque<Page *> depage;
  std::deque<page_id_t> nihaoa;
  BasicPageGuard page = GetLeafPage(key, depage, Operation::Read, nihaoa);

  // std::cout << "GetLeafPage" << std::endl;
  // std::cout << std::endl;
  auto leaf_page = page.AsMut<LeafPage>();
  page.is_dirty_ = false;
  int index = 0;
  while (index < leaf_page->GetSize() && comparator_(leaf_page->KeyAt(index), key) < 0) {
    index++;
  }
  // std::cout << page.PageId() << std::endl;
  // std::cout << std::endl;
  page_id_t page_id = page.PageId();
  // page.Drop();
  root_latch_.RUnlock();
  // std::cout << "leaf_page key   " << leaf_page->KeyAt(0) << std::endl;
  return INDEXITERATOR_TYPE(page_id, index, bpm_, leaf_page);
}

/*
 * Input parameter is void, construct an index iterator representing the end
 * of the key/value pair in the leaf node
 * @return : index iterator
 */
INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::End() -> INDEXITERATOR_TYPE {
  // std::cout << "end  index" << std::endl;
  // std::cout << std::endl;
  // std::cout << "End()" << std::endl;
  return INDEXITERATOR_TYPE(INVALID_PAGE_ID, 0, bpm_, nullptr);
}

/**
 * @return Page id of the root of this tree
 */
INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::GetRootPageId() -> page_id_t {
  std::lock_guard<std::mutex> lock(latch_);
  return header_page_id_;
}

INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::SetRootPageId() -> void {
  root_latch_.WLock();
  // std::cout << "SetRootPageId" << std::endl;
  BasicPageGuard guard = bpm_->FetchPageBasic(header_page_id_);
  auto root_page = guard.AsMut<BPlusTreeHeaderPage>();
  root_page->root_page_id_ = header_page_id_;
  root_latch_.WUnlock();
}
/*****************************************************************************
 * UTILITIES AND DEBUG
 *****************************************************************************/

/*
 * This method is used for test only
 * Read data from file and insert one by one
 */
INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::InsertFromFile(const std::string &file_name, Transaction *txn) {
  int64_t key;
  std::ifstream input(file_name);
  while (input) {
    input >> key;

    KeyType index_key;
    index_key.SetFromInteger(key);
    RID rid(key);
    Insert(index_key, rid, txn);
  }
}
/*
 * This method is used for test only
 * Read data from file and remove one by one
 */
INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::RemoveFromFile(const std::string &file_name, Transaction *txn) {
  int64_t key;
  std::ifstream input(file_name);
  while (input) {
    input >> key;
    KeyType index_key;
    index_key.SetFromInteger(key);
    Remove(index_key, txn);
  }
}

INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::Print(BufferPoolManager *bpm) {
  auto root_page_id = GetRootPageId();
  auto guard = bpm->FetchPageWrite(root_page_id);
  PrintTree(guard.PageId(), guard.template As<BPlusTreePage>());
}

INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::PrintTree(page_id_t page_id, const BPlusTreePage *page) {
  if (page->IsLeafPage()) {
    auto *leaf = reinterpret_cast<const LeafPage *>(page);
    std::cout << "Leaf Page: " << page_id << "\tNext: " << leaf->GetNextPageId() << std::endl;

    // Print the contents of the leaf page.
    std::cout << "Contents: ";
    for (int i = 0; i < leaf->GetSize(); i++) {
      std::cout << leaf->KeyAt(i);
      if ((i + 1) < leaf->GetSize()) {
        std::cout << ", ";
      }
    }
    std::cout << std::endl;
    std::cout << std::endl;

  } else {
    auto *internal = reinterpret_cast<const InternalPage *>(page);
    std::cout << "Internal Page: " << page_id << std::endl;

    // Print the contents of the internal page.
    std::cout << "Contents: ";
    for (int i = 0; i < internal->GetSize(); i++) {
      std::cout << internal->KeyAt(i) << ": " << internal->ValueAt(i);
      if ((i + 1) < internal->GetSize()) {
        std::cout << ", ";
      }
    }
    std::cout << std::endl;
    std::cout << std::endl;
    for (int i = 0; i < internal->GetSize(); i++) {
      auto guard = bpm_->FetchPageBasic(internal->ValueAt(i));
      PrintTree(guard.PageId(), guard.template As<BPlusTreePage>());
    }
  }
}

/**
 * This method is used for debug only, You don't need to modify
 */
INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::Draw(BufferPoolManager *bpm, const std::string &outf) {
  if (IsEmpty()) {
    LOG_WARN("Drawing an empty tree");
    return;
  }

  std::ofstream out(outf);
  out << "digraph G {" << std::endl;
  auto root_page_id = GetRootPageId();
  auto guard = bpm->FetchPageBasic(root_page_id);
  ToGraph(guard.PageId(), guard.template As<BPlusTreePage>(), out);
  out << "}" << std::endl;
  out.close();
}

/**
 * This method is used for debug only, You don't need to modify
 */
INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::ToGraph(page_id_t page_id, const BPlusTreePage *page, std::ofstream &out) {
  std::string leaf_prefix("LEAF_");
  std::string internal_prefix("INT_");
  if (page->IsLeafPage()) {
    auto *leaf = reinterpret_cast<const LeafPage *>(page);
    // Print node name
    out << leaf_prefix << page_id;
    // Print node properties
    out << "[shape=plain color=green ";
    // Print data of the node
    out << "label=<<TABLE BORDER=\"0\" CELLBORDER=\"1\" CELLSPACING=\"0\" CELLPADDING=\"4\">\n";
    // Print data
    out << "<TR><TD COLSPAN=\"" << leaf->GetSize() << "\">P=" << page_id << "</TD></TR>\n";
    out << "<TR><TD COLSPAN=\"" << leaf->GetSize() << "\">"
        << "max_size=" << leaf->GetMaxSize() << ",min_size=" << leaf->GetMinSize() << ",size=" << leaf->GetSize()
        << "</TD></TR>\n";
    out << "<TR>";
    for (int i = 0; i < leaf->GetSize(); i++) {
      out << "<TD>" << leaf->KeyAt(i) << "</TD>\n";
    }
    out << "</TR>";
    // Print table end
    out << "</TABLE>>];\n";
    // Print Leaf node link if there is a next page
    if (leaf->GetNextPageId() != INVALID_PAGE_ID) {
      out << leaf_prefix << page_id << " -> " << leaf_prefix << leaf->GetNextPageId() << ";\n";
      out << "{rank=same " << leaf_prefix << page_id << " " << leaf_prefix << leaf->GetNextPageId() << "};\n";
    }
  } else {
    auto *inner = reinterpret_cast<const InternalPage *>(page);
    // Print node name
    out << internal_prefix << page_id;
    // Print node properties
    out << "[shape=plain color=pink ";  // why not?
    // Print data of the node
    out << "label=<<TABLE BORDER=\"0\" CELLBORDER=\"1\" CELLSPACING=\"0\" CELLPADDING=\"4\">\n";
    // Print data
    out << "<TR><TD COLSPAN=\"" << inner->GetSize() << "\">P=" << page_id << "</TD></TR>\n";
    out << "<TR><TD COLSPAN=\"" << inner->GetSize() << "\">"
        << "max_size=" << inner->GetMaxSize() << ",min_size=" << inner->GetMinSize() << ",size=" << inner->GetSize()
        << "</TD></TR>\n";
    out << "<TR>";
    for (int i = 0; i < inner->GetSize(); i++) {
      out << "<TD PORT=\"p" << inner->ValueAt(i) << "\">";
      if (i > 0) {
        out << inner->KeyAt(i);
      } else {
        out << " ";
      }
      out << "</TD>\n";
    }
    out << "</TR>";
    // Print table end
    out << "</TABLE>>];\n";
    // Print leaves
    for (int i = 0; i < inner->GetSize(); i++) {
      auto child_guard = bpm_->FetchPageBasic(inner->ValueAt(i));
      auto child_page = child_guard.template As<BPlusTreePage>();
      ToGraph(child_guard.PageId(), child_page, out);
      if (i > 0) {
        auto sibling_guard = bpm_->FetchPageBasic(inner->ValueAt(i - 1));
        auto sibling_page = sibling_guard.template As<BPlusTreePage>();
        if (!sibling_page->IsLeafPage() && !child_page->IsLeafPage()) {
          out << "{rank=same " << internal_prefix << sibling_guard.PageId() << " " << internal_prefix
              << child_guard.PageId() << "};\n";
        }
      }
      out << internal_prefix << page_id << ":p" << child_guard.PageId() << " -> ";
      if (child_page->IsLeafPage()) {
        out << leaf_prefix << child_guard.PageId() << ";\n";
      } else {
        out << internal_prefix << child_guard.PageId() << ";\n";
      }
    }
  }
}

INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::DrawBPlusTree() -> std::string {
  if (IsEmpty()) {
    return "()";
  }

  PrintableBPlusTree p_root = ToPrintableBPlusTree(GetRootPageId());
  std::ostringstream out_buf;
  p_root.Print(out_buf);

  return out_buf.str();
}

INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::ToPrintableBPlusTree(page_id_t root_id) -> PrintableBPlusTree {
  auto root_page_guard = bpm_->FetchPageBasic(root_id);
  auto root_page = root_page_guard.template As<BPlusTreePage>();
  PrintableBPlusTree proot;

  if (root_page->IsLeafPage()) {
    auto leaf_page = root_page_guard.template As<LeafPage>();
    proot.keys_ = leaf_page->ToString();
    proot.size_ = proot.keys_.size() + 4;  // 4 more spaces for indent

    return proot;
  }

  // draw internal page
  auto internal_page = root_page_guard.template As<InternalPage>();
  proot.keys_ = internal_page->ToString();
  proot.size_ = 0;
  for (int i = 0; i < internal_page->GetSize(); i++) {
    page_id_t child_id = internal_page->ValueAt(i);
    PrintableBPlusTree child_node = ToPrintableBPlusTree(child_id);
    proot.size_ += child_node.size_;
    proot.children_.push_back(child_node);
  }

  return proot;
}

template class BPlusTree<GenericKey<4>, RID, GenericComparator<4>>;

template class BPlusTree<GenericKey<8>, RID, GenericComparator<8>>;

template class BPlusTree<GenericKey<16>, RID, GenericComparator<16>>;

template class BPlusTree<GenericKey<32>, RID, GenericComparator<32>>;

template class BPlusTree<GenericKey<64>, RID, GenericComparator<64>>;

}  // namespace bustub
