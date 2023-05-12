//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// index_scan_executor.cpp
//
// Identification: src/execution/index_scan_executor.cpp
//
// Copyright (c) 2015-19, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//
#include "execution/executors/index_scan_executor.h"

namespace bustub {
IndexScanExecutor::IndexScanExecutor(ExecutorContext *exec_ctx, const IndexScanPlanNode *plan)
    : AbstractExecutor(exec_ctx), plan_(plan) {}

void IndexScanExecutor::Init() {
  Catalog *catalog = exec_ctx_->GetCatalog();
  index_info_ = catalog->GetIndex(plan_->index_oid_);
  table_info_ = catalog->GetTable(index_info_->table_name_);
  // std::cout << typeid(dynamic_cast<BPlusTreeIndexForTwoIntegerColumn *>(index_info_->index_.get())).name <<
  // std::endl;
  tree_ = dynamic_cast<BPlusTreeIndexForTwoIntegerColumn *>(index_info_->index_.get());
  iter_ = tree_->GetBeginIterator();
  end_iter_ = tree_->GetEndIterator();
}

auto IndexScanExecutor::Next(Tuple *tuple, RID *rid) -> bool {
  while (iter_ != end_iter_) {
    RID rid1 = (*iter_).second;
    auto nihao = table_info_->table_->GetTuple(rid1);
    if (!nihao.first.is_deleted_) {
      *tuple = nihao.second;
      iter_.operator++();
      return true;
    }

    iter_.operator++();
  }
  return false;
}

}  // namespace bustub
