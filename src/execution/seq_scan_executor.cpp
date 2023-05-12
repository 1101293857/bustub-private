//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// seq_scan_executor.cpp
//
// Identification: src/execution/seq_scan_executor.cpp
//
// Copyright (c) 2015-2021, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "execution/executors/seq_scan_executor.h"

namespace bustub {

SeqScanExecutor::SeqScanExecutor(ExecutorContext *exec_ctx, const SeqScanPlanNode *plan)
    : AbstractExecutor(exec_ctx), plan_(plan) {}

void SeqScanExecutor::Init() {
  // throw NotImplementedException("SeqScanExecutor is not implemented");
  TableInfo *table_info = exec_ctx_->GetCatalog()->GetTable(plan_->GetTableOid());
  // 迭代器，这里我们不使用普通指针的原因是 TableIterator 没有默认构造方法
  iterator_ = std::make_unique<TableIterator>(table_info->table_->MakeIterator());
  try {
    if (exec_ctx_->GetTransaction()->GetIsolationLevel() != IsolationLevel::READ_UNCOMMITTED &&
        !exec_ctx_->GetLockManager()->LockTable(exec_ctx_->GetTransaction(), LockManager::LockMode::INTENTION_SHARED,
                                                table_info->oid_)) {
      throw ExecutionException("lock table share failed");
    }
  } catch (TransactionAbortException &e) {
    throw ExecutionException("seq scan TransactionAbort");
  }
}

auto SeqScanExecutor::Next(Tuple *tuple, RID *rid) -> bool {
  try {
    if (!exec_ctx_->GetTransaction()->GetSharedRowLockSet()->empty()) {
      if (exec_ctx_->GetTransaction()->GetIsolationLevel() == IsolationLevel::READ_COMMITTED &&
          !exec_ctx_->GetLockManager()->UnlockRow(
              exec_ctx_->GetTransaction(), exec_ctx_->GetCatalog()->GetTable(plan_->GetTableOid())->oid_, *rid)) {
        throw ExecutionException("unlock row share failed");
      }
    }
  } catch (TransactionAbortException &e) {
    throw ExecutionException("seq scan TransactionAbort");
  }
  while (!(iterator_->IsEnd())) {
    TupleMeta tuplemeta = iterator_->GetTuple().first;
    if (tuplemeta.is_deleted_) {
      continue;
    }
    try {
      if (exec_ctx_->GetTransaction()->GetIsolationLevel() != IsolationLevel::READ_UNCOMMITTED &&
          !exec_ctx_->GetLockManager()->LockRow(exec_ctx_->GetTransaction(), LockManager::LockMode::SHARED,
                                                exec_ctx_->GetCatalog()->GetTable(plan_->GetTableOid())->oid_,
                                                iterator_->GetRID())) {
        throw ExecutionException("lock row  intention share failed");
      }
    } catch (TransactionAbortException &e) {
      throw ExecutionException("seq scan TransactionAbort");
    }

    *tuple = iterator_->GetTuple().second;
    *rid = iterator_->GetRID();
    iterator_->operator++();
    return true;

  }

  return false;
}

}  // namespace bustub
