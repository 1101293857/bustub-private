//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// insert_executor.cpp
//
// Identification: src/execution/insert_executor.cpp
//
// Copyright (c) 2015-2021, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include <memory>

#include "execution/executors/insert_executor.h"

namespace bustub {

InsertExecutor::InsertExecutor(ExecutorContext *exec_ctx, const InsertPlanNode *plan,
                               std::unique_ptr<AbstractExecutor> &&child_executor)
    : AbstractExecutor(exec_ctx), plan_(plan), child_executor_(std::move(child_executor)) {
  is_success_ = false;
}

void InsertExecutor::Init() {
  child_executor_->Init();
  Catalog *cata = exec_ctx_->GetCatalog();
  tableinfo_ = cata->GetTable(plan_->table_oid_);
  try {
    if (!exec_ctx_->GetLockManager()->LockTable(exec_ctx_->GetTransaction(), LockManager::LockMode::INTENTION_EXCLUSIVE,
                                                tableinfo_->oid_)) {
      throw ExecutionException("lock table intention exclusive failed");
    }
  } catch (TransactionAbortException &e) {
    throw ExecutionException("insert TransactionAbort");
  }
}

auto InsertExecutor::Next(Tuple *tuple, RID *rid) -> bool {
  if (is_success_) {
    return false;
  }
  int count = 0;
  while (child_executor_->Next(tuple, rid)) {
    // 向 table_heap_ 中插入，然后需要向 index 中插入
    //  indexs 中插入的 tuple 和原本的 tuple 不同，需要调用 KeyFromTuple 构造
    // bool status = child_executor_->Next(tuple, rid);
    // std::cout << status << std::endl;
    // if(!status){
    //     break;
    // }
    TupleMeta tuplemeta;
    tuplemeta.is_deleted_ = false;
    RID rid1 = (tableinfo_->table_->InsertTuple(tuplemeta, *tuple)).value();



    // auto twr = TableWriteRecord{tableinfo_->oid_, rid1, tableinfo_->table_.get()};
    // twr.wtype_ = WType::INSERT;
    // exec_ctx_->GetTransaction()->GetWriteSet()->push_back(twr);



    try {
      if (!exec_ctx_->GetLockManager()->LockRow(exec_ctx_->GetTransaction(), LockManager::LockMode::EXCLUSIVE,
                                                tableinfo_->oid_, rid1)) {
        throw ExecutionException("lock row  exclusive failed");
      }
    } catch (TransactionAbortException &e) {
      throw ExecutionException("insert TransactionAbort");
    }
    auto indexs = exec_ctx_->GetCatalog()->GetTableIndexes(tableinfo_->name_);
    for (auto index : indexs) {
      auto key = tuple->KeyFromTuple(tableinfo_->schema_, index->key_schema_, index->index_->GetKeyAttrs());
      index->index_->InsertEntry(key, rid1, exec_ctx_->GetTransaction());


      auto iwr = IndexWriteRecord{rid1,          tableinfo_->oid_, WType::INSERT,
                                    key, index->index_oid_,     exec_ctx_->GetCatalog()};
      exec_ctx_->GetTransaction()->GetIndexWriteSet()->push_back(iwr);
    }
    count++;
  }
  // 返回值 tuple 需要 std::vector<Value> 和 Schema 来构造
  // Value 是数据，需要 typeID 和数据来构造，这里分别是 INTEGER （整数）和删除的行数 count
  // Schema 是表的结构，通过 std::vector<Column> 构造
  // Column 是每一行的属性，通过属性名（随便命名）和 typeID 来构造
  std::vector<Value> value;
  value.emplace_back(INTEGER, count);
  std::vector<Column> column;
  column.emplace_back("InsertCount", INTEGER);
  Schema schema(column);
  // Schema schema(plan_->OutputSchema());
  *tuple = Tuple(value, &schema);
  is_success_ = true;
  return true;
}

}  // namespace bustub
