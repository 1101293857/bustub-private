//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// update_executor.cpp
//
// Identification: src/execution/update_executor.cpp
//
// Copyright (c) 2015-2021, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//
#include <memory>

#include "execution/executors/update_executor.h"

namespace bustub {

UpdateExecutor::UpdateExecutor(ExecutorContext *exec_ctx, const UpdatePlanNode *plan,
                               std::unique_ptr<AbstractExecutor> &&child_executor)
    : AbstractExecutor(exec_ctx), plan_(plan), child_executor_(std::move(child_executor)) {
  // As of Fall 2022, you DON'T need to implement update executor to have perfect score in project 3 / project 4.
}

void UpdateExecutor::Init() {
  child_executor_->Init();
  Catalog *cata = exec_ctx_->GetCatalog();
  table_info_ = cata->GetTable(plan_->table_oid_);
  is_success_ = false;
}

auto UpdateExecutor::Next(Tuple *tuple, RID *rid) -> bool {
  if (is_success_) {
    return false;
  }
  int count = 0;
  while (child_executor_->Next(tuple, rid)) {
    // 向 table_heap_ 中插入，然后需要向 index 中插入
    //  indexs 中插入的 tuple 和原本的 tuple 不同，需要调用 KeyFromTuple 构造
    TupleMeta tuplemeta;
    tuplemeta.is_deleted_ = true;
    table_info_->table_->UpdateTupleMeta(tuplemeta, *rid);
    auto indexs = exec_ctx_->GetCatalog()->GetTableIndexes(table_info_->name_);
    for (auto index : indexs) {
      auto key = tuple->KeyFromTuple(table_info_->schema_, index->key_schema_, index->index_->GetKeyAttrs());
      index->index_->DeleteEntry(key, *rid, exec_ctx_->GetTransaction());
    }
    tuplemeta.is_deleted_ = false;

    std::vector<Value> value1;
    for (auto &a : plan_->target_expressions_) {
      value1.emplace_back(a->Evaluate(tuple, child_executor_->GetOutputSchema()));
    }

    Tuple tuple1 = Tuple(value1, &child_executor_->GetOutputSchema());
    // // std::cout << plan_->target_expressions_ << std::endl;
    RID mm = table_info_->table_->InsertTuple(tuplemeta, tuple1).value();
    indexs = exec_ctx_->GetCatalog()->GetTableIndexes(table_info_->name_);
    for (auto index : indexs) {
      auto key = tuple1.KeyFromTuple(table_info_->schema_, index->key_schema_, index->index_->GetKeyAttrs());
      index->index_->InsertEntry(key, mm, exec_ctx_->GetTransaction());
    }
    // for (auto index : indexs) {
    //   auto key = tuple->KeyFromTuple(table_info_->schema_, index->key_schema_, index->index_->GetKeyAttrs());
    //   index->index_->InsertEntry(key, *rid, exec_ctx_->GetTransaction());
    // }
    count++;
  }
  // 返回值 tuple 需要 std::vector<Value> 和 Schema 来构造
  // Value 是数据，需要 typeID 和数据来构造，这里分别是 INTEGER （整数）和删除的行数 count
  // Schema 是表的结构，通过 std::vector<Column> 构造
  // Column 是每一行的属性，通过属性名（随便命名）和 typeID 来构造
  std::vector<Value> value;
  value.emplace_back(INTEGER, count);
  std::vector<Column> column;
  column.emplace_back("UpdateCount", INTEGER);
  Schema schema(column);
  // Schema schema(plan_->OutputSchema());
  *tuple = Tuple(value, &schema);
  is_success_ = true;
  return true;
}

}  // namespace bustub
