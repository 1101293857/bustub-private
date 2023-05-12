//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// hash_join_executor.cpp
//
// Identification: src/execution/hash_join_executor.cpp
//
// Copyright (c) 2015-2021, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "execution/executors/hash_join_executor.h"
#include "type/value_factory.h"

namespace bustub {

HashJoinExecutor::HashJoinExecutor(ExecutorContext *exec_ctx, const HashJoinPlanNode *plan,
                                   std::unique_ptr<AbstractExecutor> &&left_child,
                                   std::unique_ptr<AbstractExecutor> &&right_child)
    : AbstractExecutor(exec_ctx),
      plan_(plan),
      left_executor_(std::move(left_child)),
      right_executor_(std::move(right_child)),
      left_schema_(left_executor_->GetOutputSchema()),
      right_schema_(right_executor_->GetOutputSchema()) {
  if (!(plan->GetJoinType() == JoinType::LEFT || plan->GetJoinType() == JoinType::INNER)) {
    // Note for 2023 Spring: You ONLY need to implement left join and inner join.
    throw bustub::NotImplementedException(fmt::format("join type {} not supported", plan->GetJoinType()));
  }
  is_ineer_ = (plan_->GetJoinType() == JoinType::INNER);
}

void HashJoinExecutor::Init() {
  Tuple tuple;
  RID rid;
  left_executor_->Init();
  right_executor_->Init();
  if (plan_->right_key_expressions_.size() == 1) {
    while (right_executor_->Next(&tuple, &rid)) {
      // right_tuples_.push_back(tuple);
      // std::cout << tuple.ToString(&right_schema_) << std::endl;

      Value v = plan_->right_key_expressions_[0]->Evaluate(&tuple, right_schema_);
      JoinKey key;
      key.value_ = v;
      if (right_table_map_1_.count(key) == 0) {
        std::vector<int> tuples;
        right_table_map_1_[key] = tuples;
      }
      right_table_map_1_[key].push_back(right_tuples_.size());
      right_tuples_.push_back(tuple);
    }
  } else if (plan_->right_key_expressions_.size() == 2) {
    while (right_executor_->Next(&tuple, &rid)) {
      // right_tuples_.push_back(tuple);
      // std::cout << tuple.ToString(&right_schema_) << std::endl;

      Value v = plan_->right_key_expressions_[0]->Evaluate(&tuple, right_schema_);
      JoinKey key;
      key.value_ = v;
      if (right_table_map_1_.count(key) == 0) {
        std::vector<int> tuples;
        right_table_map_1_[key] = tuples;
      }
      right_table_map_1_[key].push_back(right_tuples_.size());

      Value v1 = plan_->right_key_expressions_[1]->Evaluate(&tuple, right_schema_);
      JoinKey key1;
      key1.value_ = v1;
      if (right_table_map_2_.count(key1) == 0) {
        std::vector<int> tuples;
        right_table_map_2_[key1] = tuples;
      }
      right_table_map_2_[key1].push_back(right_tuples_.size());
      right_tuples_.push_back(tuple);
    }
  }
}

auto HashJoinExecutor::Next(Tuple *tuple, RID *rid) -> bool {
  std::vector<Column> columns(left_schema_.GetColumns());
  for (const auto &column : right_schema_.GetColumns()) {
    columns.push_back(column);
  }
  Schema schema(columns);
  if (is_ineer_) {
    return InnerJoin(schema, tuple);
  }
  return LeftJoin(schema, tuple);
}

auto HashJoinExecutor::InnerJoin(const Schema &schema, Tuple *tuple) -> bool {
  if (is_success_) {
    return false;
  }
  if (index_ != 0) {
    std::vector<Value> value;
    for (uint32_t i = 0; i < left_schema_.GetColumnCount(); i++) {
      value.push_back(left_tuple_.GetValue(&left_schema_, i));
    }
    for (uint32_t i = 0; i < right_schema_.GetColumnCount(); i++) {
      value.push_back(right_tuples_[result_right_tuples_[index_]].GetValue(&right_schema_, i));
    }
    *tuple = {value, &schema};
    index_++;
    if (result_right_tuples_.size() <= index_) {
      index_ = 0;
    }
    return true;
  }

  if (index_ == 0) {
    while (left_executor_->Next(&left_tuple_, &left_rid_)) {
      right_executor_->Init();
      std::vector<std::vector<int>> tuples;
      // for(size_t i=0;i<plan_->left_key_expressions_.size();i++){
      //   Value v = plan_->left_key_expressions_[i]->Evaluate(&left_tuple_,left_schema_);
      //   JoinKey key;
      //   key.value = v;
      //   if(right_table_map_.count(key)==1)
      //     tuples.push_back(right_table_map_[key]);
      // }
      Value v = plan_->left_key_expressions_[0]->Evaluate(&left_tuple_, left_schema_);
      JoinKey key;
      key.value_ = v;
      if (right_table_map_1_.count(key) == 1) {
        tuples.push_back(right_table_map_1_[key]);
      }
      if (plan_->left_key_expressions_.size() == 2) {
        Value v1 = plan_->left_key_expressions_[1]->Evaluate(&left_tuple_, left_schema_);
        JoinKey key1;
        key1.value_ = v1;
        if (right_table_map_2_.count(key1) == 1) {
          tuples.push_back(right_table_map_2_[key1]);
        }
      }
      if (tuples.empty()) {
        continue;
      }
      if (plan_->left_key_expressions_.size() == 1 && tuples.size() == 1) {  // 一个等式
        result_right_tuples_ = tuples[0];
        std::vector<Value> value;
        for (uint32_t i = 0; i < left_schema_.GetColumnCount(); i++) {
          value.push_back(left_tuple_.GetValue(&left_schema_, i));
        }
        for (uint32_t i = 0; i < right_schema_.GetColumnCount(); i++) {
          value.push_back(right_tuples_[result_right_tuples_[0]].GetValue(&right_schema_, i));
        }
        *tuple = {value, &schema};
        index_++;
        if (result_right_tuples_.size() <= index_) {
          index_ = 0;
        }
        return true;
      }
      if (plan_->left_key_expressions_.size() == 2 && tuples.size() == 2) {  // 两个等式
        result_right_tuples_.clear();
        for (size_t kkx = 0; kkx < tuples[0].size(); kkx++) {
          for (size_t kky = 0; kky < tuples[1].size(); kky++) {
            if (tuples[0][kkx] == tuples[1][kky]) {
              result_right_tuples_.push_back(tuples[0][kkx]);
              break;
            }
          }
        }
        if (result_right_tuples_.empty()) {
          continue;
        }
        std::vector<Value> value;
        for (uint32_t i = 0; i < left_schema_.GetColumnCount(); i++) {
          value.push_back(left_tuple_.GetValue(&left_schema_, i));
        }
        for (uint32_t i = 0; i < right_schema_.GetColumnCount(); i++) {
          value.push_back(right_tuples_[result_right_tuples_[0]].GetValue(&right_schema_, i));
        }
        *tuple = {value, &schema};
        index_++;
        if (result_right_tuples_.size() <= index_) {
          index_ = 0;
        }
        return true;
      }
    }
    is_success_ = true;
    index_ = right_tuples_.size() + 1;
  }
  return false;
}

auto HashJoinExecutor::LeftJoin(const Schema &schema, Tuple *tuple) -> bool {
  if (is_success_) {
    return false;
  }
  if (index_ != 0) {
    std::vector<Value> value;
    for (uint32_t i = 0; i < left_schema_.GetColumnCount(); i++) {
      value.push_back(left_tuple_.GetValue(&left_schema_, i));
    }
    for (uint32_t i = 0; i < right_schema_.GetColumnCount(); i++) {
      value.push_back(right_tuples_[result_right_tuples_[index_]].GetValue(&right_schema_, i));
    }
    *tuple = {value, &schema};
    index_++;
    if (result_right_tuples_.size() <= index_) {
      index_ = 0;
    }
    return true;
  }

  if (index_ == 0) {
    while (left_executor_->Next(&left_tuple_, &left_rid_)) {
      right_executor_->Init();
      std::vector<std::vector<int>> tuples;
      Value v = plan_->left_key_expressions_[0]->Evaluate(&left_tuple_, left_schema_);
      JoinKey key;
      key.value_ = v;
      if (right_table_map_1_.count(key) == 1) {
        tuples.push_back(right_table_map_1_[key]);
      }
      if (plan_->left_key_expressions_.size() == 2) {
        Value v1 = plan_->left_key_expressions_[1]->Evaluate(&left_tuple_, left_schema_);
        JoinKey key1;
        key1.value_ = v1;
        if (right_table_map_2_.count(key1) == 1) {
          tuples.push_back(right_table_map_2_[key1]);
        }
      }
      if (tuples.empty()) {
        std::vector<Value> value;
        for (uint32_t i = 0; i < left_schema_.GetColumnCount(); i++) {
          value.push_back(left_tuple_.GetValue(&left_schema_, i));
        }
        for (uint32_t i = 0; i < right_schema_.GetColumnCount(); i++) {
          value.push_back(ValueFactory::GetNullValueByType(right_schema_.GetColumn(i).GetType()));
        }
        *tuple = {value, &schema};
        index_ = 0;
        return true;
      }
      if (plan_->left_key_expressions_.size() == 1 && tuples.size() == 1) {  // 一个等式
        result_right_tuples_ = tuples[0];
        std::vector<Value> value;
        for (uint32_t i = 0; i < left_schema_.GetColumnCount(); i++) {
          value.push_back(left_tuple_.GetValue(&left_schema_, i));
        }
        for (uint32_t i = 0; i < right_schema_.GetColumnCount(); i++) {
          value.push_back(right_tuples_[result_right_tuples_[0]].GetValue(&right_schema_, i));
        }
        *tuple = {value, &schema};
        index_++;
        if (result_right_tuples_.size() <= index_) {
          index_ = 0;
        }
        return true;
      }
      if (plan_->left_key_expressions_.size() == 2 && tuples.size() == 2) {  // 两个等式
        result_right_tuples_.clear();
        for (size_t kkx = 0; kkx < tuples[0].size(); kkx++) {
          for (size_t kky = 0; kky < tuples[1].size(); kky++) {
            if (tuples[0][kkx] == tuples[1][kky]) {
              result_right_tuples_.push_back(tuples[0][kkx]);
              break;
            }
          }
        }
        if (result_right_tuples_.empty()) {
          std::vector<Value> value;
          for (uint32_t i = 0; i < left_schema_.GetColumnCount(); i++) {
            value.push_back(left_tuple_.GetValue(&left_schema_, i));
          }
          for (uint32_t i = 0; i < right_schema_.GetColumnCount(); i++) {
            value.push_back(ValueFactory::GetNullValueByType(right_schema_.GetColumn(i).GetType()));
          }
          *tuple = {value, &schema};
          index_ = 0;
          return true;
        }
        std::vector<Value> value;
        for (uint32_t i = 0; i < left_schema_.GetColumnCount(); i++) {
          value.push_back(left_tuple_.GetValue(&left_schema_, i));
        }
        for (uint32_t i = 0; i < right_schema_.GetColumnCount(); i++) {
          value.push_back(right_tuples_[result_right_tuples_[0]].GetValue(&right_schema_, i));
        }
        *tuple = {value, &schema};
        index_++;
        if (result_right_tuples_.size() <= index_) {
          index_ = 0;
        }
        return true;
      }
    }
    is_success_ = true;
    index_ = right_tuples_.size() + 1;
  }
  return false;
}

}  // namespace bustub
