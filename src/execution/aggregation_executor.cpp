//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// aggregation_executor.cpp
//
// Identification: src/execution/aggregation_executor.cpp
//
// Copyright (c) 2015-2021, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//
#include <memory>
#include <vector>

#include "execution/executors/aggregation_executor.h"

namespace bustub {

AggregationExecutor::AggregationExecutor(ExecutorContext *exec_ctx, const AggregationPlanNode *plan,
                                         std::unique_ptr<AbstractExecutor> &&child)
    : AbstractExecutor(exec_ctx),
      plan_(plan),
      child_(std::move(child)),
      aht_(plan->aggregates_, plan->agg_types_),
      aht_iterator_(aht_.Begin()) {}

void AggregationExecutor::Init() {
  child_->Init();
  Tuple tuple;
  RID rid;
  while (child_->Next(&tuple, &rid)) {
    auto aggregate_key = MakeAggregateKey(&tuple);
    auto aggregate_value = MakeAggregateValue(&tuple);
    aht_.InsertCombine(aggregate_key, aggregate_value);
  }
  aht_iterator_ = aht_.Begin();
  is_success_ = false;
}

auto AggregationExecutor::Next(Tuple *tuple, RID *rid) -> bool {
  Schema schema(plan_->OutputSchema());
  if (aht_iterator_ != aht_.End()) {
    std::vector<Value> value(aht_iterator_.Key().group_bys_);
    for (const auto &aggregate : aht_iterator_.Val().aggregates_) {
      value.push_back(aggregate);
    }
    *tuple = {value, &schema};
    ++aht_iterator_;
    is_success_ = true;
    return true;
  }
  if (is_success_) {
    return false;
  }
  // 应对某些没有 group_bys_ 的情况
  if (plan_->group_bys_.empty()) {
    std::vector<Value> value;
    for (auto aggregate : plan_->agg_types_) {
      switch (aggregate) {
        case AggregationType::CountStarAggregate:
          value.push_back(ValueFactory::GetIntegerValue(0));
          break;
        case AggregationType::CountAggregate:
        case AggregationType::SumAggregate:
        case AggregationType::MinAggregate:
        case AggregationType::MaxAggregate:
          value.push_back(ValueFactory::GetNullValueByType(TypeId::INTEGER));
          break;
      }
    }
    *tuple = {value, &schema};
    is_success_ = true;
    return true;
  }

  return false;
}

auto AggregationExecutor::GetChildExecutor() const -> const AbstractExecutor * { return child_.get(); }

}  // namespace bustub
