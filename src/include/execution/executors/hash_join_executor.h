//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// hash_join_executor.h
//
// Identification: src/include/execution/executors/hash_join_executor.h
//
// Copyright (c) 2015-2021, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#pragma once

#include <memory>
#include <unordered_map>
#include <utility>
#include <vector>
#include "common/util/hash_util.h"
#include "execution/executor_context.h"
#include "execution/executors/abstract_executor.h"
#include "execution/plans/hash_join_plan.h"
#include "storage/table/tuple.h"
namespace bustub {
struct JoinKey {
  Value value_;
  auto operator==(const JoinKey &other) const -> bool { return value_.CompareEquals(other.value_) == CmpBool::CmpTrue; }
};
}  // namespace bustub
namespace std {
template <>
struct hash<bustub::JoinKey> {
  auto operator()(const bustub::JoinKey &agg_key) const -> std::size_t {
    size_t curr_hash = 0;
    if (!agg_key.value_.IsNull()) {
      curr_hash = bustub::HashUtil::CombineHashes(curr_hash, bustub::HashUtil::HashValue(&agg_key.value_));
    }
    return curr_hash;
  }
};
}  // namespace std

namespace bustub {

/**
 * HashJoinExecutor executes a nested-loop JOIN on two tables.
 */
class HashJoinExecutor : public AbstractExecutor {
 public:
  /**
   * Construct a new HashJoinExecutor instance.
   * @param exec_ctx The executor context
   * @param plan The HashJoin join plan to be executed
   * @param left_child The child executor that produces tuples for the left side of join
   * @param right_child The child executor that produces tuples for the right side of join
   */
  HashJoinExecutor(ExecutorContext *exec_ctx, const HashJoinPlanNode *plan,
                   std::unique_ptr<AbstractExecutor> &&left_child, std::unique_ptr<AbstractExecutor> &&right_child);

  /** Initialize the join */
  void Init() override;

  /**
   * Yield the next tuple from the join.
   * @param[out] tuple The next tuple produced by the join.
   * @param[out] rid The next tuple RID, not used by hash join.
   * @return `true` if a tuple was produced, `false` if there are no more tuples.
   */
  auto Next(Tuple *tuple, RID *rid) -> bool override;

  /** @return The output schema for the join */
  auto GetOutputSchema() const -> const Schema & override { return plan_->OutputSchema(); };

 private:
  /** The NestedLoopJoin plan node to be executed. */
  auto InnerJoin(const Schema &schema, Tuple *tuple) -> bool;
  auto LeftJoin(const Schema &schema, Tuple *tuple) -> bool;
  const HashJoinPlanNode *plan_;
  bool is_ineer_{false};
  std::unique_ptr<AbstractExecutor> left_executor_;
  std::unique_ptr<AbstractExecutor> right_executor_;
  std::vector<Tuple> right_tuples_;
  std::vector<int> result_right_tuples_;
  uint64_t index_{0};
  Tuple left_tuple_;
  RID left_rid_;
  Schema left_schema_;
  Schema right_schema_;
  bool is_success_{false};
  std::unordered_map<JoinKey, std::vector<int>> right_table_map_1_;
  std::unordered_map<JoinKey, std::vector<int>> right_table_map_2_;
  // bool is_match_{true};
};

}  // namespace bustub
