//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// nested_loop_join_executor.cpp
//
// Identification: src/execution/nested_loop_join_executor.cpp
//
// Copyright (c) 2015-2021, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "execution/executors/nested_loop_join_executor.h"
#include "binder/table_ref/bound_join_ref.h"
#include "common/exception.h"

namespace bustub {

NestedLoopJoinExecutor::NestedLoopJoinExecutor(ExecutorContext *exec_ctx, const NestedLoopJoinPlanNode *plan,
                                               std::unique_ptr<AbstractExecutor> &&left_executor,
                                               std::unique_ptr<AbstractExecutor> &&right_executor)
    : AbstractExecutor(exec_ctx),
      plan_(plan),
      left_exector_(std::move(left_executor)),
      right_executor_(std::move(right_executor)) {}

void NestedLoopJoinExecutor::Init() {
  left_exector_->Init();
  right_executor_->Init();
  Tuple tuple;
  RID rid;
  std::vector<Tuple> left_tuples, right_tuples;
  auto left_schema = left_exector_->GetOutputSchema();
  auto right_schema = right_executor_->GetOutputSchema();
  while (left_exector_->Next(&tuple, &rid)) {
    left_tuples.push_back(tuple);
  }
  while (right_executor_->Next(&tuple, &rid)) {
    right_tuples.push_back(tuple);
  }
  for (auto &left_tuple : left_tuples) {
    bool is_left_join_valid = false;
    for (auto &right_tuple : right_tuples) {
      auto join_result = plan_->Predicate().EvaluateJoin(&left_tuple, left_schema, &right_tuple, right_schema);
      if (!join_result.IsNull() && join_result.GetAs<bool>()) {
        is_left_join_valid = true;

        std::vector<Value> values;
        for (size_t i = 0; i < left_schema.GetColumns().size(); i++) {
          values.push_back(left_tuple.GetValue(&left_schema, i));
        }
        for (size_t i = 0; i < right_schema.GetColumns().size(); i++) {
          values.push_back(right_tuple.GetValue(&right_schema, i));
        }
        result_tuples_.push_back(Tuple(values, &plan_->OutputSchema()));
      }
    }
    if (!is_left_join_valid && plan_->GetJoinType() == JoinType::LEFT) {
      std::vector<Value> values;
      for (size_t i = 0; i < left_schema.GetColumns().size(); i++) {
        values.push_back(left_tuple.GetValue(&left_schema, i));
      }
      for (size_t i = 0; i < right_schema.GetColumns().size(); i++) {
        values.push_back(ValueFactory::GetNullValueByType(right_schema.GetColumn(i).GetType()));
      }
      result_tuples_.push_back(Tuple(values, &plan_->OutputSchema()));
    }
  }
}

auto NestedLoopJoinExecutor::Next(Tuple *tuple, RID *rid) -> bool {
  if (result_tuples_.empty()) {
    return false;
  }
  *tuple = result_tuples_.front();
  result_tuples_.pop_front();
  *rid = tuple->GetRid();
  return true;
}

}  // namespace bustub
