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
      aht_(plan->GetAggregates(), plan->GetAggregateTypes()),
      aht_iterator_(aht_.Begin()) {}

void AggregationExecutor::Init() {
  child_->Init();
  Tuple tuple;
  RID rid;
  while (child_->Next(&tuple, &rid)) {
    auto aggre_key = this->MakeAggregateKey(&tuple);
    auto aggre_value = this->MakeAggregateValue(&tuple);
    aht_.InsertCombine(aggre_key, aggre_value);
  }
  aht_iterator_ = aht_.Begin();
}

auto AggregationExecutor::Next(Tuple *tuple, RID *rid) -> bool {
  if (aht_iterator_ == aht_.End()) {
    if (!plan_->GetGroupBys().empty()) {
      return false;
    }
    AggregateValue result_value;
    if (aht_.CheckCountStart(&result_value)) {
      *tuple = Tuple(result_value.aggregates_, &plan_->OutputSchema());
      *rid = tuple->GetRid();
      return true;
    }
    return false;
  }
  auto aggre_key = aht_iterator_.Key();
  auto aggre_value = aht_iterator_.Val();

  std::vector<Value> values;
  values.reserve(aggre_key.group_bys_.size() + aggre_value.aggregates_.size());

  for (auto group_by_key : aggre_key.group_bys_) {
    values.push_back(group_by_key);
  }
  for (auto group_by_value : aggre_value.aggregates_) {
    values.push_back(group_by_value);
  }
  *tuple = Tuple(values, &plan_->OutputSchema());
  *rid = tuple->GetRid();
  ++aht_iterator_;
  return true;
}

auto AggregationExecutor::GetChildExecutor() const -> const AbstractExecutor * { return child_.get(); }

}  // namespace bustub
