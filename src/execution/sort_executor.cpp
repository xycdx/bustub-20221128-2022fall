#include "execution/executors/sort_executor.h"

namespace bustub {

SortExecutor::SortExecutor(ExecutorContext *exec_ctx, const SortPlanNode *plan,
                           std::unique_ptr<AbstractExecutor> &&child_executor)
    : AbstractExecutor(exec_ctx), plan_(plan), child_executor_(std::move(child_executor)) {}

void SortExecutor::Init() {
  child_executor_->Init();
  Tuple child_tuple;
  RID child_rid;
  while (child_executor_->Next(&child_tuple, &child_rid)) {
    tuples_.push_back(child_tuple);
  }

  auto order_bys = plan_->GetOrderBy();
  auto child_schema = plan_->OutputSchema();

  auto cmp = [&](const Tuple &left_tuple, const Tuple &right_tuple) -> bool {
    bool result = false;
    for (auto order_by : order_bys) {
      auto sort_type = order_by.first;
      auto left_predict = order_by.second;
      auto left_value = left_predict->Evaluate(&left_tuple, child_schema);
      auto right_value = left_predict->Evaluate(&right_tuple, child_schema);

      CmpBool cmp_result = CmpBool::CmpNull;
      if (left_value.CompareEquals(right_value) == CmpBool::CmpTrue) {
        continue;
      }
      if (sort_type == OrderByType::DESC) {
        // std::cout << "DESC:" << left_value.ToString() << " " << right_value.ToString() << std::endl;
        cmp_result = left_value.CompareGreaterThan(right_value);
      } else {
        // std::cout << "ASC:" << left_value.ToString() << " " << right_value.ToString() << std::endl;
        cmp_result = left_value.CompareLessThan(right_value);
      }
      if (cmp_result == CmpBool::CmpTrue) {
        result = true;
        break;
      } else if (cmp_result == CmpBool::CmpFalse) {
        result = false;
        break;
      }
    }
    return result;
  };

  std::sort(tuples_.begin(), tuples_.end(), cmp);
  std::reverse(tuples_.begin(), tuples_.end());
}

auto SortExecutor::Next(Tuple *tuple, RID *rid) -> bool {
  if (tuples_.empty()) {
    return false;
  }
  *tuple = tuples_.back();
  tuples_.pop_back();
  *rid = tuple->GetRid();
  return true;
}

}  // namespace bustub
