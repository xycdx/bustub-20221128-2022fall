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
    : AbstractExecutor(exec_ctx), plan_(plan), child_executor_(std::move((child_executor))) {}

void InsertExecutor::Init() { child_executor_->Init(); }

auto InsertExecutor::Next([[maybe_unused]] Tuple *tuple, RID *rid) -> bool {
  if (has_inserted_) {
    return false;
  }
  has_inserted_ = true;
  auto insert_table = exec_ctx_->GetCatalog()->GetTable(plan_->TableOid());
  auto indexes = exec_ctx_->GetCatalog()->GetTableIndexes(insert_table->name_);

  int cnt = 0;
  while (child_executor_->Next(tuple, rid)) {
    insert_table->table_->InsertTuple(*tuple, rid, exec_ctx_->GetTransaction());
    for (auto index : indexes) {
      Tuple key_tuple = tuple->KeyFromTuple(child_executor_->GetOutputSchema(), index->key_schema_,
                                            index->index_->GetMetadata()->GetKeyAttrs());
      index->index_->InsertEntry(key_tuple, *rid, exec_ctx_->GetTransaction());
    }
    cnt++;
  }
  std::vector<Value> result{Value(INTEGER, cnt)};
  *tuple = Tuple(result, &plan_->OutputSchema());
  return true;
}

}  // namespace bustub
