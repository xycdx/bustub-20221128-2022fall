//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// delete_executor.cpp
//
// Identification: src/execution/delete_executor.cpp
//
// Copyright (c) 2015-2021, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include <memory>

#include "execution/executors/delete_executor.h"

namespace bustub {

DeleteExecutor::DeleteExecutor(ExecutorContext *exec_ctx, const DeletePlanNode *plan,
                               std::unique_ptr<AbstractExecutor> &&child_executor)
    : AbstractExecutor(exec_ctx), plan_(plan), child_executor_(std::move((child_executor))) {}

void DeleteExecutor::Init() { child_executor_->Init(); }

auto DeleteExecutor::Next([[maybe_unused]] Tuple *tuple, RID *rid) -> bool {
  if (has_deleted_) {
    return false;
  }
  has_deleted_ = true;
  auto delete_table = exec_ctx_->GetCatalog()->GetTable(plan_->TableOid());
  auto indexes = exec_ctx_->GetCatalog()->GetTableIndexes(delete_table->name_);

  int cnt = 0;
  while (child_executor_->Next(tuple, rid)) {
    delete_table->table_->MarkDelete(*rid, exec_ctx_->GetTransaction());
    for (auto index : indexes) {
      Tuple key_tuple = tuple->KeyFromTuple(child_executor_->GetOutputSchema(), index->key_schema_,
                                            index->index_->GetMetadata()->GetKeyAttrs());
      index->index_->DeleteEntry(key_tuple, *rid, exec_ctx_->GetTransaction());
    }
    cnt++;
  }
  std::vector<Value> result{Value(INTEGER, cnt)};
  *tuple = Tuple(result, &plan_->OutputSchema());
  return true;
}

}  // namespace bustub
