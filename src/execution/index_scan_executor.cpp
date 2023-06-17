//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// index_scan_executor.cpp
//
// Identification: src/execution/index_scan_executor.cpp
//
// Copyright (c) 2015-19, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//
#include "execution/executors/index_scan_executor.h"

namespace bustub {
IndexScanExecutor::IndexScanExecutor(ExecutorContext *exec_ctx, const IndexScanPlanNode *plan)
    : AbstractExecutor(exec_ctx), plan_(plan), begin_(0, exec_ctx->GetBufferPoolManager(), 0) {}

void IndexScanExecutor::Init() {
  auto index = exec_ctx_->GetCatalog()->GetIndex(plan_->GetIndexOid());
  tree_ = dynamic_cast<BPlusTreeIndexForOneIntegerColumn *>(index->index_.get());
  auto table_info = exec_ctx_->GetCatalog()->GetTable(index->table_name_);
  table_ = table_info->table_.get();
  begin_ = tree_->GetBeginIterator();
  
  
}

auto IndexScanExecutor::Next(Tuple *tuple, RID *rid) -> bool {
  if (begin_.IsEnd()) {
    return false;
  }
  *rid = (*begin_).second;
  table_->GetTuple(*rid, tuple, exec_ctx_->GetTransaction());
  ++begin_;
  return true;
}

}  // namespace bustub
