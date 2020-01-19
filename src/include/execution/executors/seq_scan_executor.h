//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// seq_scan_executor.h
//
// Identification: src/include/execution/executors/seq_scan_executor.h
//
// Copyright (c) 2015-19, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#pragma once

#include <vector>

#include "execution/executor_context.h"
#include "execution/executors/abstract_executor.h"
#include "execution/plans/seq_scan_plan.h"
#include "storage/table/tuple.h"

namespace bustub {

/**
 * SeqScanExecutor executes a sequential scan over a table.
 */
class SeqScanExecutor : public AbstractExecutor 
{
 public:
  /**
   * Creates a new sequential scan executor.
   * @param exec_ctx the executor context
   * @param plan the sequential scan plan to be executed
   */
  SeqScanExecutor(
    ExecutorContext *exec_ctx, 
    const SeqScanPlanNode *plan) : 
    AbstractExecutor(exec_ctx), plan_(plan)
  {

  }

  void Init() override 
  {
    table_ptr_ = exec_ctx_->GetCatalog()->GetTable(plan_->GetTableOid())->table_.get();
    schema_ = &exec_ctx_->GetCatalog()->GetTable(plan_->GetTableOid())->schema_;
    iter_ = &table_ptr_->Begin(exec_ctx_->GetTransaction());
  }

  bool Next(Tuple *tuple) override 
  { 
    while (*iter_ != table_ptr_->End())
    {      
      *tuple = *(*iter_);
      if (plan_->GetPredicate()->Evaluate(tuple,schema_).GetAs<bool>())
      {
        ++(*iter_);
        return true;
      }
      ++(*iter_);
    }
    return false; 
  }

  const Schema *GetOutputSchema() override 
  { 
    return plan_->OutputSchema(); 
  }

 private:
  /** The sequential scan plan node to be executed. */
  const SeqScanPlanNode *plan_;
  TableHeap* table_ptr_;
  Schema* schema_;
  TableIterator* iter_;
};
}  // namespace bustub
