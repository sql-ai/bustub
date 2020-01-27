//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// insert_executor.h
//
// Identification: src/include/execution/executors/insert_executor.h
//
// Copyright (c) 2015-19, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#pragma once

#include <memory>
#include <utility>

#include "execution/executor_context.h"
#include "execution/executors/abstract_executor.h"
#include "execution/plans/insert_plan.h"
#include "storage/table/tuple.h"

namespace bustub {
/**
 * InsertExecutor executes an insert into a table.
 * Inserted values can either be embedded in the plan itself ("raw insert") 
 * or come from a child executor.
 */
class InsertExecutor : public AbstractExecutor 
{
 public:
  /**
   * Creates a new insert executor.
   * @param exec_ctx the executor context
   * @param plan the insert plan to be executed
   * @param child_executor the child executor to obtain insert values from, can be nullptr
   */
  InsertExecutor
  (
    ExecutorContext *exec_ctx, 
    const InsertPlanNode *plan,
    std::unique_ptr<AbstractExecutor> &&child_executor
  )
  : AbstractExecutor(exec_ctx), plan_(plan), child_executor_(move(child_executor))
  {    
  }

  const Schema *GetOutputSchema() override 
  { 
    return plan_->OutputSchema(); 
  }

  void Init() override 
  {
    table_ptr_  = GetExecutorContext()->GetCatalog()->GetTable(plan_->TableOid())->table_.get();
    schema_     = &GetExecutorContext()->GetCatalog()->GetTable(plan_->TableOid())->schema_;
    child_executor_->Init();
  }

  // Note that Insert does not make use of the tuple pointer being passed in.
  // We return false if the insert failed for any reason, and return true if all inserts succeeded.
  bool Next(Tuple *tuple) override 
  { 
    RID rid;
    if (plan_->IsRawInsert())
    {
      const std::vector<std::vector<Value>> &raw_vals = plan_->RawValues();    
      for (auto& row :   raw_vals)
      {        
        if (table_ptr_->InsertTuple(Tuple(row, schema_), &rid, exec_ctx_->GetTransaction()) == false)
        {
          return false;
        }
      }
    } 
    else 
    {
      // plan_->GetChildPlan();
      Tuple tuple;
      while (child_executor_->Next(&tuple))
      {
        if (table_ptr_->InsertTuple(tuple, &rid, exec_ctx_->GetTransaction()) == false)
        {
          return false;
        }
      }      
    }
    return true; 
  }

 private:
  /** The insert plan node to be executed. */
  const InsertPlanNode *plan_;
  std::unique_ptr<AbstractExecutor> child_executor_;
  TableHeap* table_ptr_;
  Schema* schema_;    
};
}  // namespace bustub
