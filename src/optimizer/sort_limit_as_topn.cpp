#include "optimizer/optimizer.h"

namespace bustub {

auto Optimizer::OptimizeSortLimitAsTopN(const AbstractPlanNodeRef &plan) -> AbstractPlanNodeRef {
  // TODO(student): implement sort + limit -> top N optimizer rule

  std::vector<AbstractPlanNodeRef> children;
  for(auto child:plan->GetChildren())
  {
    auto new_child=OptimizeSortLimitAsTopN(child);
    children.push_back(new_child);
  }

  auto optimized_plan=plan->CloneWithChildren(std::move(children));
  auto plan_children=optimized_plan->GetChildren();
  if(optimized_plan->GetType()==PlanType::Limit&&plan_children.size()==1&&plan_children[0]->GetType()==PlanType::Sort)
  {
    auto limit_plan=dynamic_cast<const LimitPlanNode &>(*optimized_plan);
    auto sort_plan=dynamic_cast<const SortPlanNode &>(*plan_children[0]);

    SchemaRef output_schema=std::make_shared<Schema>(limit_plan.OutputSchema());
    auto order_bys=sort_plan.GetOrderBy();
    size_t limit=limit_plan.GetLimit();

    AbstractPlanNodeRef topn_child=sort_plan.GetChildAt(0);
    return std::make_shared<TopNPlanNode>(output_schema,topn_child,order_bys,limit);
  }

  return optimized_plan;
}

}  // namespace bustub
