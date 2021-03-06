//===----------------------------------------------------------------------===//
//
//                         PelotonDB
//
// mapper_hash_join.cpp
//
// Identification: src/backend/bridge/dml/mapper/mapper_hash_join.cpp
//
// Copyright (c) 2015, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "backend/bridge/dml/mapper/mapper.h"
#include "backend/planner/hash_join_plan.h"
#include "backend/planner/projection_plan.h"
#include "backend/bridge/ddl/schema_transformer.h"

namespace peloton {
namespace bridge {

//===--------------------------------------------------------------------===//
// HashJoin
//===--------------------------------------------------------------------===//

/**
 * @brief Convert a Postgres HashState into a Peloton HashPlanNode
 * @return Pointer to the constructed AbstractPlan
 */
const planner::AbstractPlan *PlanTransformer::TransformHashJoin(
    const HashJoinPlanState *hj_plan_state) {
  planner::AbstractPlan *result = nullptr;
  planner::HashJoinPlan *plan_node = nullptr;
  PelotonJoinType join_type =
      PlanTransformer::TransformJoinType(hj_plan_state->jointype);
  if (join_type == JOIN_TYPE_INVALID) {
    LOG_ERROR("unsupported join type: %d", hj_plan_state->jointype);
    return nullptr;
  }

  LOG_INFO("Handle hash join with join type: %d", join_type);

  /*
  std::vector<planner::HashJoinPlan::JoinClause> join_clauses(
      BuildHashJoinClauses(mj_plan_state->mj_Clauses,
                            mj_plan_state->mj_NumClauses);
  */

  expression::AbstractExpression *join_filter = ExprTransformer::TransformExpr(
      reinterpret_cast<ExprState *>(hj_plan_state->joinqual));

  expression::AbstractExpression *plan_filter = ExprTransformer::TransformExpr(
      reinterpret_cast<ExprState *>(hj_plan_state->qual));

  expression::AbstractExpression *predicate = nullptr;
  if (join_filter && plan_filter) {
    predicate = expression::ConjunctionFactory(EXPRESSION_TYPE_CONJUNCTION_AND,
                                               join_filter, plan_filter);
  } else if (join_filter) {
    predicate = join_filter;
  } else {
    predicate = plan_filter;
  }

  /* Transform project info */
  std::unique_ptr<const planner::ProjectInfo> project_info(nullptr);

  project_info.reset(BuildProjectInfoFromTLSkipJunk(hj_plan_state->targetlist));

  LOG_INFO("%s", project_info.get()->Debug().c_str());

  if (project_info.get()->isNonTrivial()) {
    // we have non-trivial projection
    LOG_INFO("We have non-trivial projection");
    auto project_schema = SchemaTransformer::GetSchemaFromTupleDesc(
        hj_plan_state->tts_tupleDescriptor);
    result =
        new planner::ProjectionPlan(project_info.release(), project_schema);
    plan_node = new planner::HashJoinPlan(join_type, predicate, nullptr);
    result->AddChild(plan_node);
  } else {
    LOG_INFO("We have direct mapping projection");
    plan_node =
        new planner::HashJoinPlan(join_type, predicate, project_info.release());
    result = plan_node;
  }

  const planner::AbstractPlan *outer =
      PlanTransformer::TransformPlan(outerAbstractPlanState(hj_plan_state));
  const planner::AbstractPlan *inner =
      PlanTransformer::TransformPlan(innerAbstractPlanState(hj_plan_state));

  /* Add the children nodes */
  plan_node->AddChild(outer);
  plan_node->AddChild(inner);

  LOG_INFO("Finishing mapping Hash join, JoinType: %d", join_type);
  return result;
}

}  // namespace bridge
}  // namespace peloton
