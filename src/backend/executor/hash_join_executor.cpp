/*-------------------------------------------------------------------------
 *
 * hash_join.cpp
 * file description
 *
 * Copyright(c) 2015, CMU
 *
 * /peloton/src/executor/hash_join_executor.cpp
 *
 *-------------------------------------------------------------------------
 */

#include <vector>

#include "backend/common/types.h"
#include "backend/common/logger.h"
#include "backend/executor/logical_tile_factory.h"
#include "backend/executor/hash_join_executor.h"
#include "backend/expression/abstract_expression.h"
#include "backend/expression/container_tuple.h"

namespace peloton {
namespace executor {

/**
 * @brief Constructor for hash join executor.
 * @param node Hash join node corresponding to this executor.
 */
HashJoinExecutor::HashJoinExecutor(const planner::AbstractPlan *node,
                                   ExecutorContext *executor_context)
    : AbstractJoinExecutor(node, executor_context) {}

bool HashJoinExecutor::DInit() {
  assert(children_.size() == 2);
  auto status = AbstractJoinExecutor::DInit();
  if (status == false) return status;
  assert(children_[1]->GetRawNode()->GetPlanNodeType() == PLAN_NODE_TYPE_HASH);
  hash_executor_ = reinterpret_cast<HashExecutor *>(children_[1]);
  return true;
}

/**
 * @brief Creates logical tiles from the two input logical tiles after applying
 * join predicate.
 * @return true on success, false otherwise.
 */
bool HashJoinExecutor::DExecute() {
  // build hash map for right table
  if (!right_child_done_) {
    while (hash_executor_->Execute() == true)
      BufferRightTile(children_[1]->GetOutput());
    right_child_done_ = true;
  }

  for (;;) {
    // left child & right child all done
    if (left_child_done_ && right_child_done_) {
      return BuildOuterJoinOutput();
    }

    // if there is remaining pairs in buffer, release one at a time.
    if (!buffered_output_tiles.empty()) {
      // just hand in one.
      SetOutput(buffered_output_tiles.front());
      buffered_output_tiles.pop_front();
      return true;
    }

    // traverse every left child tile
    if (children_[0]->Execute()) {
      BufferLeftTile(children_[0]->GetOutput());
      LogicalTile *left_tile = left_result_tiles_.back().get();

      // traverse every tuple in curt left tile
      for (auto left_tile_row_itr : *left_tile) {
        auto hash = HashExecutor::HashMapType::key_type(
            left_tile, left_tile_row_itr, &hash_executor_->GetHashKeyIds());
        auto hash_result = hash_executor_->GetHashTable().find(hash);
        if (hash_result != hash_executor_->GetHashTable().end()) {
          RecordMatchedLeftRow(left_logical_tile_itr_, left_tile_row_itr);
          // traverse right set
          for (auto iter = hash_result->second.begin();
               iter != hash_result->second.end(); ++iter) {
            auto tile_index = iter->first;
            auto tuple_index = iter->second;
            RecordMatchedRightRow(tile_index, tuple_index);
            LogicalTile *right_tile = right_result_tiles_[tile_index].get();
            LogicalTile::PositionListsBuilder pos_lists_builder(left_tile,
                                                                right_tile);
            pos_lists_builder.AddRow(left_tile_row_itr, tuple_index);
            auto output_tile = BuildOutputLogicalTile(left_tile, right_tile);
            output_tile->SetPositionListsAndVisibility(
                pos_lists_builder.Release());
            buffered_output_tiles.emplace_back(output_tile.release());
          }  // end of traversing right set
        }    // end of if match
      }      // end of traversal of curt left_tile

      // Release at most one pair.
      // PS: This should be done after traversing all the tuples in curt tile
      left_logical_tile_itr_++;
      if (!buffered_output_tiles.empty()) {
        // release one at a time
        SetOutput(buffered_output_tiles.front());
        buffered_output_tiles.pop_front();
        return true;
      }
    }  // end of still have left tile

    // All left tiles are exhausted.
    else {
      left_child_done_ = true;
      return BuildOuterJoinOutput();
    }
  }  // end of infinite loop
  // never should go here
  return false;
}  // end of DExecute
}  // namespace executor
}  // namespace peloton

