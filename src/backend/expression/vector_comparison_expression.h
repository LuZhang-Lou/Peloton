//===----------------------------------------------------------------------===//
//
//                         PelotonDB
//
// vecotor_comparison_expression.h
//
// Identification: src/backend/expression/vector_comparison_expression.h
//
// Copyright (c) 2015, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#pragma once

#include "backend/common/executor_context.h"
#include "backend/common/serializer.h"
#include "backend/storage/tuple.h"
#include "backend/common/tuple_schema.h"
#include "backend/common/value_peeker.h"
#include "backend/expression/abstract_expression.h"
#include "backend/expression/comparison_expression.h"
#include "backend/storage/data_table.h"

#include <string>
#include <cassert>

namespace peloton {
namespace expression {

// Compares two tuples column by column using lexicographical compare.
template<typename OP>
NValue compare_tuple(const AbstractTuple& tuple1, const TableTuple& tuple2)
{
    assert(tuple1.getSchema()->columnCount() == tuple2.getSchema()->columnCount());
    NValue fallback_result = OP::includes_equality() ? NValue::getTrue() : NValue::getFalse();
    int schemaSize = tuple1.getSchema()->columnCount();
    for (int columnIdx = 0; columnIdx < schemaSize; ++columnIdx) {
        NValue value1 = tuple1.getNValue(columnIdx);
        if (value1.isNull()) {
            fallback_result = NValue::getNullValue(VALUE_TYPE_BOOLEAN);
            if (OP::implies_null_for_row()) {
                return fallback_result;
            }
            continue;
        }
        NValue value2 = tuple2.getNValue(columnIdx);
        if (value2.isNull()) {
            fallback_result = NValue::getNullValue(VALUE_TYPE_BOOLEAN);
            if (OP::implies_null_for_row()) {
                return fallback_result;
            }
            continue;
        }
        if (OP::compare_withoutNull(value1, tuple2.getNValue(columnIdx)).isTrue()) {
            if (OP::implies_true_for_row(value1, value2)) {
                // allow early return on strict inequality
                return NValue::getTrue();
            }
        }
        else {
            if (OP::implies_false_for_row(value1, value2)) {
                // allow early return on strict inequality
                return NValue::getFalse();
            }
        }
    }
    // The only cases that have not already short-circuited involve all equal columns.
    // Each op either includes or excludes that particular case.
    return fallback_result;
}

//Assumption - quantifier is on the right
template <typename OP, typename ValueExtractorLeft, typename ValueExtractorRight>
class VectorComparisonExpression : public AbstractExpression {
public:
    VectorComparisonExpression(ExpressionType et,
                           AbstractExpression *left,
                           AbstractExpression *right,
                           QuantifierType quantifier)
        : AbstractExpression(et, left, right),
          m_quantifier(quantifier)
    {
        assert(left != NULL);
        assert(right != NULL);
    };

    NValue eval(const AbstractTuple *tuple1, const TableTuple *tuple2) const;

    std::string debugInfo(const std::string &spacer) const {
        return (spacer + "VectorComparisonExpression\n");
    }

private:
    QuantifierType m_quantifier;
};

struct NValueExtractor
{
    typedef NValue ValueType;

    NValueExtractor(NValue value) :
        m_value(value), m_hasNext(true)
    {}

    int64_t resultSize() const
    {
        return hasNullValue() ? 0 : 1;
    }

    bool hasNullValue() const
    {
        return m_value.isNull();
    }

    bool hasNext()
    {
        return m_hasNext;
    }

    ValueType next()
    {
        m_hasNext = false;
        return m_value;
    }

    template<typename OP>
    NValue compare(const AbstractTuple& tuple) const
    {
        assert(tuple.getSchema()->columnCount() == 1);
        return compare<OP>(tuple.getNValue(0));
    }

    template<typename OP>
    NValue compare(const NValue& nvalue) const
    {
        if (m_value.isNull()) {
            return NValue::getNullValue(VALUE_TYPE_BOOLEAN);
        }
        if (nvalue.isNull()) {
            return NValue::getNullValue(VALUE_TYPE_BOOLEAN);
        }
        return OP::compare_withoutNull(m_value, nvalue);
    }

    std::string debug() const
    {
        return m_value.isNull() ? "NULL" : m_value.debug();
    }

    ValueType m_value;
    bool m_hasNext;
};

struct TupleExtractor
{
    typedef AbstractTuple ValueType;

    TupleExtractor(NValue value) :
        m_table(getOutputTable(value)),
        m_iterator(m_table->iterator()),
        m_tuple(m_table->schema()),
        m_size(m_table->activeTupleCount())
    {}

    int64_t resultSize() const
    {
        return m_size;
    }

    bool hasNext()
    {
        return m_iterator.next(m_tuple);
    }

    ValueType next()
    {
        return m_tuple;
    }

    bool hasNullValue() const
    {
        if (m_tuple.isNullTuple()) {
            return true;
        }
        int schemaSize = m_tuple.getSchema()->columnCount();
        for (int columnIdx = 0; columnIdx < schemaSize; ++columnIdx) {
            if (m_tuple.isNull(columnIdx)) {
                return true;
            }
        }
        return false;
    }

    template<typename OP>
    NValue compare(const AbstractTuple& tuple) const
    {
        return compare_tuple<OP>(m_tuple, tuple);
    }

    template<typename OP>
    NValue compare(const NValue& nvalue) const
    {
        assert(m_tuple.getSchema()->columnCount() == 1);
        NValue lvalue = m_tuple.getNValue(0);
        if (lvalue.isNull()) {
            return NValue::getNullValue(VALUE_TYPE_BOOLEAN);
        }
        if (nvalue.isNull()) {
            return NValue::getNullValue(VALUE_TYPE_BOOLEAN);
        }
        return OP::compare_withoutNull(lvalue, nvalue);
    }

    std::string debug() const
    {
        return m_tuple.isNullTuple() ? "NULL" : m_tuple.debug("TEMP");
    }

private:
    static Table* getOutputTable(const NValue& value)
    {
        int subqueryId = ValuePeeker::peekInteger(value);
        ExecutorContext* exeContext = ExecutorContext::getExecutorContext();
        Table* table = exeContext->getSubqueryOutputTable(subqueryId);
        assert(table != NULL);
        return table;
    }

    Table* m_table;
    TableIterator& m_iterator;
    ValueType m_tuple;
    int64_t m_size;
};

template <typename OP, typename ValueExtractorOuter, typename ValueExtractorInner>
NValue VectorComparisonExpression<OP, ValueExtractorOuter, ValueExtractorInner>::eval(const AbstractTuple *tuple1, const TableTuple *tuple2) const
{
    // Outer and inner expressions can be either a row (expr1, expr2, expr3...) or a single expr
    // The quantifier is expected on the right side of the expression "outer_expr OP ANY/ALL(inner_expr )"

    // The outer_expr OP ANY inner_expr evaluates as follows:
    // There is an exact match OP (outer_expr, inner_expr) == true => TRUE
    // There no match and the inner_expr produces a row where inner_expr is NULL => NULL
    // There no match and the inner_expr produces only non- NULL rows or empty => FALSE
    // The outer_expr is NULL or empty and the inner_expr is empty => FALSE
    // The outer_expr is NULL or empty and the inner_expr produces any row => NULL

    // The outer_expr OP ALL inner_expr evaluates as follows:
    // If inner_expr is empty => TRUE
    // If outer_expr OP inner_expr is TRUE for all inner_expr values => TRUE
    // If inner_expr contains NULL and outer_expr OP inner_expr is TRUE for all other inner values => NULL
    // If inner_expr contains NULL and outer_expr OP inner_expr is FALSE for some other inner values => FALSE
    // The outer_expr is NULL or empty and the inner_expr is empty => TRUE
    // The outer_expr is NULL or empty and the inner_expr produces any row => NULL

    // The outer_expr OP inner_expr evaluates as follows:
    // If inner_expr is NULL or empty => NULL
    // If outer_expr is NULL or empty => NULL
    // If outer_expr/inner_expr has more than 1 result => runtime exception
    // Else => outer_expr OP inner_expr

    // Evaluate the outer_expr. The return value can be either the value itself or a subquery id
    // in case of the row expression on the left side
    NValue lvalue = m_left->eval(tuple1, tuple2);
    ValueExtractorOuter outerExtractor(lvalue);
    if (outerExtractor.resultSize() > 1) {
        // throw runtime exception
        char message[256];
        snprintf(message, 256, "More than one row returned by a scalar/row subquery");
        throw Exception(VOLT_EE_EXCEPTION_TYPE_EEEXCEPTION, message);
    }

    // Evaluate the inner_expr. The return value is a subquery id or a value as well
    NValue rvalue = m_right->eval(tuple1, tuple2);
    ValueExtractorInner innerExtractor(rvalue);
    if (m_quantifier == QUANTIFIER_TYPE_NONE && innerExtractor.resultSize() > 1) {
        // throw runtime exception
        char message[256];
        snprintf(message, 256, "More than one row returned by a scalar/row subquery");
        throw Exception(VOLT_EE_EXCEPTION_TYPE_EEEXCEPTION, message);
    }

    if (innerExtractor.resultSize() == 0) {
        switch (m_quantifier) {
        case QUANTIFIER_TYPE_NONE: {
            return NValue::getNullValue(VALUE_TYPE_BOOLEAN);
        }
        case QUANTIFIER_TYPE_ANY: {
            return NValue::getFalse();
        }
        case QUANTIFIER_TYPE_ALL: {
            return NValue::getTrue();
        }
        }
    }

    assert (innerExtractor.resultSize() > 0);
    if (!outerExtractor.hasNext() || outerExtractor.hasNullValue()) {
        return NValue::getNullValue(VALUE_TYPE_BOOLEAN);
    }

    //  Iterate over the inner results until
    //  no qualifier - the first match ( single row at most)
    //  ANY qualifier - the first match
    //  ALL qualifier - the first mismatch
    bool hasInnerNull = false;
    NValue result;
    while (innerExtractor.hasNext()) {
        typename ValueExtractorInner::ValueType innerValue = innerExtractor.next();
        result = outerExtractor.template compare<OP>(innerValue);
        if (result.isTrue()) {
            if (m_quantifier != QUANTIFIER_TYPE_ALL) {
                return result;
            }
        }
        else if (result.isFalse()) {
            if (m_quantifier != QUANTIFIER_TYPE_ANY) {
                return result;
            }
        }
        else { //  result is null
            hasInnerNull = true;
        }
    }

    // A NULL match along the way determines the result
    // for cases that never found a definitive result.
    if (hasInnerNull) {
        return NValue::getNullValue(VALUE_TYPE_BOOLEAN);
    }
    // Otherwise, return the unanimous result. false for ANY, true for ALL.
    return result;
}

}  // End expression namespace
}  // End peloton namespace
