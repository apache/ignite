/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.ignite.internal.processors.query.calcite.metadata;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.math.MathContext;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.calcite.plan.RelOptUtil;
import org.apache.calcite.plan.volcano.RelSubset;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.JoinRelType;
import org.apache.calcite.rel.metadata.ReflectiveRelMetadataProvider;
import org.apache.calcite.rel.metadata.RelColumnOrigin;
import org.apache.calcite.rel.metadata.RelMdSelectivity;
import org.apache.calcite.rel.metadata.RelMdUtil;
import org.apache.calcite.rel.metadata.RelMetadataProvider;
import org.apache.calcite.rel.metadata.RelMetadataQuery;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.rex.RexCall;
import org.apache.calcite.rex.RexLiteral;
import org.apache.calcite.rex.RexLocalRef;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.rex.RexSlot;
import org.apache.calcite.rex.RexUtil;
import org.apache.calcite.schema.Statistic;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.SqlOperator;
import org.apache.calcite.sql.type.BasicSqlType;
import org.apache.calcite.sql.type.SqlTypeFamily;
import org.apache.calcite.util.BuiltInMethod;
import org.apache.calcite.util.DateString;
import org.apache.calcite.util.TimeString;
import org.apache.calcite.util.TimestampString;
import org.apache.ignite.internal.processors.query.QueryUtils;
import org.apache.ignite.internal.processors.query.calcite.rel.AbstractIgniteJoin;
import org.apache.ignite.internal.processors.query.calcite.rel.AbstractIndexScan;
import org.apache.ignite.internal.processors.query.calcite.rel.IgniteExchange;
import org.apache.ignite.internal.processors.query.calcite.rel.IgniteHashIndexSpool;
import org.apache.ignite.internal.processors.query.calcite.rel.IgniteIndexScan;
import org.apache.ignite.internal.processors.query.calcite.rel.IgniteSortedIndexSpool;
import org.apache.ignite.internal.processors.query.calcite.rel.IgniteTableScan;
import org.apache.ignite.internal.processors.query.calcite.rel.IgniteTableSpool;
import org.apache.ignite.internal.processors.query.calcite.rel.ProjectableFilterableTableScan;
import org.apache.ignite.internal.processors.query.calcite.schema.IgniteStatisticsImpl;
import org.apache.ignite.internal.processors.query.calcite.schema.IgniteTable;
import org.apache.ignite.internal.processors.query.calcite.type.IgniteTypeFactory;
import org.apache.ignite.internal.processors.query.calcite.util.Commons;
import org.apache.ignite.internal.processors.query.calcite.util.RexUtils;
import org.apache.ignite.internal.processors.query.stat.ColumnStatistics;
import org.apache.ignite.internal.util.typedef.F;
import org.h2.value.Value;

/** */
public class IgniteMdSelectivity extends RelMdSelectivity {
    /**
     * Math context to use in estimations calculations.
     */
    private final MathContext MATH_CONTEXT = MathContext.DECIMAL64;

    /** */
    public static final RelMetadataProvider SOURCE =
        ReflectiveRelMetadataProvider.reflectiveSource(
            BuiltInMethod.SELECTIVITY.method, new IgniteMdSelectivity());

    /** */
    public Double getSelectivity(AbstractIndexScan rel, RelMetadataQuery mq, RexNode predicate) {
        if (predicate != null)
            return getSelectivity((ProjectableFilterableTableScan)rel, mq, predicate);

        List<RexNode> lowerCond = rel.lowerCondition();
        List<RexNode> upperCond = rel.upperCondition();

        if (F.isEmpty(lowerCond) && F.isEmpty(upperCond))
            return RelMdUtil.guessSelectivity(rel.condition());

        double idxSelectivity = 1.0;
        int len = F.isEmpty(lowerCond) ? upperCond.size() : F.isEmpty(upperCond) ? lowerCond.size() :
            Math.max(lowerCond.size(), upperCond.size());

        for (int i = 0; i < len; i++) {
            RexCall lower = F.isEmpty(lowerCond) || lowerCond.size() <= i ? null : (RexCall)lowerCond.get(i);
            RexCall upper = F.isEmpty(upperCond) || upperCond.size() <= i ? null : (RexCall)upperCond.get(i);

            assert lower != null || upper != null;

            if (lower != null && upper != null)
                idxSelectivity *= lower.op.kind == SqlKind.EQUALS ? .1 : .2;
            else
                idxSelectivity *= .35;
        }

        List<RexNode> conjunctions = RelOptUtil.conjunctions(rel.condition());

        if (!F.isEmpty(lowerCond))
            conjunctions.removeAll(lowerCond);
        if (!F.isEmpty(upperCond))
            conjunctions.removeAll(upperCond);

        RexNode remaining = RexUtil.composeConjunction(RexUtils.builder(rel), conjunctions, true);

        return idxSelectivity * RelMdUtil.guessSelectivity(remaining);
    }

    /** */
    public Double getSelectivity(ProjectableFilterableTableScan rel, RelMetadataQuery mq, RexNode predicate) {
        if (predicate == null)
            return RelMdUtil.guessSelectivity(rel.condition());

        RexNode condition = rel.pushUpPredicate();
        if (condition == null)
            return RelMdUtil.guessSelectivity(predicate);

        RexNode diff = RelMdUtil.minusPreds(RexUtils.builder(rel), predicate, condition);
        return RelMdUtil.guessSelectivity(diff);
    }

    /** */
    public Double getSelectivity(IgniteSortedIndexSpool rel, RelMetadataQuery mq, RexNode predicate) {
        if (predicate != null) {
            return mq.getSelectivity(rel.getInput(),
                RelMdUtil.minusPreds(
                    rel.getCluster().getRexBuilder(),
                    predicate,
                    rel.condition()));
        }

        return mq.getSelectivity(rel.getInput(), rel.condition());
    }

    public Double getSelectivity(RelSubset rel, RelMetadataQuery mq, RexNode predicate) {
        RelNode best = rel.getBest();
        if (best == null)
            return super.getSelectivity(rel, mq, predicate);

        return getSelectivity(best, mq, predicate);
    }

    public Double getSelectivity(IgniteIndexScan rel, RelMetadataQuery mq, RexNode predicate) {
        return getTablePredicateBasedSelectivity(rel, rel.getTable().unwrap(IgniteTable.class), mq, predicate);
    }

    public Double getSelectivity(IgniteTableScan rel, RelMetadataQuery mq, RexNode predicate) {
        IgniteTable tbl = rel.getTable().unwrap(IgniteTable.class);

        return getTablePredicateBasedSelectivity(rel, tbl, mq, predicate);
    }

    /**
     * Convert specified value into comparable type: BigDecimal,
     *
     * @param val Value to convert to comparable form.
     * @return Comparable form of value.
     */
    private BigDecimal toComparableValue(RexLiteral val) {
        RelDataType type = val.getType();
        if (type instanceof BasicSqlType) {
            BasicSqlType bType = (BasicSqlType)type;

            switch ((SqlTypeFamily)bType.getFamily()) {
                case NULL:
                    return null;

                case NUMERIC:
                    return val.getValueAs(BigDecimal.class);

                case DATE:
                    return new BigDecimal(val.getValueAs(DateString.class).getMillisSinceEpoch());

                case TIME:
                    return new BigDecimal(val.getValueAs(TimeString.class).getMillisOfDay());

                case TIMESTAMP:
                    return new BigDecimal(val.getValueAs(TimestampString.class).getMillisSinceEpoch());

                case BOOLEAN:
                    return (val.getValueAs(Boolean.class)) ? BigDecimal.ONE : BigDecimal.ZERO;

            }
        }

        return null;
    }

    /**
     * Convert specified value into comparable type: BigDecimal,
     *
     * @param val Value to convert to comparable form.
     * @return Comparable form of value.
     */
    private BigDecimal toComparableValue(Value val) {
        if (val == null)
            return null;

        switch (val.getType()) {
            case Value.NULL:
                throw new IllegalArgumentException("Can't compare null values");

            case Value.BOOLEAN:
                return (val.getBoolean()) ? BigDecimal.ONE : BigDecimal.ZERO;

            case Value.BYTE:
                return new BigDecimal(val.getByte());

            case Value.SHORT:
                return new BigDecimal(val.getShort());

            case Value.INT:
                return new BigDecimal(val.getInt());

            case Value.LONG:
                return new BigDecimal(val.getLong());

            case Value.DECIMAL:
                return val.getBigDecimal();

            case Value.DOUBLE:
                return BigDecimal.valueOf(val.getDouble());

            case Value.FLOAT:
                return BigDecimal.valueOf(val.getFloat());

            case Value.DATE:
                return BigDecimal.valueOf(val.getDate().getTime());

            case Value.TIME:
                return BigDecimal.valueOf(val.getTime().getTime());

            case Value.TIMESTAMP:
                return BigDecimal.valueOf(val.getTimestamp().getTime());

            case Value.BYTES:
                BigInteger bigInteger = new BigInteger(1, val.getBytes());
                return new BigDecimal(bigInteger);

            case Value.STRING:
            case Value.STRING_FIXED:
            case Value.STRING_IGNORECASE:
            case Value.ARRAY:
            case Value.JAVA_OBJECT:
            case Value.GEOMETRY:
                return null;

            case Value.UUID:
                BigInteger bigInt = new BigInteger(1, val.getBytes());
                return new BigDecimal(bigInt);

            default:
                throw new IllegalStateException("Unsupported H2 type: " + val.getType());
        }
    }

    /**
     * Predicate based selectivity for table. Estimate condition on each column taking in comparison it's statistics.
     *
     * @param rel Original rel node to fallback calculation by.
     * @param tbl Underlying IgniteTable.
     * @param mq RelMetadataQuery
     * @param predicate Predicate to estimate selectivity by.
     * @return Selectivity.
     */
    private double getTablePredicateBasedSelectivity(RelNode rel, IgniteTable tbl, RelMetadataQuery mq, RexNode predicate) {
        if (tbl == null)
            return super.getSelectivity(rel, mq, predicate);

        double sel = 1.0;

        Map<ColumnStatistics, Boolean> addNotNull = new HashMap<>();

        for (RexNode pred : RelOptUtil.conjunctions(predicate)) {
            ColumnStatistics colStat = getColStat(rel, mq, tbl, pred);

            if (pred.getKind() == SqlKind.IS_NULL)
                sel *= estimateIsNullSelectivity(rel, mq, tbl, pred);
            else if (pred.getKind() == SqlKind.IS_NOT_NULL) {
                if (colStat != null)
                    addNotNull.put(colStat, Boolean.FALSE);

                sel *= estimateIsNotNullSelectivity(rel, mq, tbl, pred);
            }
            else if (pred.isA(SqlKind.EQUALS)) {
                if (colStat != null) {
                    Boolean colNotNull = addNotNull.get(colStat);

                    addNotNull.put(colStat, (colNotNull == null) || colNotNull);
                }

                sel *= estimateEqualsSelectivity(rel, mq, tbl, pred);
            }
            else if (pred.isA(SqlKind.COMPARISON)) {
                if (colStat != null) {
                    Boolean colNotNull = addNotNull.get(colStat);

                    addNotNull.put(colStat, (colNotNull == null) || colNotNull);
                }

                sel *= estimateComparisonSelectivity(rel, mq, tbl, pred);
            }
            else
                sel *= .25;
        }

        // Estimate not null selectivity in addition to comparison.
        for (Map.Entry<ColumnStatistics, Boolean> colAddNotNull : addNotNull.entrySet())
            if (colAddNotNull.getValue())
                sel *= estimateNotNullSelectivity(colAddNotNull.getKey());

        return sel;
    }

    /**
     * Compute selectivity for "is null" condition.
     *
     * @param rel RelNode.
     * @param mq RelMetadataQuery.
     * @param tbl IgniteTable.
     * @param pred RexNode.
     * @return Selectivity estimation.
     */
    private double estimateIsNullSelectivity(RelNode rel, RelMetadataQuery mq, IgniteTable tbl, RexNode pred) {
        ColumnStatistics colStat = getColStat(rel, mq, tbl, pred);

        if (colStat == null)
            return guessSelectivity(pred);

        return estimateNullSelectivity(colStat);
    }

    /**
     * Compute selectivity for "is not null" condition.
     *
     * @param rel RelNode.
     * @param mq  RelMetadataQuery.
     * @param tbl IgniteTable.
     * @param pred RexNode.
     * @return Selectivity estimation.
     */
    private double estimateIsNotNullSelectivity(RelNode rel, RelMetadataQuery mq, IgniteTable tbl, RexNode pred) {
        ColumnStatistics colStat = getColStat(rel, mq, tbl, pred);

        if (colStat == null)
            return guessSelectivity(pred);

        return estimateNotNullSelectivity(colStat);
    }

    /**
     * Estimate selectivity for equals predicate.
     *
     * @param rel RElNode.
     * @param mq RelMetadataQuery.
     * @param tbl IgniteTable.
     * @param pred RexNode with predicate.
     *
     * @return Selectivity.
     */
    private double estimateEqualsSelectivity(
        RelNode rel,
        RelMetadataQuery mq,
        IgniteTable tbl,
        RexNode pred) {
        ColumnStatistics colStat = getColStat(rel, mq, tbl, pred);

        if (colStat == null)
            return guessSelectivity(pred);

        RexLiteral val = getOperand(pred, RexLiteral.class);

        if (val == null)
            return guessSelectivity(pred);

        BigDecimal comparableVal = toComparableValue(val);

        if (comparableVal == null)
            return guessSelectivity(pred);

        return estimateEqualsSelectivity(colStat, comparableVal);
    }

    /**
     * Estimate selectivity for comparison predicate.
     *
     * @param rel RelNode.
     * @param mq RelMetadataQuery.
     * @param tbl IgniteTable.
     * @param pred RexNode.
     * @return Selectivity.
     */
    private double estimateComparisonSelectivity(RelNode rel, RelMetadataQuery mq, IgniteTable tbl, RexNode pred) {
        ColumnStatistics colStat = getColStat(rel, mq, tbl, pred);

        if (colStat == null)
            return guessSelectivity(pred);

        return estimateRangeSelectivity(colStat, pred);
    }

    /**
     * Get column statistics.
     *
     * @param rel RelNode.
     * @param mq RelMetadataQuery.
     * @param tbl IgniteTable to get statistics from.
     * @param pred Predicate to get statistics by related column.
     * @return ColumnStatistics or {@code null}.
     */
    private ColumnStatistics getColStat(RelNode rel, RelMetadataQuery mq, IgniteTable tbl, RexNode pred) {
        Statistic stat = tbl.getStatistic();

        if (stat == null)
            return null;

        RexNode operand = getOperand(pred, RexLocalRef.class);

        if (operand == null)
            return null;

        Set<RelColumnOrigin> origins = mq.getColumnOrigins(rel, ((RexSlot)operand).getIndex());

        if (origins == null || origins.isEmpty())
            return null;

        IgniteTypeFactory typeFactory = Commons.typeFactory(rel);

        List<String> columns = tbl.getRowType(typeFactory).getFieldNames();

        String colName = columns.get(origins.iterator().next().getOriginColumnOrdinal());

        //        String colName = getColName(pred, columns);

        if (QueryUtils.KEY_FIELD_NAME.equals(colName))
            colName = tbl.descriptor().typeDescription().keyFieldName();

        return ((IgniteStatisticsImpl)stat).getColumnsStatistics().get(colName);
    }

    /**
     * Estimate range selectivity based on predicate.
     *
     * @param colStat Column statistics to use.
     * @param pred  Condition.
     * @return Selectivity.
     */
    private double estimateRangeSelectivity(ColumnStatistics colStat, RexNode pred) {
        if (pred instanceof RexCall) {
            RexLiteral literal = getOperand(pred, RexLiteral.class);
            BigDecimal val = toComparableValue(literal);

            return estimateSelectivity(colStat, val, pred);
        }

        return 1.;
    }

    /**
     * Estimate range selectivity based on predicate, condition and column statistics.
     *
     * @param colStat Column statistics to use.
     * @param val Condition value.
     * @param pred Condition.
     * @return Selectivity.
     */
    private double estimateSelectivity(ColumnStatistics colStat, BigDecimal val, RexNode pred) {
        // Without value or statistics we can only guess.
        if (val == null)
            return guessSelectivity(pred);

        SqlOperator op = ((RexCall)pred).op;

        BigDecimal min = toComparableValue(colStat.min());
        BigDecimal max = toComparableValue(colStat.max());
        BigDecimal total = (min == null || max == null) ? null : max.subtract(min).abs();

        if (total == null)
            // No min/max mean that all values are null for coumn.
            return 0.;

        // All values the same so check condition and return all or nothing selectivity.
        if (total.signum() == 0) {
            BigDecimal diff = val.subtract(min);
            int diffSign = diff.signum();

            switch (op.getKind()) {
                case GREATER_THAN:
                    return (diffSign < 0) ? 1. : 0.;

                case LESS_THAN:
                    return (diffSign > 0) ? 1. : 0.;

                case GREATER_THAN_OR_EQUAL:
                    return (diffSign <= 0) ? 1. : 0.;

                case LESS_THAN_OR_EQUAL:
                    return (diffSign >= 0) ? 1. : 0.;

                default:
                    return guessSelectivity(pred);
            }
        }

        // Estimate percent of selectivity by ranges.
        BigDecimal actual = BigDecimal.ZERO;

        switch (op.getKind()) {
            case GREATER_THAN:
            case GREATER_THAN_OR_EQUAL:
                actual = max.subtract(val);

                if (actual.signum() < 0)
                    return 0.;

                break;

            case LESS_THAN:
            case LESS_THAN_OR_EQUAL:
                actual = val.subtract(min);

                if (actual.signum() < 0)
                    return 0.;

                break;

            default:
                return guessSelectivity(pred);
        }

        return (actual.compareTo(total) > 0) ? 1 : actual.divide(total, MATH_CONTEXT).doubleValue();
    }

    /**
     * Estimate "=" selectivity by column statistics.
     *
     * @param colStat Column statistics.
     * @param comparableVal Comparable value to compare with.
     * @return Selectivity.
     */
    private double estimateEqualsSelectivity(ColumnStatistics colStat, BigDecimal comparableVal) {
        if (colStat.total() == 0)
            return 1.;

        if (colStat.total() - colStat.nulls() == 0)
            return 0.;

        if (colStat.min() != null) {
            BigDecimal minComparable = toComparableValue(colStat.min());
            if (minComparable != null && minComparable.compareTo(comparableVal) > 0)
                return 0.;
        }

        if (colStat.max() != null) {
            BigDecimal maxComparable = toComparableValue(colStat.max());
            if (maxComparable != null && maxComparable.compareTo(comparableVal) < 0)
                return 0.;
        }

        double expectedRows = (double)(colStat.distinct()) / (colStat.total() - colStat.nulls());

        return expectedRows / colStat.total();
    }

    /**
     * Estimate "is not null" selectivity by column statistics.
     *
     * @param colStat Column statistics.
     * @return Selectivity.
     */
    private double estimateNotNullSelectivity(ColumnStatistics colStat) {
        if (colStat.total() == 0)
            return 1;

        return (double)(colStat.total() - colStat.nulls()) / colStat.total();
    }

    /**
     * Estimate "is null" selectivity by column statistics.
     *
     * @param colStat Column statistics.
     * @return Selectivity.
     */
    private double estimateNullSelectivity(ColumnStatistics colStat) {
        if (colStat.total() == 0)
            return 1;

        return (double)colStat.nulls() / colStat.total();
    }

    /**
     * Get RexNode operand by type.
     *
     * @param pred RexNode to get operand by.
     * @param cls Operand class.
     * @return Operand or {@code null} if it unable to find operand with specified type.
     */
    private <T> T getOperand(RexNode pred, Class<T> cls) {
        if (pred instanceof RexCall) {
            List<RexNode> operands = ((RexCall)pred).getOperands();

            if (operands.size() > 2)
                return null;

            for (RexNode operand : operands) {
                if (cls.isAssignableFrom(operand.getClass()))
                    return (T)operand;

                if (operand instanceof RexCall) {
                    T res = getOperand(operand, cls);

                    if (res != null)
                        return res;
                }
            }
        }
        return null;
    }

    /**
     * Guess selectivity by predicate type only.
     *
     * @param pred Predicate to guess selectivity by.
     * @return Selectivity.
     */
    private double guessSelectivity(RexNode pred) {
        if (pred.getKind() == SqlKind.IS_NULL)
            return .1;
        else if (pred.getKind() == SqlKind.IS_NOT_NULL)
            return .9;
        else if (pred.isA(SqlKind.EQUALS))
            return .15;
        else if (pred.isA(SqlKind.COMPARISON))
            return .5;
        else
            return .25;
    }

    /**
     * Get selectivity of exchange by it's input selectivity.
     *
     * @param exch IgniteExchange.
     * @param mq RelMetadataQuery
     * @param predicate Predicate.
     * @return Selectivity or {@code null} if it can't be estimated.
     */
    public Double getSelectivity(IgniteExchange exch, RelMetadataQuery mq, RexNode predicate) {
        RelNode input = exch.getInput();

        if (input == null)
            return null;

        return getSelectivity(input, mq, predicate);
    }

    /**
     * Get selectivity of table spool by it's input selectivity.
     *
     * @param tspool IgniteTableSpool.
     * @param mq RelMetadataQuery
     * @param predicate Predicate.
     * @return Selectivity or {@code null} if it can't be estimated.
     */
    public Double getSelectivity(IgniteTableSpool tspool, RelMetadataQuery mq, RexNode predicate) {
        RelNode input = tspool.getInput();

        if (input == null)
            return null;

        return getSelectivity(input, mq, predicate);
    }

    /**
     * Get selectivity of join by it's input selectivity.
     *
     * @param join AbstractIgniteJoin.
     * @param mq RelMetadataQuery
     * @param predicate Predicate.
     * @return Selectivity or {@code null} if it can't be estimated.
     */
    public Double getSelectivity(AbstractIgniteJoin join, RelMetadataQuery mq, RexNode predicate) {
        double sel = 1.0;
        JoinRelType joinType = join.getJoinType();
        RexNode leftPred, rightPred;
        final RexBuilder rexBuilder = join.getCluster().getRexBuilder();
        int[] adjustments = new int[join.getRowType().getFieldCount()];

        if (predicate != null) {
            RexNode pred;
            List<RexNode> leftFilters = new ArrayList<>();
            List<RexNode> rightFilters = new ArrayList<>();
            List<RexNode> joinFilters = new ArrayList<>();
            List<RexNode> predList = RelOptUtil.conjunctions(predicate);

            RelOptUtil.classifyFilters(
                join,
                predList,
                joinType,
                joinType == JoinRelType.INNER,
                !joinType.generatesNullsOnLeft(),
                !joinType.generatesNullsOnRight(),
                joinFilters,
                leftFilters,
                rightFilters);
            leftPred = RexUtil.composeConjunction(rexBuilder, leftFilters, true);
            rightPred = RexUtil.composeConjunction(rexBuilder, rightFilters, true);

            for (RelNode child : join.getInputs()) {
                RexNode modifiedPred = null;

                if (child == join.getLeft())
                    pred = leftPred;
                else
                    pred = rightPred;

                if (pred != null) {
                    // convert the predicate to reference the types of the children
                    modifiedPred =
                        pred.accept(new RelOptUtil.RexInputConverter(
                            rexBuilder,
                            null,
                            child.getRowType().getFieldList(),
                            adjustments));
                }
                Double childSelectivity = mq.getSelectivity(child, modifiedPred);

                if (childSelectivity == null)
                    return null;

                sel *= childSelectivity;
            }
            sel *= RelMdUtil.guessSelectivity(RexUtil.composeConjunction(rexBuilder, joinFilters, true));
        }

        return sel;
    }

    /**
     * Get selectivity of hash index spool by it's input selectivity.
     *
     * @param rel IgniteHashIndexSpool.
     * @param mq RelMetadataQuery
     * @param predicate Predicate.
     * @return Selectivity or {@code null} if it can't be estimated.
     */
    public Double getSelectivity(IgniteHashIndexSpool rel, RelMetadataQuery mq, RexNode predicate) {
        if (predicate != null) {
            return mq.getSelectivity(rel.getInput(),
                RelMdUtil.minusPreds(
                    rel.getCluster().getRexBuilder(),
                    predicate,
                    rel.condition()));
        }

        return mq.getSelectivity(rel.getInput(), rel.condition());
    }
}
