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
import java.math.MathContext;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.calcite.plan.RelOptUtil;
import org.apache.calcite.plan.volcano.RelSubset;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.Join;
import org.apache.calcite.rel.metadata.ReflectiveRelMetadataProvider;
import org.apache.calcite.rel.metadata.RelColumnOrigin;
import org.apache.calcite.rel.metadata.RelMdSelectivity;
import org.apache.calcite.rel.metadata.RelMdUtil;
import org.apache.calcite.rel.metadata.RelMetadataProvider;
import org.apache.calcite.rel.metadata.RelMetadataQuery;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rex.RexCall;
import org.apache.calcite.rex.RexInputRef;
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
import org.apache.ignite.internal.processors.query.calcite.rel.IgniteExchange;
import org.apache.ignite.internal.processors.query.calcite.rel.IgniteHashIndexSpool;
import org.apache.ignite.internal.processors.query.calcite.rel.IgniteSortedIndexSpool;
import org.apache.ignite.internal.processors.query.calcite.rel.IgniteTableSpool;
import org.apache.ignite.internal.processors.query.calcite.rel.ProjectableFilterableTableScan;
import org.apache.ignite.internal.processors.query.calcite.schema.IgniteCacheTable;
import org.apache.ignite.internal.processors.query.calcite.schema.IgniteStatisticsImpl;
import org.apache.ignite.internal.processors.query.calcite.schema.IgniteTable;
import org.apache.ignite.internal.processors.query.calcite.util.RexUtils;
import org.apache.ignite.internal.processors.query.stat.ColumnStatistics;
import org.jetbrains.annotations.Nullable;

/** */
public class IgniteMdSelectivity extends RelMdSelectivity {
    /** Default selectivity for IS NULL conditions. */
    private static final double IS_NULL_SELECTIVITY = 0.1;

    /** Default selectivity for IS NOT NULL conditions. */
    private static final double IS_NOT_NULL_SELECTIVITY = 1 - IS_NULL_SELECTIVITY;

    /** Default selectivity for equals conditions. */
    private static final double EQUALS_SELECTIVITY = 0.25;

    /** Default selectivity for comparison conitions. */
    private static final double COMPARISON_SELECTIVITY = 0.5;

    /** Default selectivity for other conditions. */
    private static final double DEFAULT_SELECTIVITY = 0.25;

    /**
     * Math context to use in estimations calculations.
     */
    private final MathContext MATH_CONTEXT = MathContext.DECIMAL64;

    /** */
    public static final RelMetadataProvider SOURCE =
        ReflectiveRelMetadataProvider.reflectiveSource(
            BuiltInMethod.SELECTIVITY.method, new IgniteMdSelectivity());

    /** */
    public Double getSelectivity(ProjectableFilterableTableScan rel, RelMetadataQuery mq, RexNode predicate) {
        if (predicate == null)
            return getTablePredicateBasedSelectivity(rel, mq, rel.condition());

        RexNode condition = rel.pushUpPredicate();

        if (condition == null)
            return getTablePredicateBasedSelectivity(rel, mq, predicate);

        RexNode diff = RelMdUtil.minusPreds(RexUtils.builder(rel), predicate, condition);

        return getTablePredicateBasedSelectivity(rel, mq, diff);
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

    /** */
    public Double getSelectivity(RelSubset rel, RelMetadataQuery mq, RexNode predicate) {
        RelNode best = rel.getBest();

        if (best == null)
            return super.getSelectivity(rel, mq, predicate);

        return getSelectivity(best, mq, predicate);
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

                default:
                    return null;
            }
        }

        return null;
    }

    /**
     * Predicate based selectivity for table. Estimate condition on each column taking in comparison it's statistics.
     *
     * @param rel Original rel node to fallback calculation by.
     * @param mq RelMetadataQuery.
     * @param predicate Predicate to estimate selectivity by.
     * @return Selectivity.
     */
    private double getTablePredicateBasedSelectivity(
        ProjectableFilterableTableScan rel,
        RelMetadataQuery mq,
        RexNode predicate
    ) {
        if (predicate == null || predicate.isAlwaysTrue())
            return 1.0;

        if (predicate.isAlwaysFalse())
            return 0.0;

        double sel = 1.0;

        Map<RexSlot, Boolean> addNotNull = new HashMap<>();

        for (RexNode pred : RelOptUtil.conjunctions(predicate)) {
            SqlKind predKind = pred.getKind();

            if (predKind == SqlKind.OR) {
                double orSelTotal = 1;

                for (RexNode orPred : RelOptUtil.disjunctions(pred))
                    orSelTotal *= 1 - getTablePredicateBasedSelectivity(rel, mq, orPred);

                sel *= 1 - orSelTotal;

                continue;
            }
            else if (predKind == SqlKind.NOT) {
                assert pred instanceof RexCall;

                sel *= 1 - getTablePredicateBasedSelectivity(rel, mq, ((RexCall)pred).getOperands().get(0));

                continue;
            }
            else if (predKind == SqlKind.SEARCH) {
                // Intentionally use of RexUtil.expandSearch (not RexUtils.expandSearchNullable), since here we
                // expand operator not for bytecode generation and expect output with OR/AND operators.
                sel *= getTablePredicateBasedSelectivity(rel, mq,
                    RexUtil.expandSearch(rel.getCluster().getRexBuilder(), null, pred));

                continue;
            }

            RexSlot op = null;

            if (pred instanceof RexCall)
                op = getOperand((RexCall)pred);
            else if (pred instanceof RexSlot)
                op = (RexSlot)pred;

            ColumnStatistics colStat = getColumnStatistics(mq, rel, op);

            if (colStat == null)
                sel *= guessSelectivity(pred);
            else if (predKind == SqlKind.LOCAL_REF) {
                if (op != null)
                    addNotNull.put(op, Boolean.TRUE);

                sel *= estimateRefSelectivity(rel, mq, (RexLocalRef)pred);
            }
            else if (predKind == SqlKind.IS_NULL) {
                if (op != null)
                    addNotNull.put(op, Boolean.FALSE);

                sel *= estimateIsNullSelectivity(colStat);
            }
            else if (predKind == SqlKind.IS_NOT_NULL) {
                if (op != null)
                    addNotNull.put(op, Boolean.FALSE);

                sel *= estimateIsNotNullSelectivity(colStat);
            }
            else if (predKind == SqlKind.EQUALS) {
                if (op != null)
                    addNotNull.put(op, Boolean.TRUE);

                assert pred instanceof RexCall;

                sel *= estimateEqualsSelectivity(colStat, (RexCall)pred);
            }
            else if (predKind.belongsTo(SqlKind.COMPARISON)) {
                if (op != null)
                    addNotNull.put(op, Boolean.TRUE);

                assert pred instanceof RexCall;

                sel *= estimateRangeSelectivity(colStat, (RexCall)pred);
            }
            else
                sel *= .25;
        }

        // Estimate not null selectivity in addition to comparison.
        for (Map.Entry<RexSlot, Boolean> colAddNotNull : addNotNull.entrySet()) {
            if (colAddNotNull.getValue()) {
                ColumnStatistics colStat = getColumnStatistics(mq, rel, colAddNotNull.getKey());

                sel *= (colStat == null) ? IS_NOT_NULL_SELECTIVITY : estimateIsNotNullSelectivity(colStat);
            }
        }

        return sel;
    }

    /**
     * Finds a column statistics by a given operand within table scan.
     *
     * @param mq Metadata query which used to find column origins in case
     *      the operand is input reference.
     * @param rel Table scan the operand related to.
     * @param op Operand to search statistics for.
     * @return Column statistcs or {@code null} if it's not possoble to determine
     *      the origins of the given operand or the is no statistics gathered
     *      for given column.
     */
    private @Nullable ColumnStatistics getColumnStatistics(RelMetadataQuery mq, ProjectableFilterableTableScan rel, RexSlot op) {
        RelColumnOrigin origin;

        if (op instanceof RexLocalRef)
            origin = rel.columnOriginsByRelLocalRef(op.getIndex());
        else if (op instanceof RexInputRef)
            origin = mq.getColumnOrigin(rel, op.getIndex());
        else
            return null;

        String colName = extactFieldName(origin);

        IgniteTable tbl = rel.getTable().unwrap(IgniteTable.class);

        assert tbl != null;

        if (tbl instanceof IgniteCacheTable && QueryUtils.KEY_FIELD_NAME.equals(colName))
            colName = ((IgniteCacheTable)tbl).descriptor().typeDescription().keyFieldName();

        Statistic stat = tbl.getStatistic();

        if (!(stat instanceof IgniteStatisticsImpl))
            return null;

        return ((IgniteStatisticsImpl)stat).getColumnStatistics(colName);
    }

    /** Returns field name for provided {@link RelColumnOrigin}. */
    private static String extactFieldName(RelColumnOrigin origin) {
        return origin.getOriginTable().getRowType().getFieldNames().get(origin.getOriginColumnOrdinal());
    }

    /**
     * Estimate local ref selectivity (means is true confition).
     *
     * @param rel RelNode.
     * @param mq RelMetadataQuery.
     * @param ref RexLocalRef.
     * @return Selectivity estimation.
     */
    private double estimateRefSelectivity(ProjectableFilterableTableScan rel, RelMetadataQuery mq, RexLocalRef ref) {
        ColumnStatistics colStat = getColumnStatistics(mq, rel, ref);
        double res = 0.33;

        if (colStat == null || colStat.max() == null || colStat.min() == null) {
            // true, false and null with equivalent probability
            return res;
        }

        // Check whether it can be considered as BOOL.
        boolean isBool = (colStat.max().compareTo(BigDecimal.ONE) == 0 || colStat.max().compareTo(BigDecimal.ZERO) == 0)
            && (colStat.min().compareTo(BigDecimal.ONE) == 0 || colStat.min().compareTo(BigDecimal.ZERO) == 0);

        if (!isBool)
            return res;

        Boolean min = colStat.min().compareTo(BigDecimal.ONE) == 0;
        Boolean max = colStat.max().compareTo(BigDecimal.ONE) == 0;

        if (!max)
            return 0;

        double notNullSel = estimateIsNotNullSelectivity(colStat);

        return (max && min) ? notNullSel : notNullSel / 2;
    }

    /**
     * Estimate range selectivity based on predicate.
     *
     * @param colStat Column statistics to use.
     * @param pred  Condition.
     * @return Selectivity.
     */
    private double estimateRangeSelectivity(ColumnStatistics colStat, RexCall pred) {
        RexLiteral literal = null;

        if (pred.getOperands().get(1) instanceof RexLiteral)
            literal = (RexLiteral)pred.getOperands().get(1);

        if (literal == null)
            return guessSelectivity(pred);

        BigDecimal val = toComparableValue(literal);

        return estimateSelectivity(colStat, val, pred);
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

        BigDecimal min = colStat.min();
        BigDecimal max = colStat.max();
        BigDecimal total = (min == null || max == null) ? null : max.subtract(min).abs();

        if (total == null)
            // No min/max mean that all values are null for column.
            return guessSelectivity(pred);

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
     * @param pred Comparable value to compare with.
     * @return Selectivity.
     */
    private double estimateEqualsSelectivity(ColumnStatistics colStat, RexCall pred) {
        if (colStat.total() == 0)
            return 1.;

        if (colStat.total() - colStat.nulls() == 0)
            return 0.;

        RexLiteral literal = null;
        if (pred.getOperands().get(1) instanceof RexLiteral)
            literal = (RexLiteral)pred.getOperands().get(1);

        if (literal == null)
            return guessSelectivity(pred);

        BigDecimal comparableVal = toComparableValue(literal);

        if (comparableVal == null)
            return guessSelectivity(pred);

        if (colStat.min() != null) {
            BigDecimal minComparable = colStat.min();
            if (minComparable != null && minComparable.compareTo(comparableVal) > 0)
                return 0.;
        }

        if (colStat.max() != null) {
            BigDecimal maxComparable = colStat.max();
            if (maxComparable != null && maxComparable.compareTo(comparableVal) < 0)
                return 0.;
        }

        double expectedRows = ((double)(colStat.total() - colStat.nulls())) / (colStat.distinct());

        return expectedRows / colStat.total();
    }

    /**
     * Estimate "is not null" selectivity by column statistics.
     *
     * @param colStat Column statistics.
     * @return Selectivity.
     */
    private double estimateIsNotNullSelectivity(ColumnStatistics colStat) {
        if (colStat.total() == 0)
            return IS_NOT_NULL_SELECTIVITY;

        return (double)(colStat.total() - colStat.nulls()) / colStat.total();
    }

    /**
     * Estimate "is null" selectivity by column statistics.
     *
     * @param colStat Column statistics.
     * @return Selectivity.
     */
    private double estimateIsNullSelectivity(ColumnStatistics colStat) {
        if (colStat.total() == 0)
            return IS_NULL_SELECTIVITY;

        return (double)colStat.nulls() / colStat.total();
    }

    /**
     * Get operand from given predicate.
     *
     * Assumes that predicat is normalized, thus operand should be only on the left side.
     *
     * @param pred RexNode to get operand by.
     * @return Operand or {@code null} if it's not possible to find operand with specified type.
     */
    private RexSlot getOperand(RexCall pred) {
        List<RexNode> operands = pred.getOperands();

        if (operands.isEmpty() || operands.size() > 2)
            return null;

        RexNode op = operands.get(0);

        if (op instanceof RexCall && op.isA(SqlKind.CAST))
            op = ((RexCall)op).operands.get(0);

        return op instanceof RexSlot ? (RexSlot)op : null;
    }

    /**
     * Guess selectivity by predicate type only.
     *
     * @param pred Predicate to guess selectivity by.
     * @return Selectivity.
     */
    private double guessSelectivity(@Nullable RexNode pred) {
        if (pred != null) {
            if (pred.getKind() == SqlKind.IS_NULL)
                return IS_NULL_SELECTIVITY;
            else if (pred.getKind() == SqlKind.IS_NOT_NULL)
                return IS_NOT_NULL_SELECTIVITY;
            else if (pred.isA(SqlKind.EQUALS))
                return EQUALS_SELECTIVITY;
            else if (pred.isA(SqlKind.COMPARISON))
                return COMPARISON_SELECTIVITY;
        }

        return DEFAULT_SELECTIVITY;
    }
    /** */
    @Override public @Nullable Double getSelectivity(Join rel, RelMetadataQuery mq, @Nullable RexNode predicate) {
        return guessSelectivity(predicate);
    }

    /**
     * Get selectivity of exchange by it's input selectivity.
     *
     * @param exch IgniteExchange.
     * @param mq RelMetadataQuery.
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
     * @param mq RelMetadataQuery.
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
     * Get selectivity of hash index spool by it's input selectivity.
     *
     * @param rel IgniteHashIndexSpool.
     * @param mq RelMetadataQuery.
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
