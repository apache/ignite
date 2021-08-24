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
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.calcite.plan.RelOptUtil;
import org.apache.calcite.plan.volcano.RelSubset;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.metadata.ReflectiveRelMetadataProvider;
import org.apache.calcite.rel.metadata.RelColumnOrigin;
import org.apache.calcite.rel.metadata.RelMdSelectivity;
import org.apache.calcite.rel.metadata.RelMdUtil;
import org.apache.calcite.rel.metadata.RelMetadataProvider;
import org.apache.calcite.rel.metadata.RelMetadataQuery;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rex.RexCall;
import org.apache.calcite.rex.RexLiteral;
import org.apache.calcite.rex.RexLocalRef;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.rex.RexSlot;
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
import org.apache.ignite.internal.processors.query.calcite.schema.IgniteStatisticsImpl;
import org.apache.ignite.internal.processors.query.calcite.schema.IgniteTable;
import org.apache.ignite.internal.processors.query.calcite.type.IgniteTypeFactory;
import org.apache.ignite.internal.processors.query.calcite.util.Commons;
import org.apache.ignite.internal.processors.query.calcite.util.RexUtils;
import org.apache.ignite.internal.processors.query.stat.ColumnStatistics;
import org.h2.value.Value;

/** */
public class IgniteMdSelectivity extends RelMdSelectivity {
    /** Default selectivity for is null conditions. */
    private static final double IS_NULL_SELECTIVITY = 0.1;

    /** Default selectivity for is not null confitions. */
    private static final double NOT_NULL_SELECTIVITY = 0.9;

    /** Default selectivity for equals conditions. */
    private static final double EQUALS_SELECTIVITY = 0.15;

    /** Default selectivity for comparison conitions. */
    private static final double COMPARISON_SELECTIVITY = 0.5;

    /** Default selectivity for other conditions. */
    private static final double OTHER_SELECTIVITY = 0.25;

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
        IgniteTable tbl = rel.getTable().unwrap(IgniteTable.class);

        if (predicate == null)
            return getTablePredicateBasedSelectivity(rel, tbl, mq, rel.condition());

        RexNode condition = rel.pushUpPredicate();

        if (condition == null)
            return getTablePredicateBasedSelectivity(rel, tbl, mq, predicate);

        RexNode diff = RelMdUtil.minusPreds(RexUtils.builder(rel), predicate, condition);

        return getTablePredicateBasedSelectivity(rel, tbl, mq, diff);
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
     * @param mq RelMetadataQuery.
     * @param predicate Predicate to estimate selectivity by.
     * @return Selectivity.
     */
    private double getTablePredicateBasedSelectivity(
        RelNode rel,
        IgniteTable tbl,
        RelMetadataQuery mq,
        RexNode predicate
    ) {
        if (tbl == null)
            return RelMdUtil.guessSelectivity(predicate);

        double sel = 1.0;

        Map<RexSlot, Boolean> addNotNull = new HashMap<>();

        for (RexNode pred : RelOptUtil.conjunctions(predicate)) {
            SqlKind predKind = pred.getKind();
            RexLocalRef op = getOperand(pred, RexLocalRef.class);

            if (predKind == SqlKind.OR) {
                double orSelTotal = 1;

                for (RexNode orPred : RelOptUtil.disjunctions(pred))
                    orSelTotal *= 1 - getTablePredicateBasedSelectivity(rel, tbl, mq, orPred);


                sel *= 1 - orSelTotal;
            }
            else if (predKind == SqlKind.NOT) {
                if (op == null)
                    sel *= guessSelectivity(pred);
                else {
                    tryAddNotNull(addNotNull, tbl, op);

                    sel *= 1 - getTablePredicateBasedSelectivity(rel, tbl, mq, op);
                }
            }
            else if (predKind == SqlKind.LOCAL_REF) {
                if (op != null)
                    addNotNull.put(op, Boolean.TRUE);

                sel *= estimateRefSelectivity(rel, mq, tbl, (RexLocalRef)pred);
            } else if (predKind == SqlKind.IS_NULL) {
                if (op != null)
                    addNotNull.put(op, Boolean.FALSE);

                sel *= estimateIsNullSelectivity(rel, mq, tbl, pred);

            } else if (predKind == SqlKind.IS_NOT_NULL) {
                if (op != null)
                    addNotNull.put(op, Boolean.FALSE);

                sel *= estimateIsNotNullSelectivity(rel, mq, tbl, pred);
            } else if (predKind == SqlKind.EQUALS) {
                if (op != null)
                    addNotNull.put(op, Boolean.TRUE);

                sel *= estimateEqualsSelectivity(rel, mq, tbl, pred);
            } else if (predKind.belongsTo(SqlKind.COMPARISON)) {
                if (op != null)
                    addNotNull.put(op, Boolean.TRUE);

                sel *= estimateComparisonSelectivity(rel, mq, tbl, pred);
            } else
                sel *= .25;
        }

        // Estimate not null selectivity in addition to comparison.
        for (Map.Entry<RexSlot, Boolean> colAddNotNull : addNotNull.entrySet()) {
            if (colAddNotNull.getValue()) {
                ColumnStatistics colStat = getColStatBySlot(rel, mq, tbl, colAddNotNull.getKey());

                sel *= (colStat == null) ? NOT_NULL_SELECTIVITY : estimateNotNullSelectivity(colStat);
            }
        }

        return sel;
    }

    /**
     * Try to add operand "not null" flag if there are no false flag for it.
     *
     * @param addNotNull Map with "add not null" flags for operands.
     * @param tbl IgniteTable.
     * @param op RexSlot to add operand by.
     */
    private void tryAddNotNull(Map<RexSlot, Boolean> addNotNull, IgniteTable tbl, RexSlot op) {
        Boolean colNotNull = addNotNull.get(op);

        addNotNull.put(op, (colNotNull == null) || colNotNull);
    }

    /**
     * Estimate local ref selectivity (means is true confition).
     *
     * @param rel RelNode.
     * @param mq RelMetadataQuery.
     * @param tbl IgniteTable.
     * @param ref RexLocalRef.
     * @return Selectivity estimation.
     */
    private double estimateRefSelectivity(RelNode rel, RelMetadataQuery mq, IgniteTable tbl, RexLocalRef ref) {
        ColumnStatistics colStat = getColStatBySlot(rel, mq, tbl, ref);
        double res = 0.33;
        if (colStat == null)
            // true, false and null with equivalent probability
            return res;

        if (colStat.max() == null || colStat.max().getType() != Value.BOOLEAN)
            return res;

        Boolean min = colStat.min().getBoolean();
        Boolean max = colStat.max().getBoolean();

        if (!max)
            return 0;

        double notNullSel = estimateNotNullSelectivity(colStat);

        return (max && min) ? notNullSel : notNullSel / 2;
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
        SqlKind predKind = pred.getKind();

        if (predKind != SqlKind.IS_NULL && predKind != SqlKind.IS_NOT_NULL && predKind != SqlKind.LOCAL_REF &&
            predKind != SqlKind.NOT && !predKind.belongsTo(SqlKind.COMPARISON))
            return null;

        RexLocalRef operand = getOperand(pred, RexLocalRef.class);

        if (operand == null)
            return null;

        return getColStatBySlot(rel, mq, tbl, operand);
    }

    /**
     * Get column statistics.
     *
     * @param rel RelNode.
     * @param mq RelMetadataQuery.
     * @param tbl IgniteTable to get statistics from.
     * @param pred RelSlot to get statistics by related column.
     * @return ColumnStatistics or {@code null}.
     */
    private ColumnStatistics getColStatBySlot(RelNode rel, RelMetadataQuery mq, IgniteTable tbl, RexSlot pred) {
        Set<RelColumnOrigin> origins = mq.getColumnOrigins(rel, pred.getIndex());

        if (origins == null || origins.isEmpty())
            return null;

        IgniteTypeFactory typeFactory = Commons.typeFactory(rel);

        List<String> columns = tbl.getRowType(typeFactory).getFieldNames();

        String colName = columns.get(origins.iterator().next().getOriginColumnOrdinal());

        if (QueryUtils.KEY_FIELD_NAME.equals(colName))
            colName = tbl.descriptor().typeDescription().keyFieldName();

        Statistic stat = tbl.getStatistic();

        if (stat == null)
            return null;

        return ((IgniteStatisticsImpl)stat).getColumnStatistics(colName);
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

            if (literal == null)
                return 1.;

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

            if (operands.isEmpty() || operands.size() > 2)
                return null;

            boolean assignable0 = cls.isAssignableFrom(operands.get(0).getClass());
            boolean assignable1 = operands.size() > 1 && cls.isAssignableFrom(operands.get(1).getClass());

            if (assignable0 && assignable1)
                return null;

            if (assignable0)
                return (T)operands.get(0);

            if (assignable1)
                return (T)operands.get(1);

            for (RexNode operand : operands) {
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
            return IS_NULL_SELECTIVITY;
        else if (pred.getKind() == SqlKind.IS_NOT_NULL)
            return NOT_NULL_SELECTIVITY;
        else if (pred.isA(SqlKind.EQUALS))
            return EQUALS_SELECTIVITY;
        else if (pred.isA(SqlKind.COMPARISON))
            return COMPARISON_SELECTIVITY;
        else
            return OTHER_SELECTIVITY;
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
