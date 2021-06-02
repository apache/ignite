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

package org.apache.ignite.internal.processors.query.h2.opt;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.math.MathContext;
import java.util.ArrayList;
import java.util.HashSet;

import org.apache.ignite.internal.processors.query.stat.ColumnStatistics;
import org.apache.ignite.internal.processors.query.stat.ObjectStatisticsImpl;
import org.apache.ignite.internal.util.typedef.F;
import org.h2.engine.Session;
import org.h2.expression.Comparison;
import org.h2.expression.Expression;
import org.h2.index.BaseIndex;
import org.h2.index.IndexCondition;
import org.h2.index.IndexType;
import org.h2.result.SortOrder;
import org.h2.table.Column;
import org.h2.table.IndexColumn;
import org.h2.table.TableFilter;
import org.h2.value.Value;
import org.jetbrains.annotations.Nullable;

/**
 * Index base.
 */
public abstract class H2IndexCostedBase extends BaseIndex {
    /** GridH2Table. */
    protected GridH2Table tbl;

    /**
     * Constructor.
     *
     * @param tbl Table.
     * @param name Index name.
     * @param cols Indexed columns.
     * @param type Index type.
     */
    protected H2IndexCostedBase(GridH2Table tbl, String name, IndexColumn[] cols, IndexType type) {
        initBaseIndex(tbl, 0, name, cols, type);
        this.tbl = tbl;
    }

    /** Null fraction by default. */
    private final double NULL_FRACTION = 0.25;

    /**
     * Math context to use in estimations calculations.
     */
    private final MathContext MATH_CONTEXT = MathContext.DECIMAL64;

    /**
     * Selectivity for closed range queries, in percent.
     */
    private final int RANGE_CLOSE_SELECTIVITY = 25;

    /**
     * Selectivity for open range queries, in percent.
     */
    private final int RANGE_OPEN_SELECTIVITY = 33;

    /**
     *  Nulls values fraction.
     *
     * @param colStat Column statistics or {@code null} if there is no statistics for column.
     * @return Null fraction estimation (by default or by statistics.
     */
    private double nulls(ColumnStatistics colStat) {
        if (colStat == null)
            return NULL_FRACTION;
        else {
            if (colStat.total() > 0)
                return (double)colStat.nulls() / colStat.total();
            else
                return 0;
        }
    }

    /**
     * Row cost calculation.
     *
     * @param ses Session.
     * @param filter Table filter.
     * @param masks Masks array.
     * @param rowCount Total rows count.
     * @param locTblStats Local table statistics.
     * @return Row cost.
     */
    private long rowCost(
        Session ses,
        TableFilter filter,
        int[] masks,
        long rowCount,
        ObjectStatisticsImpl locTblStats
    ) {
        double totalCardinality = 0;

        long rowsCost = rowCount;

        if (masks != null) {
            int i = 0, len = columns.length;

            while (i < len) {
                Column column = columns[i++];
                ColumnStatistics colStats = getColumnStatistics(locTblStats, column);

                int index = column.getColumnId();
                int mask = masks[index];

                if (isByteFlag(mask, IndexCondition.EQUALITY)) {
                    if (i == len && getIndexType().isUnique()) {
                        rowsCost = 3;

                        break;
                    }
                    // Estimate by is null
                    Value equalValue = getEqualValue(ses, column, filter);
                    Boolean equalNull = (equalValue == null) ? null : equalValue.getType() == Value.NULL;
                    rowCount = getColumnSize(colStats, rowCount, equalNull);

                    if (colStats != null && equalNull == Boolean.TRUE) {
                        rowsCost = Math.min(5 + Math.max(Math.round(rowsCost * nulls(colStats)), 1), rowsCost -
                            (i > 0 ? 1 : 0));
                        continue;
                    }
                    if (colStats != null && equalNull == Boolean.FALSE)
                        rowsCost = Math.max(Math.round(rowsCost * (1 - nulls(colStats))), 1);

                    long distinctRows;
                    if (colStats == null) {
                        double cardinality = (double)column.getSelectivity() / 100;
                        totalCardinality = 1 - (1 - totalCardinality) * (1 - cardinality);
                        distinctRows = Math.round((double) rowCount * totalCardinality);
                    }
                    else {
                        double cardinality;
                        long nonNulls = colStats.total() - colStats.nulls();
                        if (nonNulls == 0)
                            cardinality = 1;
                        else
                            cardinality = (double) colStats.distinct() / nonNulls;
                        totalCardinality = 1 - (1 - totalCardinality) * (1 - cardinality);
                        distinctRows = Math.round(rowCount * totalCardinality);
                    }

                    if (distinctRows <= 0)
                        distinctRows = 1;

                    rowsCost = Math.min(5 + Math.max(rowsCost / distinctRows, 1), rowsCost - (i > 0 ? 1 : 0));
                }
                else if (isByteFlag(mask, IndexCondition.RANGE)
                    || isByteFlag(mask, IndexCondition.START)
                    || isByteFlag(mask, IndexCondition.END)) {
                    Value min = getStartValue(ses, column, filter);
                    Value max = getEndValue(ses, column, filter);
                    int percent = estimatePercent(colStats, min, max);

                    rowsCost = Math.min(5 + rowsCost * percent / 100, rowsCost - (i > 0 ? 1 : 0));

                    break;
                }
                else if (isNullFilter(ses, column, filter)) {
                    if (colStats != null)
                        rowsCost = Math.min(5 + colStats.nulls(), rowsCost - (i > 0 ? 1 : 0));
                    break;
                }
                else if (isNotNullFilter(ses, column, filter)) {
                    if (colStats != null)
                        rowsCost = Math.min(5 + colStats.total() - colStats.nulls(), rowsCost - (i > 0 ? 1 : 0));
                    break;
                }
                else
                    break;
            }
        }
        return rowsCost;
    }

    /**
     * Get total number of values in column.
     *
     * @param colStats Column statistics.
     * @param rowCount Total row count in table.
     * @param nulls if {@code true} - try to estimate only nulls count,
     *              if {@code false} - try to estimate only non null count,
     *              if {@code null} - try to estimate total count of values.
     * @return Column value count.
     */
    private long getColumnSize(@Nullable ColumnStatistics colStats, long rowCount, Boolean nulls) {
        if (colStats == null)
            return rowCount;
        else if (nulls == null)
            return colStats.total();
        else if (nulls)
            return colStats.nulls();
        else
            return colStats.total() - colStats.nulls();
    }

    /**
     * Get constant value if there are clause with equal condition for specified column.
     *
     * @param ses Session.
     * @param column Column to get value by.
     * @param filter Table filter.
     * @return "Equal" value or {@code null} if there are no equal clause with constant expression.
     */
    private Value getEqualValue(Session ses, Column column, TableFilter filter) {
        Value maxValue = null;
        for (IndexCondition cond : filter.getIndexConditions()) {
            if (!column.equals(cond.getColumn()))
                continue;

            if (isByteFlag(cond.getCompareType(), Comparison.EQUAL) && cond.isEvaluatable()) {

                Expression expr = cond.getExpression();
                if (expr != null && expr.isConstant()) {
                    Value curVal = cond.getCurrentValue(ses);
                    if (null == maxValue || (curVal != null || filter.getTable().compareTypeSafe(curVal, maxValue) < 0))
                        maxValue = curVal;
                }
            }
        }
        return maxValue;
    }

    /**
     * Get "start" value - constant for "bigger" or "bigger or equals" clause.
     *
     * @param ses Session.
     * @param column Column to get value by.
     * @param filter Table filter.
     * @return "Start" value or {@code null} if there are no such clause with constant expression.
     */
    private Value getStartValue(Session ses, Column column, TableFilter filter) {
        if (filter == null)
            return null;
        Value maxValue = null;
        for (IndexCondition cond : filter.getIndexConditions()) {
            if (!column.equals(cond.getColumn()))
                continue;

            if ((isByteFlag(cond.getCompareType(), Comparison.BIGGER)
                || isByteFlag(cond.getCompareType(), Comparison.BIGGER_EQUAL))
                && cond.isEvaluatable()) {

                Expression expr = cond.getExpression();
                if (expr != null && expr.isConstant()) {
                    Value curVal = cond.getCurrentValue(ses);
                    if (null == maxValue || (curVal != null || filter.getTable().compareTypeSafe(curVal, maxValue) < 0))
                        maxValue = curVal;
                }
            }
        }
        return maxValue;
    }

    /**
     * Get "end" value - constant for "smaller" or "smaller or equal" clause.
     *
     * @param ses Session.
     * @param column Column to get value by.
     * @param filter Table filter.
     * @return "End" value of {@code null} if there are no such clause with constant expression.
     */
    private Value getEndValue(Session ses, Column column, TableFilter filter) {
        if (filter == null)
            return null;
        Value minValue = null;
        for (IndexCondition cond : filter.getIndexConditions()) {
            if (!column.equals(cond.getColumn()))
                continue;

            if ((isByteFlag(cond.getCompareType(), Comparison.SMALLER)
                || isByteFlag(cond.getCompareType(), Comparison.SMALLER_EQUAL))
                && cond.isEvaluatable()) {
                Expression expr = cond.getExpression();
                if (expr != null && expr.isConstant()) {
                    Value curVal = cond.getCurrentValue(ses);
                    if (null == minValue || (curVal != null || filter.getTable().compareTypeSafe(minValue, curVal) < 0))
                        minValue = curVal;
                }
            }
        }
        return minValue;
    }

    /**
     * Check if specified filter compare specified column to not null.
     *
     * @param ses Session to resolve values.
     * @param column Column to check.
     * @param filter Table filter.
     * @return {@code true} if column value should be null, {@code falce} otherwise (or if it not sure).
     */
    private boolean isNotNullFilter(Session ses, Column column, TableFilter filter) {
        // TODO:  IGNITE-14813 check not null expression (TableFilter contains only fullCondition without getter to check it)
        return false;
    }

    /**
     * Check if specified filter compare specified column to null.
     *
     * @param ses Session to resolve values.
     * @param column Column to check.
     * @param filter Table filter.
     * @return {@code true} if column value should be null, {@code falce} otherwise (or if it not sure).
     */
    private boolean isNullFilter(Session ses, Column column, TableFilter filter) {
        if (filter == null)
            return false;
        for (IndexCondition cond : filter.getIndexConditions()) {
            if (column.equals(cond.getColumn()))
                continue;

            if (isByteFlag(cond.getCompareType(), Comparison.SPATIAL_INTERSECTS) && cond.isEvaluatable()) {
                Expression expr = cond.getExpression();

                if (expr != null && expr.isConstant()) {
                    Value curVal = cond.getCurrentValue(ses);

                    if (curVal != null && curVal.getType() == Value.NULL)
                        return true;
                }
            }
        }
        return false;
    }

    /**
     * Test if value contains all masks bits.
     *
     * @param value Value to test.
     * @param mask Mask to test by.
     * @return {@code true} if value contains all necessary bits, {@code false} otherwise.
     */
    private boolean isByteFlag(int value, int mask) {
        return (value & mask) == mask;
    }

    /**
     * Estimate percent of selected rows by specified min/max conditions (of total rows, with nulls).
     *
     * @param colStat Column statistics to use, if exists.
     * @param min The lower border.
     * @param max The higher border.
     * @return Percent of total rows, selected with specified conditions (0-100).
     */
    private int estimatePercent(ColumnStatistics colStat, Value min, Value max) {
        if (colStat == null || colStat.min() == null || colStat.max() == null)
            // Fall back to previous behaviour without statistics, even without min/max testing
            return estimatePercentFallback(min, max);

        BigDecimal minValue = (min == null) ? null : getComparableValue(min);
        BigDecimal maxValue = (max == null) ? null : getComparableValue(max);

        if (minValue == null && maxValue == null)
            return estimatePercentFallback(min, max);

        BigDecimal minStat = getComparableValue(colStat.min());
        BigDecimal maxStat = getComparableValue(colStat.max());

        if (minStat == null || maxStat == null)
            return estimatePercentFallback(min, max);

        BigDecimal start = (minValue == null || minValue.compareTo(minStat) < 0) ? minStat : minValue;
        BigDecimal end = (maxValue == null || maxValue.compareTo(maxStat) > 0) ? maxStat : maxValue;

        BigDecimal actual = end.subtract(start);

        if (actual.signum() < 0)
            return 0;

        BigDecimal total = maxStat.subtract(minStat);

        if (total.signum() < 0)
            return estimatePercentFallback(min, max);

        // If one select from column with exactly one (same for all rows) value - all rows will be selected if
        // the border is equal to that single value
        if (total.signum() == 0)
            return (minStat.equals(start)) ? 100 - (int)Math.round(nulls(colStat) * 100) : 0;

        // 1) actual range divided by total range to get simple piece of table (selecting values part, 0-1)
        // 2) taking into account nulls by multiplying by percent of non null values: (100 - null)/100
        // 3) but we need result in percent, so instead of multiplying it by 100 just remove division by 100 from second step
        int result = actual.multiply(BigDecimal.valueOf(100 - colStat.nulls())).divide(total, MATH_CONTEXT).intValue();
        return result > 100 ? 100 : result;
    }

    /**
     * Fallback percent estimation.
     *
     * @param min Min border.
     * @param max Max border.
     * @return Percent estimation of returning rows.
     */
    private int estimatePercentFallback(Value min, Value max) {
        return (min == null || max == null) ? RANGE_OPEN_SELECTIVITY : RANGE_CLOSE_SELECTIVITY;
    }

    /**
     * Convert specified value into comparable type: BigDecimal,
     *
     * @param value Value to convert to comparable form.
     * @return Comparable form of value.
     */
    private BigDecimal getComparableValue(Value value) {
        switch (value.getType()) {
            case Value.NULL:
                throw new IllegalArgumentException("Can't compare null values");

            case Value.BOOLEAN:
                return new BigDecimal(value.getBoolean() ? 1 : 0);

            case Value.BYTE:
                return new BigDecimal(value.getByte());

            case Value.SHORT:
                return new BigDecimal(value.getShort());

            case Value.INT:
                return new BigDecimal(value.getInt());

            case Value.LONG:
                return new BigDecimal(value.getLong());

            case Value.DECIMAL:
                return value.getBigDecimal();

            case Value.DOUBLE:
                return new BigDecimal(value.getDouble());

            case Value.FLOAT:
                return new BigDecimal(value.getFloat());

            case Value.DATE:
                return new BigDecimal(value.getDate().getTime());

            case Value.TIME:
                return new BigDecimal(value.getTime().getTime());

            case Value.TIMESTAMP:
                return new BigDecimal(value.getTimestamp().getTime());

            case Value.BYTES:
                BigInteger bigInteger = new BigInteger(1, value.getBytes());
                return new BigDecimal(bigInteger);

            case Value.STRING:
            case Value.STRING_FIXED:
            case Value.STRING_IGNORECASE:
            case Value.ARRAY:
            case Value.JAVA_OBJECT:
            case Value.GEOMETRY:
                return null;

            case Value.UUID:
                BigInteger bigInt = new BigInteger(1, value.getBytes());
                return new BigDecimal(bigInt);

            default:
                throw new IllegalStateException("Unsupported H2 type: " + value.getType());
        }
    }

    /**
     * Get column statistics.
     *
     * @param locTblStats Whole table statistics, can be {@code null}.
     * @param column Column to get statistics by.
     * @return Column statistics or {@code null}.
     */
    private ColumnStatistics getColumnStatistics(@Nullable ObjectStatisticsImpl locTblStats, Column column) {
        return (locTblStats == null) ? null : locTblStats.columnStatistics(column.getName());
    }

    /**
     * Estimate sorting cost.
     *
     * @param rowCount Total rows count.
     * @param filters Filters array.
     * @param filter Column filter index.
     * @param sortOrder Sort order.
     * @param isScanIndex Flag if current index is a scan index.
     * @return Sorting cost.
     */
    private long sortingCost(
        long rowCount,
        TableFilter[] filters,
        int filter,
        SortOrder sortOrder,
        boolean isScanIndex
    ) {
        if (sortOrder == null)
            return 0;

        long sortingCost = 100 + rowCount / 10;

        if (!isScanIndex) {
            boolean sortOrderMatches = true;
            int coveringCount = 0;
            int[] sortTypes = sortOrder.getSortTypes();

            TableFilter tableFilter = filters == null ? null : filters[filter];

            for (int i = 0, len = sortTypes.length; i < len; i++) {
                if (i >= indexColumns.length) {
                    // We can still use this index if we are sorting by more
                    // than it's columns, it's just that the coveringCount
                    // is lower than with an index that contains
                    // more of the order by columns.
                    break;
                }

                Column col = sortOrder.getColumn(i, tableFilter);

                if (col == null) {
                    sortOrderMatches = false;

                    break;
                }

                IndexColumn indexCol = indexColumns[i];

                if (!col.equals(indexCol.column)) {
                    sortOrderMatches = false;

                    break;
                }

                int sortType = sortTypes[i];

                if (sortType != indexCol.sortType) {
                    sortOrderMatches = false;

                    break;
                }

                coveringCount++;
            }

            if (sortOrderMatches)
                // "coveringCount" makes sure that when we have two
                // or more covering indexes, we choose the one
                // that covers more.
                sortingCost = 100 - coveringCount;
        }
        return sortingCost;
    }

    /**
     * Get cost range.
     *
     * @param ses Session.
     * @param masks Condition masks.
     * @param rowCount Total row count.
     * @param filters Filters array.
     * @param filter Filter array index.
     * @param sortOrder Sort order.
     * @param isScanIndex Flag if current index is a scan index.
     * @param allColumnsSet All columns to select.
     * @return The cost.
     */
    public long getCostRangeIndex(
        Session ses,
        int[] masks,
        long rowCount,
        TableFilter[] filters,
        int filter,
        SortOrder sortOrder,
        boolean isScanIndex,
        HashSet<Column> allColumnsSet
    ) {
        ObjectStatisticsImpl locTblStats = (ObjectStatisticsImpl) tbl.tableStatistics();

        if (locTblStats != null)
            rowCount = locTblStats.rowCount();

        // Small increment to account statistics outdates.
        rowCount += 1000;

        TableFilter tableFilter = (filters == null) ? null : filters[filter];

        long rowsCost = rowCost(ses, tableFilter, masks, rowCount, locTblStats);

        // If the ORDER BY clause matches the ordering of this index,
        // it will be cheaper than another index, so adjust the cost
        // accordingly.
        long sortingCost = sortingCost(rowCount, filters, filter, sortOrder, isScanIndex);

        boolean skipColumnsIntersection = false;

        if (filters != null && tableFilter != null && columns != null) {
            skipColumnsIntersection = true;

            ArrayList<IndexCondition> idxConds = tableFilter.getIndexConditions();

            // Only pk with _key used.
            if (F.isEmpty(idxConds))
                skipColumnsIntersection = false;

            for (IndexCondition cond : idxConds) {
                if (cond.getColumn() == columns[0]) {
                    skipColumnsIntersection = false;

                    break;
                }
            }
        }

        // If we have two indexes with the same cost, and one of the indexes can
        // satisfy the query without needing to read from the primary table
        // (scan index), make that one slightly lower cost.
        boolean needsToReadFromScanIndex = true;

        if (!isScanIndex && allColumnsSet != null && !skipColumnsIntersection && !allColumnsSet.isEmpty()) {
            boolean foundAllColumnsWeNeed = true;

            for (Column c : allColumnsSet) {
                boolean found = false;

                for (Column c2 : columns) {
                    if (c == c2) {
                        found = true;

                        break;
                    }
                }

                if (!found) {
                    foundAllColumnsWeNeed = false;

                    break;
                }
            }

            if (foundAllColumnsWeNeed)
                needsToReadFromScanIndex = false;
        }

        long rc;

        if (isScanIndex)
            rc = rowsCost + sortingCost + 20;
        else if (needsToReadFromScanIndex)
            rc = rowsCost + rowsCost + sortingCost + 20;
        else
            // The (20-x) calculation makes sure that when we pick a covering
            // index, we pick the covering index that has the smallest number of
            // columns (the more columns we have in index - the higher cost).
            // This is faster because a smaller index will fit into fewer data
            // blocks.
            rc = rowsCost + sortingCost + columns.length;

        return rc;
    }
}
