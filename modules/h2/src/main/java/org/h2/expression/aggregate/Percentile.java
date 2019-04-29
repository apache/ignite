/*
 * Copyright 2004-2019 H2 Group. Multiple-Licensed under the MPL 2.0,
 * and the EPL 1.0 (http://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.h2.expression.aggregate;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.util.ArrayList;
import java.util.Arrays;

import org.h2.api.IntervalQualifier;
import org.h2.command.dml.SelectOrderBy;
import org.h2.engine.Database;
import org.h2.engine.Mode;
import org.h2.engine.Session;
import org.h2.engine.SysProperties;
import org.h2.expression.Expression;
import org.h2.expression.ExpressionColumn;
import org.h2.index.Cursor;
import org.h2.index.Index;
import org.h2.result.SearchRow;
import org.h2.result.SortOrder;
import org.h2.table.Column;
import org.h2.table.IndexColumn;
import org.h2.table.Table;
import org.h2.table.TableFilter;
import org.h2.util.DateTimeUtils;
import org.h2.util.IntervalUtils;
import org.h2.value.CompareMode;
import org.h2.value.Value;
import org.h2.value.ValueDate;
import org.h2.value.ValueDecimal;
import org.h2.value.ValueInterval;
import org.h2.value.ValueNull;
import org.h2.value.ValueTime;
import org.h2.value.ValueTimestamp;
import org.h2.value.ValueTimestampTimeZone;

/**
 * PERCENTILE_CONT, PERCENTILE_DISC, and MEDIAN inverse distribution functions.
 */
final class Percentile {

    /**
     * BigDecimal value of 0.5.
     */
    static final BigDecimal HALF = BigDecimal.valueOf(0.5d);

    private static boolean isNullsLast(Index index) {
        IndexColumn ic = index.getIndexColumns()[0];
        int sortType = ic.sortType;
        return (sortType & SortOrder.NULLS_LAST) != 0
                || (sortType & SortOrder.NULLS_FIRST) == 0
                        && (sortType & SortOrder.DESCENDING) != 0 ^ SysProperties.SORT_NULLS_HIGH;
    }

    /**
     * Get the index (if any) for the column specified in the inverse
     * distribution function.
     *
     * @param on the expression (usually a column expression)
     * @return the index, or null
     */
    static Index getColumnIndex(Expression on) {
        if (on instanceof ExpressionColumn) {
            ExpressionColumn col = (ExpressionColumn) on;
            Column column = col.getColumn();
            TableFilter filter = col.getTableFilter();
            if (filter != null) {
                Table table = filter.getTable();
                ArrayList<Index> indexes = table.getIndexes();
                Index result = null;
                if (indexes != null) {
                    boolean nullable = column.isNullable();
                    for (int i = 1, size = indexes.size(); i < size; i++) {
                        Index index = indexes.get(i);
                        if (!index.canFindNext()) {
                            continue;
                        }
                        if (!index.isFirstColumn(column)) {
                            continue;
                        }
                        // Prefer index without nulls last for nullable columns
                        if (result == null || result.getColumns().length > index.getColumns().length
                                || nullable && isNullsLast(result) && !isNullsLast(index)) {
                            result = index;
                        }
                    }
                }
                return result;
            }
        }
        return null;
    }

    /**
     * Get the result from the array of values.
     *
     * @param database the database
     * @param array array with values
     * @param dataType the data type
     * @param orderByList ORDER BY list
     * @param percentile argument of percentile function, or 0.5d for median
     * @param interpolate whether value should be interpolated
     * @return the result
     */
    static Value getValue(Database database, Value[] array, int dataType, ArrayList<SelectOrderBy> orderByList,
            BigDecimal percentile, boolean interpolate) {
        final CompareMode compareMode = database.getCompareMode();
        Arrays.sort(array, compareMode);
        int count = array.length;
        boolean reverseIndex = orderByList != null && (orderByList.get(0).sortType & SortOrder.DESCENDING) != 0;
        BigDecimal fpRow = BigDecimal.valueOf(count - 1).multiply(percentile);
        int rowIdx1 = fpRow.intValue();
        BigDecimal factor = fpRow.subtract(BigDecimal.valueOf(rowIdx1));
        int rowIdx2;
        if (factor.signum() == 0) {
            interpolate = false;
            rowIdx2 = rowIdx1;
        } else {
            rowIdx2 = rowIdx1 + 1;
            if (!interpolate) {
                if (factor.compareTo(HALF) > 0) {
                    rowIdx1 = rowIdx2;
                } else {
                    rowIdx2 = rowIdx1;
                }
            }
        }
        if (reverseIndex) {
            rowIdx1 = count - 1 - rowIdx1;
            rowIdx2 = count - 1 - rowIdx2;
        }
        Value v = array[rowIdx1];
        if (!interpolate) {
            return v.convertTo(dataType);
        }
        return interpolate(v, array[rowIdx2], factor, dataType, database.getMode(), compareMode);
    }

    /**
     * Get the result from the index.
     *
     * @param session the session
     * @param expression the expression
     * @param dataType the data type
     * @param orderByList ORDER BY list
     * @param percentile argument of percentile function, or 0.5d for median
     * @param interpolate whether value should be interpolated
     * @return the result
     */
    static Value getFromIndex(Session session, Expression expression, int dataType,
            ArrayList<SelectOrderBy> orderByList, BigDecimal percentile, boolean interpolate) {
        Index index = getColumnIndex(expression);
        long count = index.getRowCount(session);
        if (count == 0) {
            return ValueNull.INSTANCE;
        }
        Cursor cursor = index.find(session, null, null);
        cursor.next();
        int columnId = index.getColumns()[0].getColumnId();
        ExpressionColumn expr = (ExpressionColumn) expression;
        if (expr.getColumn().isNullable()) {
            boolean hasNulls = false;
            SearchRow row;
            // Try to skip nulls from the start first with the same cursor that
            // will be used to read values.
            while (count > 0) {
                row = cursor.getSearchRow();
                if (row == null) {
                    return ValueNull.INSTANCE;
                }
                if (row.getValue(columnId) == ValueNull.INSTANCE) {
                    count--;
                    cursor.next();
                    hasNulls = true;
                } else {
                    break;
                }
            }
            if (count == 0) {
                return ValueNull.INSTANCE;
            }
            // If no nulls found and if index orders nulls last create a second
            // cursor to count nulls at the end.
            if (!hasNulls && isNullsLast(index)) {
                TableFilter tableFilter = expr.getTableFilter();
                SearchRow check = tableFilter.getTable().getTemplateSimpleRow(true);
                check.setValue(columnId, ValueNull.INSTANCE);
                Cursor nullsCursor = index.find(session, check, check);
                while (nullsCursor.next()) {
                    count--;
                }
                if (count <= 0) {
                    return ValueNull.INSTANCE;
                }
            }
        }
        boolean reverseIndex = (orderByList != null ? orderByList.get(0).sortType & SortOrder.DESCENDING : 0)
                != (index.getIndexColumns()[0].sortType & SortOrder.DESCENDING);
        BigDecimal fpRow = BigDecimal.valueOf(count - 1).multiply(percentile);
        long rowIdx1 = fpRow.longValue();
        BigDecimal factor = fpRow.subtract(BigDecimal.valueOf(rowIdx1));
        long rowIdx2;
        if (factor.signum() == 0) {
            interpolate = false;
            rowIdx2 = rowIdx1;
        } else {
            rowIdx2 = rowIdx1 + 1;
            if (!interpolate) {
                if (factor.compareTo(HALF) > 0) {
                    rowIdx1 = rowIdx2;
                } else {
                    rowIdx2 = rowIdx1;
                }
            }
        }
        long skip = reverseIndex ? count - 1 - rowIdx2 : rowIdx1;
        for (int i = 0; i < skip; i++) {
            cursor.next();
        }
        SearchRow row = cursor.getSearchRow();
        if (row == null) {
            return ValueNull.INSTANCE;
        }
        Value v = row.getValue(columnId);
        if (v == ValueNull.INSTANCE) {
            return v;
        }
        if (interpolate) {
            cursor.next();
            row = cursor.getSearchRow();
            if (row == null) {
                return v;
            }
            Value v2 = row.getValue(columnId);
            if (v2 == ValueNull.INSTANCE) {
                return v;
            }
            Database database = session.getDatabase();
            if (reverseIndex) {
                Value t = v;
                v = v2;
                v2 = t;
            }
            return interpolate(v, v2, factor, dataType, database.getMode(), database.getCompareMode());
        }
        return v.convertTo(dataType);
    }

    private static Value interpolate(Value v0, Value v1, BigDecimal factor, int dataType, Mode databaseMode,
            CompareMode compareMode) {
        if (v0.compareTo(v1, databaseMode, compareMode) == 0) {
            return v0.convertTo(dataType);
        }
        switch (dataType) {
        case Value.BYTE:
        case Value.SHORT:
        case Value.INT:
            return ValueDecimal.get(
                    interpolateDecimal(BigDecimal.valueOf(v0.getInt()), BigDecimal.valueOf(v1.getInt()), factor));
        case Value.LONG:
            return ValueDecimal.get(
                    interpolateDecimal(BigDecimal.valueOf(v0.getLong()), BigDecimal.valueOf(v1.getLong()), factor));
        case Value.DECIMAL:
            return ValueDecimal.get(interpolateDecimal(v0.getBigDecimal(), v1.getBigDecimal(), factor));
        case Value.FLOAT:
        case Value.DOUBLE:
            return ValueDecimal.get(
                    interpolateDecimal(
                            BigDecimal.valueOf(v0.getDouble()), BigDecimal.valueOf(v1.getDouble()), factor));
        case Value.TIME: {
            ValueTime t0 = (ValueTime) v0.convertTo(Value.TIME), t1 = (ValueTime) v1.convertTo(Value.TIME);
            BigDecimal n0 = BigDecimal.valueOf(t0.getNanos());
            BigDecimal n1 = BigDecimal.valueOf(t1.getNanos());
            return ValueTime.fromNanos(interpolateDecimal(n0, n1, factor).longValue());
        }
        case Value.DATE: {
            ValueDate d0 = (ValueDate) v0.convertTo(Value.DATE), d1 = (ValueDate) v1.convertTo(Value.DATE);
            BigDecimal a0 = BigDecimal.valueOf(DateTimeUtils.absoluteDayFromDateValue(d0.getDateValue()));
            BigDecimal a1 = BigDecimal.valueOf(DateTimeUtils.absoluteDayFromDateValue(d1.getDateValue()));
            return ValueDate.fromDateValue(
                    DateTimeUtils.dateValueFromAbsoluteDay(interpolateDecimal(a0, a1, factor).longValue()));
        }
        case Value.TIMESTAMP: {
            ValueTimestamp ts0 = (ValueTimestamp) v0.convertTo(Value.TIMESTAMP),
                    ts1 = (ValueTimestamp) v1.convertTo(Value.TIMESTAMP);
            BigDecimal a0 = timestampToDecimal(ts0.getDateValue(), ts0.getTimeNanos());
            BigDecimal a1 = timestampToDecimal(ts1.getDateValue(), ts1.getTimeNanos());
            BigInteger[] dr = interpolateDecimal(a0, a1, factor).toBigInteger()
                    .divideAndRemainder(IntervalUtils.NANOS_PER_DAY_BI);
            long absoluteDay = dr[0].longValue();
            long timeNanos = dr[1].longValue();
            if (timeNanos < 0) {
                timeNanos += DateTimeUtils.NANOS_PER_DAY;
                absoluteDay--;
            }
            return ValueTimestamp.fromDateValueAndNanos(
                    DateTimeUtils.dateValueFromAbsoluteDay(absoluteDay), timeNanos);
        }
        case Value.TIMESTAMP_TZ: {
            ValueTimestampTimeZone ts0 = (ValueTimestampTimeZone) v0.convertTo(Value.TIMESTAMP_TZ),
                    ts1 = (ValueTimestampTimeZone) v1.convertTo(Value.TIMESTAMP_TZ);
            BigDecimal a0 = timestampToDecimal(ts0.getDateValue(), ts0.getTimeNanos());
            BigDecimal a1 = timestampToDecimal(ts1.getDateValue(), ts1.getTimeNanos());
            BigDecimal offset = BigDecimal.valueOf(ts0.getTimeZoneOffsetMins())
                    .multiply(BigDecimal.ONE.subtract(factor))
                    .add(BigDecimal.valueOf(ts1.getTimeZoneOffsetMins()).multiply(factor));
            short shortOffset = offset.shortValue();
            BigDecimal shortOffsetBD = BigDecimal.valueOf(shortOffset);
            BigDecimal bd = interpolateDecimal(a0, a1, factor);
            if (offset.compareTo(shortOffsetBD) != 0) {
                bd = bd.add(
                        offset.subtract(shortOffsetBD).multiply(BigDecimal.valueOf(DateTimeUtils.NANOS_PER_MINUTE)));
            }
            BigInteger[] dr = bd.toBigInteger().divideAndRemainder(IntervalUtils.NANOS_PER_DAY_BI);
            long absoluteDay = dr[0].longValue();
            long timeNanos = dr[1].longValue();
            if (timeNanos < 0) {
                timeNanos += DateTimeUtils.NANOS_PER_DAY;
                absoluteDay--;
            }
            return ValueTimestampTimeZone.fromDateValueAndNanos(DateTimeUtils.dateValueFromAbsoluteDay(absoluteDay),
                    timeNanos, shortOffset);
        }
        case Value.INTERVAL_YEAR:
        case Value.INTERVAL_MONTH:
        case Value.INTERVAL_DAY:
        case Value.INTERVAL_HOUR:
        case Value.INTERVAL_MINUTE:
        case Value.INTERVAL_SECOND:
        case Value.INTERVAL_YEAR_TO_MONTH:
        case Value.INTERVAL_DAY_TO_HOUR:
        case Value.INTERVAL_DAY_TO_MINUTE:
        case Value.INTERVAL_DAY_TO_SECOND:
        case Value.INTERVAL_HOUR_TO_MINUTE:
        case Value.INTERVAL_HOUR_TO_SECOND:
        case Value.INTERVAL_MINUTE_TO_SECOND:
            return IntervalUtils.intervalFromAbsolute(IntervalQualifier.valueOf(dataType - Value.INTERVAL_YEAR),
                    interpolateDecimal(new BigDecimal(IntervalUtils.intervalToAbsolute((ValueInterval) v0)),
                            new BigDecimal(IntervalUtils.intervalToAbsolute((ValueInterval) v1)), factor)
                                    .toBigInteger());
        default:
            // Use the same rules as PERCENTILE_DISC
            return (factor.compareTo(HALF) > 0 ? v1 : v0).convertTo(dataType);
        }
    }

    private static BigDecimal timestampToDecimal(long dateValue, long timeNanos) {
        return new BigDecimal(BigInteger.valueOf(DateTimeUtils.absoluteDayFromDateValue(dateValue))
                .multiply(IntervalUtils.NANOS_PER_DAY_BI).add(BigInteger.valueOf(timeNanos)));
    }

    private static BigDecimal interpolateDecimal(BigDecimal d0, BigDecimal d1, BigDecimal factor) {
        return d0.multiply(BigDecimal.ONE.subtract(factor)).add(d1.multiply(factor));
    }

    private Percentile() {
    }

}
