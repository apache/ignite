/*
 * Copyright 2004-2018 H2 Group. Multiple-Licensed under the MPL 2.0,
 * and the EPL 1.0 (http://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.h2.expression;

import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Comparator;
import java.util.HashSet;

import org.h2.engine.Database;
import org.h2.engine.Session;
import org.h2.index.Cursor;
import org.h2.index.Index;
import org.h2.result.SearchRow;
import org.h2.result.SortOrder;
import org.h2.table.Column;
import org.h2.table.IndexColumn;
import org.h2.table.Table;
import org.h2.table.TableFilter;
import org.h2.util.DateTimeUtils;
import org.h2.value.CompareMode;
import org.h2.value.Value;
import org.h2.value.ValueDate;
import org.h2.value.ValueDecimal;
import org.h2.value.ValueDouble;
import org.h2.value.ValueFloat;
import org.h2.value.ValueInt;
import org.h2.value.ValueLong;
import org.h2.value.ValueNull;
import org.h2.value.ValueTime;
import org.h2.value.ValueTimestamp;
import org.h2.value.ValueTimestampTimeZone;

/**
 * Data stored while calculating a MEDIAN aggregate.
 */
class AggregateDataMedian extends AggregateData {
    private Collection<Value> values;

    private static boolean isNullsLast(Index index) {
        IndexColumn ic = index.getIndexColumns()[0];
        int sortType = ic.sortType;
        return (sortType & SortOrder.NULLS_LAST) != 0
                || (sortType & SortOrder.DESCENDING) != 0 && (sortType & SortOrder.NULLS_FIRST) == 0;
    }

    /**
     * Get the index (if any) for the column specified in the median aggregate.
     *
     * @param on the expression (usually a column expression)
     * @return the index, or null
     */
    static Index getMedianColumnIndex(Expression on) {
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
     * Get the result from the index.
     *
     * @param session the session
     * @param on the expression
     * @param dataType the data type
     * @return the result
     */
    static Value getResultFromIndex(Session session, Expression on, int dataType) {
        Index index = getMedianColumnIndex(on);
        long count = index.getRowCount(session);
        if (count == 0) {
            return ValueNull.INSTANCE;
        }
        Cursor cursor = index.find(session, null, null);
        cursor.next();
        int columnId = index.getColumns()[0].getColumnId();
        ExpressionColumn expr = (ExpressionColumn) on;
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
        long skip = (count - 1) / 2;
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
        if ((count & 1) == 0) {
            cursor.next();
            row = cursor.getSearchRow();
            if (row == null) {
                return v;
            }
            Value v2 = row.getValue(columnId);
            if (v2 == ValueNull.INSTANCE) {
                return v;
            }
            return getMedian(v, v2, dataType, session.getDatabase().getCompareMode());
        }
        return v;
    }

    @Override
    void add(Database database, int dataType, boolean distinct, Value v) {
        if (v == ValueNull.INSTANCE) {
            return;
        }
        Collection<Value> c = values;
        if (c == null) {
            values = c = distinct ? new HashSet<Value>() : new ArrayList<Value>();
        }
        c.add(v);
    }

    @Override
    Value getValue(Database database, int dataType, boolean distinct) {
        Collection<Value> c = values;
        // Non-null collection cannot be empty here
        if (c == null) {
            return ValueNull.INSTANCE;
        }
        if (distinct && c instanceof ArrayList) {
            c = new HashSet<>(c);
        }
        Value[] a = c.toArray(new Value[0]);
        final CompareMode mode = database.getCompareMode();
        Arrays.sort(a, new Comparator<Value>() {
            @Override
            public int compare(Value o1, Value o2) {
                return o1.compareTo(o2, mode);
            }
        });
        int len = a.length;
        int idx = len / 2;
        Value v1 = a[idx];
        if ((len & 1) == 1) {
            return v1.convertTo(dataType);
        }
        return getMedian(a[idx - 1], v1, dataType, mode);
    }

    private static Value getMedian(Value v0, Value v1, int dataType, CompareMode mode) {
        if (v0.compareTo(v1, mode) == 0) {
            return v0.convertTo(dataType);
        }
        switch (dataType) {
        case Value.BYTE:
        case Value.SHORT:
        case Value.INT:
            return ValueInt.get((v0.getInt() + v1.getInt()) / 2).convertTo(dataType);
        case Value.LONG:
            return ValueLong.get((v0.getLong() + v1.getLong()) / 2);
        case Value.DECIMAL:
            return ValueDecimal.get(v0.getBigDecimal().add(v1.getBigDecimal()).divide(BigDecimal.valueOf(2)));
        case Value.FLOAT:
            return ValueFloat.get((v0.getFloat() + v1.getFloat()) / 2);
        case Value.DOUBLE:
            return ValueDouble.get((v0.getFloat() + v1.getDouble()) / 2);
        case Value.TIME: {
            ValueTime t0 = (ValueTime) v0.convertTo(Value.TIME), t1 = (ValueTime) v1.convertTo(Value.TIME);
            return ValueTime.fromNanos((t0.getNanos() + t1.getNanos()) / 2);
        }
        case Value.DATE: {
            ValueDate d0 = (ValueDate) v0.convertTo(Value.DATE), d1 = (ValueDate) v1.convertTo(Value.DATE);
            return ValueDate.fromDateValue(
                    DateTimeUtils.dateValueFromAbsoluteDay((DateTimeUtils.absoluteDayFromDateValue(d0.getDateValue())
                            + DateTimeUtils.absoluteDayFromDateValue(d1.getDateValue())) / 2));
        }
        case Value.TIMESTAMP: {
            ValueTimestamp ts0 = (ValueTimestamp) v0.convertTo(Value.TIMESTAMP),
                    ts1 = (ValueTimestamp) v1.convertTo(Value.TIMESTAMP);
            long dateSum = DateTimeUtils.absoluteDayFromDateValue(ts0.getDateValue())
                    + DateTimeUtils.absoluteDayFromDateValue(ts1.getDateValue());
            long nanos = (ts0.getTimeNanos() + ts1.getTimeNanos()) / 2;
            if ((dateSum & 1) != 0) {
                nanos += DateTimeUtils.NANOS_PER_DAY / 2;
                if (nanos >= DateTimeUtils.NANOS_PER_DAY) {
                    nanos -= DateTimeUtils.NANOS_PER_DAY;
                    dateSum++;
                }
            }
            return ValueTimestamp.fromDateValueAndNanos(DateTimeUtils.dateValueFromAbsoluteDay(dateSum / 2), nanos);
        }
        case Value.TIMESTAMP_TZ: {
            ValueTimestampTimeZone ts0 = (ValueTimestampTimeZone) v0.convertTo(Value.TIMESTAMP_TZ),
                    ts1 = (ValueTimestampTimeZone) v1.convertTo(Value.TIMESTAMP_TZ);
            long dateSum = DateTimeUtils.absoluteDayFromDateValue(ts0.getDateValue())
                    + DateTimeUtils.absoluteDayFromDateValue(ts1.getDateValue());
            long nanos = (ts0.getTimeNanos() + ts1.getTimeNanos()) / 2;
            int offset = ts0.getTimeZoneOffsetMins() + ts1.getTimeZoneOffsetMins();
            if ((dateSum & 1) != 0) {
                nanos += DateTimeUtils.NANOS_PER_DAY / 2;
            }
            if ((offset & 1) != 0) {
                nanos += 30_000_000_000L;
            }
            if (nanos >= DateTimeUtils.NANOS_PER_DAY) {
                nanos -= DateTimeUtils.NANOS_PER_DAY;
                dateSum++;
            }
            return ValueTimestampTimeZone.fromDateValueAndNanos(DateTimeUtils.dateValueFromAbsoluteDay(dateSum / 2),
                    nanos, (short) (offset / 2));
        }
        default:
            // Just return first
            return v0.convertTo(dataType);
        }
    }

}
