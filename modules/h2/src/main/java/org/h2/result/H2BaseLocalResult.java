/*
 * Copyright 2004-2019 H2 Group. Multiple-Licensed under the MPL 2.0,
 * and the EPL 1.0 (http://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */

package org.h2.result;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.TreeMap;
import org.h2.engine.Session;
import org.h2.engine.SessionInterface;
import org.h2.expression.Expression;
import org.h2.message.DbException;
import org.h2.util.Utils;
import org.h2.value.TypeInfo;
import org.h2.value.Value;
import org.h2.value.ValueRow;

/** */
//TODO: GG-18632 Add external result and merge with {@link org.h2.result.LocalResultImpl}.
public class H2BaseLocalResult implements LocalResult {
    private Session session;
    private int visibleColumnCount;
    private Expression[] expressions;
    private int rowId, rowCount;
    protected ArrayList<Value[]> rows;
    private SortOrder sort;
    protected TreeMap<Value, Value[]> distinctRows;
    private Value[] currentRow;
    private int offset;
    private int limit = -1;
    private boolean fetchPercent;
    private SortOrder withTiesSortOrder;
    private boolean limitsWereApplied;
    private boolean distinct;
    private int[] distinctIndexes;
    private boolean closed;
    private boolean containsLobs;
    private Boolean containsNull;

    /**
     * Construct a local result object.
     */
    public H2BaseLocalResult() {
        // nothing to do
    }

    /**
     * Construct a local result object.
     *
     * @param session the session
     * @param expressions the expression array
     * @param visibleColumnCount the number of visible columns
     */
    public H2BaseLocalResult(Session session, Expression[] expressions,
        int visibleColumnCount) {
        this.session = session;
        rows = Utils.newSmallArrayList();
        this.visibleColumnCount = visibleColumnCount;
        rowId = -1;
        this.expressions = expressions;
    }

    /** {@inheritDoc} */
    @Override public boolean isLazy() {
        return false;
    }

    /** {@inheritDoc} */
    @Override public void setMaxMemoryRows(int maxValue) {
        // No-op.
    }

    /** {@inheritDoc} */
    @Override public H2BaseLocalResult createShallowCopy(SessionInterface targetSession) {
        if (containsLobs) {
            return null;
        }
        ResultExternal e2 = null;
        H2BaseLocalResult copy = new H2BaseLocalResult();
        copy.session = (Session)targetSession;
        copy.visibleColumnCount = this.visibleColumnCount;
        copy.expressions = this.expressions;
        copy.rowId = -1;
        copy.rowCount = this.rowCount;
        copy.rows = this.rows;
        copy.sort = this.sort;
        copy.distinctRows = this.distinctRows;
        copy.distinct = distinct;
        copy.distinctIndexes = distinctIndexes;
        copy.currentRow = null;
        copy.offset = 0;
        copy.limit = -1;
        copy.containsNull = containsNull;
        return copy;
    }

    /** {@inheritDoc} */
    @Override public void setSortOrder(SortOrder sort) {
        this.sort = sort;
    }

    /** {@inheritDoc} */
    @Override public void setDistinct() {
        assert distinctIndexes == null;
        distinct = true;
        distinctRows = new TreeMap<>(session.getDatabase().getCompareMode());
    }

    /** {@inheritDoc} */
    @Override public void setDistinct(int[] distinctIndexes) {
        assert !distinct;
        this.distinctIndexes = distinctIndexes;
        distinctRows = new TreeMap<>(session.getDatabase().getCompareMode());
    }

    /**
     * @return whether this result is a distinct result
     */
    private boolean isAnyDistinct() {
        return distinct || distinctIndexes != null;
    }

    /** {@inheritDoc} */
    @Override public void removeDistinct(Value[] values) {
        if (!distinct) {
            DbException.throwInternalError();
        }
        assert values.length == visibleColumnCount;
        if (distinctRows != null) {
            ValueRow array = ValueRow.get(values);
            distinctRows.remove(array);
            rowCount = distinctRows.size();
        }
    }

    /** {@inheritDoc} */
    @Override public boolean containsDistinct(Value[] values) {
        assert values.length == visibleColumnCount;
        if (distinctRows == null) {
            distinctRows = new TreeMap<>(session.getDatabase().getCompareMode());
            for (Value[] row : rows) {
                ValueRow array = getDistinctRow(row);
                distinctRows.put(array, array.getList());
            }
        }
        ValueRow array = ValueRow.get(values);
        return distinctRows.get(array) != null;
    }

    /** {@inheritDoc} */
    @Override public boolean containsNull() {
        Boolean r = containsNull;
        if (r == null) {
            r = false;
            reset();
            loop:
            while (next()) {
                Value[] row = currentRow;
                for (int i = 0; i < visibleColumnCount; i++) {
                    if (row[i].containsNull()) {
                        r = true;
                        break loop;
                    }
                }
            }
            reset();
            containsNull = r;
        }
        return r;
    }

    /** {@inheritDoc} */
    @Override
    public void reset() {
        rowId = -1;
        currentRow = null;
    }

    /** {@inheritDoc} */
    @Override public Value[] currentRow() {
        return currentRow;
    }

    /** {@inheritDoc} */
    @Override public boolean next() {
        if (!closed && rowId < rowCount) {
            rowId++;
            if (rowId < rowCount) {
                currentRow = rows.get(rowId);

                return true;
            }
            currentRow = null;
        }
        return false;
    }

    /** {@inheritDoc} */
    @Override public int getRowId() {
        return rowId;
    }

    /** {@inheritDoc} */
    @Override public boolean isAfterLast() {
        return rowId >= rowCount;
    }

    /**
     * @param values Values.
     */
    private void cloneLobs(Value[] values) {
        for (int i = 0; i < values.length; i++) {
            Value v = values[i];
            Value v2 = v.copyToResult();
            if (v2 != v) {
                containsLobs = true;
                session.addTemporaryLob(v2);
                values[i] = v2;
            }
        }
    }

    /**
     * @param values row.
     * @return Row,
     */
    private ValueRow getDistinctRow(Value[] values) {
        if (distinctIndexes != null) {
            int cnt = distinctIndexes.length;
            Value[] newValues = new Value[cnt];
            for (int i = 0; i < cnt; i++) {
                newValues[i] = values[distinctIndexes[i]];
            }
            values = newValues;
        }
        else if (values.length > visibleColumnCount) {
            values = Arrays.copyOf(values, visibleColumnCount);
        }
        return ValueRow.get(values);
    }

    /** {@inheritDoc} */
    @Override public void addRow(Value[] values) {
        cloneLobs(values);
        if (isAnyDistinct()) {
            assert distinctRows != null;

            ValueRow array = getDistinctRow(values);
            Value[] previous = distinctRows.get(array);
            if (previous == null || sort != null && sort.compare(previous, values) > 0) {
                distinctRows.put(array, values);

                onUpdate(array, previous, values);
            }
            rowCount = distinctRows.size();
        }
        else {
            onUpdate(null,null, values);
            rows.add(values);
            rowCount++;
        }
    }

    /** {@inheritDoc} */
    @Override public int getVisibleColumnCount() {
        return visibleColumnCount;
    }

    /** {@inheritDoc} */
    @Override public void done() {
        if (isAnyDistinct()) {
            rows = new ArrayList<>(distinctRows.values());
        }
        if (sort != null && limit != 0 && !limitsWereApplied) {
            boolean withLimit = limit > 0 && withTiesSortOrder == null;
            if (offset > 0 || withLimit) {
                sort.sort(rows, offset, withLimit ? limit : rows.size());
            }
            else {
                sort.sort(rows);
            }
        }

        applyOffsetAndLimit();
        reset();
    }

    private void applyOffsetAndLimit() {
        if (limitsWereApplied) {
            return;
        }
        int offset = Math.max(this.offset, 0);
        int limit = this.limit;
        if (offset == 0 && limit < 0 && !fetchPercent || rowCount == 0) {
            return;
        }
        if (fetchPercent) {
            if (limit < 0 || limit > 100) {
                throw DbException.getInvalidValueException("FETCH PERCENT", limit);
            }
            // Oracle rounds percent up, do the same for now
            limit = (int)(((long)limit * rowCount + 99) / 100);
        }
        boolean clearAll = offset >= rowCount || limit == 0;
        if (!clearAll) {
            int remaining = rowCount - offset;
            limit = limit < 0 ? remaining : Math.min(remaining, limit);
            if (offset == 0 && remaining <= limit) {
                return;
            }
        }
        else {
            limit = 0;
        }
        distinctRows = null;
        rowCount = limit;

        if (clearAll) {
            rows.clear();
            return;
        }
        int to = offset + limit;
        if (withTiesSortOrder != null) {
            Value[] expected = rows.get(to - 1);
            while (to < rows.size() && withTiesSortOrder.compare(expected, rows.get(to)) == 0) {
                to++;
                rowCount++;
            }
        }
        if (offset != 0 || to != rows.size()) {
            // avoid copying the whole array for each row
            rows = new ArrayList<>(rows.subList(offset, to));
        }
    }

    /** {@inheritDoc} */
    @Override public int getRowCount() {
        return rowCount;
    }

    /** {@inheritDoc} */
    @Override public void limitsWereApplied() {
        this.limitsWereApplied = true;
    }

    /** {@inheritDoc} */
    @Override public boolean hasNext() {
        return !closed && rowId < rowCount - 1;
    }

    /** {@inheritDoc} */
    @Override public void setLimit(int limit) {
        this.limit = limit;
    }

    /** {@inheritDoc} */
    @Override public void setFetchPercent(boolean fetchPercent) {
        this.fetchPercent = fetchPercent;
    }

    /** {@inheritDoc} */
    @Override public void setWithTies(SortOrder withTiesSortOrder) {
        assert sort == null || sort == withTiesSortOrder;
        this.withTiesSortOrder = withTiesSortOrder;
    }

    /** {@inheritDoc} */
    @Override public boolean needToClose() {
        return !closed;
    }

    /** {@inheritDoc} */
    @Override public void close() {
        closed = true;
    }

    /** {@inheritDoc} */
    @Override public String getAlias(int i) {
        return expressions[i].getAlias();
    }

    /** {@inheritDoc} */
    @Override public String getTableName(int i) {
        return expressions[i].getTableName();
    }

    /** {@inheritDoc} */
    @Override public String getSchemaName(int i) {
        return expressions[i].getSchemaName();
    }

    /** {@inheritDoc} */
    @Override public String getColumnName(int i) {
        return expressions[i].getColumnName();
    }

    /** {@inheritDoc} */
    @Override public TypeInfo getColumnType(int i) {
        return expressions[i].getType();
    }

    /** {@inheritDoc} */
    @Override public int getNullable(int i) {
        return expressions[i].getNullable();
    }

    /** {@inheritDoc} */
    @Override public boolean isAutoIncrement(int i) {
        return expressions[i].isAutoIncrement();
    }

    /** {@inheritDoc} */
    @Override public void setOffset(int offset) {
        this.offset = offset;
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return super.toString() + " columns: " + visibleColumnCount +
            " rows: " + rowCount + " pos: " + rowId;
    }

    /** {@inheritDoc} */
    @Override public boolean isClosed() {
        return closed;
    }

    /** {@inheritDoc} */
    @Override public int getFetchSize() {
        return 0;
    }

    /** {@inheritDoc} */
    @Override public void setFetchSize(int fetchSize) {
        // ignore
    }

    /**
     * Local result update callback.
     *
     * @param rowKey Distinct row key.
     * @param oldRow Old row values.
     * @param row New row values.
     */
    protected void onUpdate(ValueRow rowKey, Value[] oldRow, Value[] row) {
    }
}