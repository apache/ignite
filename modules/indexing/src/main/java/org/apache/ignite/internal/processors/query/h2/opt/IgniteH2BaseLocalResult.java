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

import java.util.ArrayList;
import java.util.Arrays;
import org.h2.engine.Session;
import org.h2.engine.SessionInterface;
import org.h2.expression.Expression;
import org.h2.message.DbException;
import org.h2.result.LocalResult;
import org.h2.result.SortOrder;
import org.h2.util.Utils;
import org.h2.util.ValueHashMap;
import org.h2.value.Value;
import org.h2.value.ValueArray;

/**
 * H2 local result.
 */
public class IgniteH2BaseLocalResult implements LocalResult {
    /** H2 Session. */
    private Session ses;

    /** Visible column count. */
    private int visibleColCnt;

    /** Expressions. */
    private Expression[] expressions;

    /** Row ID counter. */
    private int rowId;

    /** Row count. */
    private int rowCount;

    /** Rows. */
    private ArrayList<Value[]> rows;

    /** Sort. */
    private SortOrder sort;

    /** Distinct rows. */
    private ValueHashMap<Value[]> distinctRows;

    /** Current row. */
    private Value[] currentRow;

    /** Offset. */
    private int offset;

    /** Limit. */
    private int limit = -1;

    /** Fetch percent. */
    private boolean fetchPercent;

    /** With ties. */
    private boolean withTies;

    /** Limits were applied. */
    private boolean limitsWereApplied;

    /** Distinct. */
    private boolean distinct;

    /** Distinct indexes. */
    private int[] distinctIdxs;

    /** Closed. */
    private boolean closed;

    /** Contains lobs. */
    private boolean containsLobs;

    /**
     * Construct a local result object.
     */
    public IgniteH2BaseLocalResult() {
        // nothing to do
    }

    /**
     * Construct a local result object.
     *
     * @param ses the session
     * @param expressions the expression array
     * @param visibleColCnt the number of visible columns
     */
    public IgniteH2BaseLocalResult(Session ses, Expression[] expressions, int visibleColCnt) {
        this.ses = ses;
        this.rows = Utils.newSmallArrayList();
        this.visibleColCnt = visibleColCnt;
        this.rowId = -1;
        this.expressions = expressions;
    }

    /** {@inheritDoc} */
    @Override public boolean isLazy() {
        return false;
    }

    /**
     * Does nothing. Ignite manages memory during SQL query execution.
     */
    @Override public void setMaxMemoryRows(int maxValue) {
        // No-op.
    }

    /**
     * Create a shallow copy of the result set. The data and a temporary table
     * (if there is any) is not copied.
     *
     * @param targetSession the session of the copy
     * @return the copy if possible, or null if copying is not possible
     */
    @Override public IgniteH2BaseLocalResult createShallowCopy(SessionInterface targetSession) {
        if (containsLobs)
            return null;

        IgniteH2BaseLocalResult copy = new IgniteH2BaseLocalResult();

        copy.ses = (Session) targetSession;
        copy.visibleColCnt = this.visibleColCnt;
        copy.expressions = this.expressions;
        copy.rowId = -1;
        copy.rowCount = this.rowCount;
        copy.rows = this.rows;
        copy.sort = this.sort;
        copy.distinctRows = this.distinctRows;
        copy.distinct = distinct;
        copy.distinctIdxs = distinctIdxs;
        copy.currentRow = null;
        copy.offset = 0;
        copy.limit = -1;

        return copy;
    }

    /** {@inheritDoc} */
    @Override public void setSortOrder(SortOrder sort) {
        this.sort = sort;
    }

    /** {@inheritDoc} */
    @Override public void setDistinct() {
        assert distinctIdxs == null;

        distinct = true;
        distinctRows = new ValueHashMap<>();
    }

    /** {@inheritDoc} */
    @Override public void setDistinct(int[] distinctIdxs) {
        assert !distinct;

        this.distinctIdxs = distinctIdxs;
        distinctRows = new ValueHashMap<>();
    }

    /**
     * @return whether this result is a distinct result
     */
    private boolean isAnyDistinct() {
        return distinct || distinctIdxs != null;
    }

    /** {@inheritDoc} */
    @Override public void removeDistinct(Value[] values) {
        if (!distinct)
            DbException.throwInternalError();

        assert values.length == visibleColCnt;

        if (distinctRows != null) {
            ValueArray array = ValueArray.get(values);

            distinctRows.remove(array);

            rowCount = distinctRows.size();
        }
    }

    /** {@inheritDoc} */
    @Override public boolean containsDistinct(Value[] values) {
        assert values.length == visibleColCnt;

        if (distinctRows == null) {
            distinctRows = new ValueHashMap<>();

            for (Value[] row : rows) {
                ValueArray array = getArrayOfDistinct(row);

                distinctRows.put(array, array.getList());
            }
        }

        ValueArray array = ValueArray.get(values);

        return distinctRows.get(array) != null;
    }

    /** {@inheritDoc} */
    @Override public void reset() {
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
     * @param values Values to clone.
     */
    private void cloneLobs(Value[] values) {
        for (int i = 0; i < values.length; i++) {
            Value v = values[i];

            Value v2 = v.copyToResult();

            if (v2 != v) {
                containsLobs = true;

                ses.addTemporaryLob(v2);

                values[i] = v2;
            }
        }
    }

    /**
     * @param values
     * @return
     */
    private ValueArray getArrayOfDistinct(Value[] values) {
        if (distinctIdxs != null) {
            int cnt = distinctIdxs.length;

            Value[] newValues = new Value[cnt];

            for (int i = 0; i < cnt; i++)
                newValues[i] = values[distinctIdxs[i]];

            values = newValues;
        }
        else if (values.length > visibleColCnt)
            values = Arrays.copyOf(values, visibleColCnt);

        return ValueArray.get(values);
    }

    /** {@inheritDoc} */
    @Override public void addRow(Value[] row) {
        cloneLobs(row);

        if (isAnyDistinct()) {
            if (distinctRows != null) {
                ValueArray distinctKey = getArrayOfDistinct(row);

                int prevSize = distinctRows.size();

                distinctRows.putIfAbsent(distinctKey, row);

                rowCount = distinctRows.size();

                if (rowCount != prevSize) {
                    checkAvailableMemory(distinctKey);

                    checkAvailableMemory(row);
                }
            }
        }
        else {
            checkAvailableMemory(row);

            rows.add(row);

            rowCount++;
        }
    }

    /**
     * Check memory available for query. Implemented in child classes.
     * The no-op implementation is used for case query memory for SQL queries is not checked and tracked.
     *
     * @param row Row.
     */
    protected void checkAvailableMemory(Value... row) {
        // No-op.
    }

    /** {@inheritDoc} */
    @Override public int getVisibleColumnCount() {
        return visibleColCnt;
    }

    /** {@inheritDoc} */
    @Override public void done() {
        if (isAnyDistinct())
            rows = distinctRows.values();

        if (sort != null && limit != 0) {
            boolean withLimit = limit > 0 && !withTies;

            if (offset > 0 || withLimit)
                sort.sort(rows, offset, withLimit ? limit : rows.size());
            else
                sort.sort(rows);
        }

        applyOffsetAndLimit();

        reset();
    }

    /**
     *
     */
    private void applyOffsetAndLimit() {
        if (limitsWereApplied)
            return;

        int offset = Math.max(this.offset, 0);
        int limit = this.limit;

        if (offset == 0 && limit < 0 && !fetchPercent || rowCount == 0)
            return;

        if (fetchPercent) {
            if (limit < 0 || limit > 100)
                throw DbException.getInvalidValueException("FETCH PERCENT", limit);

            // Oracle rounds percent up, do the same for now
            limit = (int) (((long) limit * rowCount + 99) / 100);
        }

        boolean clearAll = offset >= rowCount || limit == 0;

        if (!clearAll) {
            int remaining = rowCount - offset;

            limit = limit < 0 ? remaining : Math.min(remaining, limit);

            if (offset == 0 && remaining <= limit)
                return;

        } else
            limit = 0;

        distinctRows = null;
        rowCount = limit;

        if (clearAll) {
            rows.clear();

            return;
        }

        int to = offset + limit;

        if (withTies && sort != null) {
            Value[] expected = rows.get(to - 1);

            while (to < rows.size() && sort.compare(expected, rows.get(to)) == 0) {
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
    @Override public void setWithTies(boolean withTies) {
        this.withTies = withTies;
    }

    /** {@inheritDoc} */
    @Override public boolean needToClose() {
        return true;
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
    @Override public int getDisplaySize(int i) {
        return expressions[i].getDisplaySize();
    }

    /** {@inheritDoc} */
    @Override public String getColumnName(int i) {
        return expressions[i].getColumnName();
    }

    /** {@inheritDoc} */
    @Override public int getColumnType(int i) {
        return expressions[i].getType();
    }

    /** {@inheritDoc} */
    @Override public long getColumnPrecision(int i) {
        return expressions[i].getPrecision();
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
    @Override public int getColumnScale(int i) {
        return expressions[i].getScale();
    }

    /** {@inheritDoc} */
    @Override public void setOffset(int offset) {
        this.offset = offset;
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return super.toString() + " columns: " + visibleColCnt +
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
}
