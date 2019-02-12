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
import java.util.TreeMap;
import org.h2.engine.Database;
import org.h2.engine.Session;
import org.h2.engine.SessionInterface;
import org.h2.expression.Expression;
import org.h2.message.DbException;
import org.h2.mvstore.db.MVTempResult;
import org.h2.result.LocalResult;
import org.h2.result.LocalResultImpl;
import org.h2.result.ResultExternal;
import org.h2.result.SortOrder;
import org.h2.util.Utils;
import org.h2.value.TypeInfo;
import org.h2.value.Value;
import org.h2.value.ValueRow;

/**
 * H2 local result.
 */
public class IgniteH2BaseLocalResult implements LocalResult {
    private Session session;
    private int visibleColumnCount;
    private Expression[] expressions;
    private int rowId, rowCount;
    private ArrayList<Value[]> rows;
    private SortOrder sort;
    private TreeMap<Value, Value[]> distinctRows;
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
    public IgniteH2BaseLocalResult() {
        // nothing to do
    }

    /**
     * Construct a local result object.
     *
     * @param session the session
     * @param expressions the expression array
     * @param visibleColumnCount the number of visible columns
     */
    public IgniteH2BaseLocalResult(Session session, Expression[] expressions,
        int visibleColumnCount) {
        this.session = session;
        rows = Utils.newSmallArrayList();
        this.visibleColumnCount = visibleColumnCount;
        rowId = -1;
        this.expressions = expressions;
    }

    @Override
    public boolean isLazy() {
        return false;
    }

    @Override
    public void setMaxMemoryRows(int maxValue) {
        // No-op.
    }

    /**
     * Create a shallow copy of the result set. The data and a temporary table (if there is any) is not copied.
     *
     * @param targetSession the session of the copy
     * @return the copy if possible, or null if copying is not possible
     */
    @Override
    public IgniteH2BaseLocalResult createShallowCopy(SessionInterface targetSession) {
        if (containsLobs) {
            return null;
        }
        ResultExternal e2 = null;
        IgniteH2BaseLocalResult copy = new IgniteH2BaseLocalResult();
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

    @Override
    public void setSortOrder(SortOrder sort) {
        this.sort = sort;
    }

    /**
     * Remove duplicate rows.
     */
    @Override
    public void setDistinct() {
        assert distinctIndexes == null;
        distinct = true;
        distinctRows = new TreeMap<>(session.getDatabase().getCompareMode());
    }

    /**
     * Remove rows with duplicates in columns with specified indexes.
     *
     * @param distinctIndexes distinct indexes
     */
    @Override
    public void setDistinct(int[] distinctIndexes) {
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

    /**
     * Remove the row from the result set if it exists.
     *
     * @param values the row
     */
    @Override
    public void removeDistinct(Value[] values) {
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

    /**
     * Check if this result set contains the given row.
     *
     * @param values the row
     * @return true if the row exists
     */
    @Override
    public boolean containsDistinct(Value[] values) {
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

    @Override
    public boolean containsNull() {
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

    @Override
    public void reset() {
        rowId = -1;
        currentRow = null;
    }

    @Override
    public Value[] currentRow() {
        return currentRow;
    }

    @Override
    public boolean next() {
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

    @Override
    public int getRowId() {
        return rowId;
    }

    @Override
    public boolean isAfterLast() {
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

    /**
     * Add a row to this object.
     *
     * @param values the row to add
     */
    @Override
    public void addRow(Value[] values) {
        cloneLobs(values);
        if (isAnyDistinct()) {
            if (distinctRows != null) {
                int prevSize = distinctRows.size();
                ValueRow array = getDistinctRow(values);

                if (!distinctRows.containsKey(array))
                    distinctRows.put(array, values);
                rowCount = distinctRows.size();

                if (rowCount > prevSize) {
                    checkAvailableMemory(array);
                    checkAvailableMemory(values);
                }
            }
        }
        else {
            checkAvailableMemory(values);
            rows.add(values);
            rowCount++;
        }
    }

    @Override
    public int getVisibleColumnCount() {
        return visibleColumnCount;
    }

    /**
     * This method is called after all rows have been added.
     */
    @Override
    public void done() {
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

    @Override
    public int getRowCount() {
        return rowCount;
    }

    @Override
    public void limitsWereApplied() {
        this.limitsWereApplied = true;
    }

    @Override
    public boolean hasNext() {
        return !closed && rowId < rowCount - 1;
    }

    /**
     * Set the number of rows that this result will return at the maximum.
     *
     * @param limit the limit (-1 means no limit, 0 means no rows)
     */
    @Override
    public void setLimit(int limit) {
        this.limit = limit;
    }

    /**
     * @param fetchPercent whether limit expression specifies percentage of rows
     */
    @Override
    public void setFetchPercent(boolean fetchPercent) {
        this.fetchPercent = fetchPercent;
    }

    @Override
    public void setWithTies(SortOrder withTiesSortOrder) {
        assert sort == null || sort == withTiesSortOrder;
        this.withTiesSortOrder = withTiesSortOrder;
    }

    @Override
    public boolean needToClose() {
        return false;
    }

    @Override
    public void close() {
    }

    @Override
    public String getAlias(int i) {
        return expressions[i].getAlias();
    }

    @Override
    public String getTableName(int i) {
        return expressions[i].getTableName();
    }

    @Override
    public String getSchemaName(int i) {
        return expressions[i].getSchemaName();
    }

    @Override
    public String getColumnName(int i) {
        return expressions[i].getColumnName();
    }

    @Override
    public TypeInfo getColumnType(int i) {
        return expressions[i].getType();
    }

    @Override
    public int getNullable(int i) {
        return expressions[i].getNullable();
    }

    @Override
    public boolean isAutoIncrement(int i) {
        return expressions[i].isAutoIncrement();
    }

    /**
     * Set the offset of the first row to return.
     *
     * @param offset the offset
     */
    @Override
    public void setOffset(int offset) {
        this.offset = offset;
    }

    @Override
    public String toString() {
        return super.toString() + " columns: " + visibleColumnCount +
            " rows: " + rowCount + " pos: " + rowId;
    }

    /**
     * Check if this result set is closed.
     *
     * @return true if it is
     */
    @Override
    public boolean isClosed() {
        return closed;
    }

    @Override
    public int getFetchSize() {
        return 0;
    }

    @Override
    public void setFetchSize(int fetchSize) {
        // ignore
    }

    /**
     * @param row Row.
     */
    protected void checkAvailableMemory(Value... row) {
    }
}