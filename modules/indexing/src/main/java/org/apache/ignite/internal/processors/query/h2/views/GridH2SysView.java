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

package org.apache.ignite.internal.processors.query.h2.views;

import org.apache.ignite.IgniteLogger;
import org.apache.ignite.internal.GridKernalContext;
import org.h2.engine.Session;
import org.h2.result.Row;
import org.h2.result.SearchRow;
import org.h2.table.Column;
import org.h2.value.Value;
import org.h2.value.ValueNull;
import org.h2.value.ValueString;
import org.h2.value.ValueTime;
import org.h2.value.ValueTimestamp;

/**
 * System view base class.
 */
public abstract class GridH2SysView {
    /** Table name prefix. */
    private static final String TABLE_NAME_PREFIX = "IGNITE_";

    /** Table schema name. */
    public static final String TABLE_SCHEMA_NAME = "INFORMATION_SCHEMA";

    /** Default row count approximation. */
    protected static final long DEFAULT_ROW_COUNT_APPROXIMATION = 100L;

    /** Table name. */
    protected final String tblName;

    /** Description. */
    protected final String desc;

    /** Grid context. */
    protected final GridKernalContext ctx;

    /** Logger. */
    protected final IgniteLogger log;

    /** Columns. */
    protected final Column[] cols;

    /** Indexed column names. */
    protected final String[] indexes;

    /**
     * @param tblName Table name.
     * @param desc Description.
     * @param ctx Context.
     * @param indexes Indexed columns.
     * @param cols Columns.
     */
    public GridH2SysView(String tblName, String desc, GridKernalContext ctx, String[] indexes, Column... cols) {
        assert tblName != null;
        assert ctx != null;
        assert cols != null;
        assert indexes != null;

        this.tblName = TABLE_NAME_PREFIX + tblName;
        this.ctx = ctx;
        this.cols = cols;
        this.indexes = indexes;
        this.desc = desc;
        this.log = ctx.log(this.getClass());
    }

    /**
     * @param tblName Table name.
     * @param desc Description.
     * @param ctx Context.
     * @param indexedCols Indexed column.
     * @param cols Columns.
     */
    public GridH2SysView(String tblName, String desc, GridKernalContext ctx, String indexedCols, Column... cols) {
        this(tblName, desc, ctx, new String[] {indexedCols}, cols);
    }

    /**
     * @param tblName Table name.
     * @param desc Description.
     * @param ctx Context.
     * @param cols Columns.
     */
    public GridH2SysView(String tblName, String desc, GridKernalContext ctx, Column... cols) {
        this(tblName, desc, ctx, new String[] {}, cols);
    }

    /**
     * @param name Name.
     */
    protected static Column newColumn(String name) {
        return newColumn(name, Value.STRING);
    }

    /**
     * @param name Name.
     * @param type Type.
     */
    protected static Column newColumn(String name, int type) {
        return new Column(name, type);
    }

    /**
     * @param name Name.
     * @param type Type.
     * @param precision Precision.
     * @param scale Scale.
     * @param displaySize Display size.
     */
    protected static Column newColumn(String name, int type, long precision, int scale, int displaySize) {
        return new Column(name, type, precision, scale, displaySize);
    }

    /**
     * Convert millis to ValueTime
     *
     * @param millis Millis.
     */
    protected static Value valueTimeFromMillis(long millis) {
        if (millis == -1L || millis == Long.MAX_VALUE)
            return ValueNull.INSTANCE;
        else
            // Note: ValueTime.fromMillis(long) method trying to convert time using timezone and return wrong result.
            return ValueTime.fromNanos(millis * 1_000_000L);
    }

    /**
     * Convert millis to ValueTimestamp
     *
     * @param millis Millis.
     */
    protected static Value valueTimestampFromMillis(long millis) {
        if (millis == -1L || millis == Long.MAX_VALUE)
            return ValueNull.INSTANCE;
        else
            return ValueTimestamp.fromMillis(millis);
    }

    /**
     * @param ses Session.
     * @param key Key.
     * @param data Data for each column.
     */
    protected Row createRow(Session ses, long key, Object... data) {
        Value[] values = new Value[data.length];

        for (int i = 0; i < data.length; i++) {
            Object o = data[i];

            Value v = (o == null) ? ValueNull.INSTANCE :
                (o instanceof Value) ? (Value)o : ValueString.get(o.toString());

            values[i] = cols[i].convert(v);
        }

        Row row = ses.getDatabase().createRow(values, 1);

        row.setKey(key);

        return row;
    }

    /**
     * Gets view content.
     *
     * @param ses Session.
     * @param first First.
     * @param last Last.
     */
    public abstract Iterable<Row> getRows(Session ses, SearchRow first, SearchRow last);

    /**
     * Gets row count for this view (or approximated row count, if real value can't be calculated quickly).
     */
    public long getRowCount() {
        return DEFAULT_ROW_COUNT_APPROXIMATION;
    }

    /**
     * Check if the row count can be retrieved quickly.
     *
     * @return true if it can
     */
    public boolean canGetRowCount() {
        return false;
    }

    /**
     * Gets table name.
     */
    public String getTableName() {
        return tblName;
    }

    /**
     * Gets description.
     */
    public String getDescription() {
        return desc;
    }

    /**
     * Gets grid context.
     */
    public GridKernalContext getGridContext() {
        return ctx;
    }

    /**
     * Gets columns.
     */
    public Column[] getColumns() {
        return cols;
    }

    /**
     * Gets indexed column names.
     */
    public String[] getIndexes() {
        return indexes;
    }

    /**
     * Gets SQL script for creating table.
     */
    public String getCreateSQL() {
        StringBuilder sql = new StringBuilder();
        sql.append("CREATE TABLE " + getTableName() + '(');

        boolean isFirst = true;
        for (Column col : getColumns()) {
            if (isFirst)
                isFirst = false;
            else
                sql.append(", ");

            sql.append(col.getCreateSQL());
        }

        sql.append(')');

        return sql.toString();
    }

    /**
     * Gets column index by name.
     *
     * @param colName Column name.
     */
    protected int getColumnIndex(String colName) {
        assert colName != null;

        for (int i = 0; i < cols.length; i++)
            if (colName.equalsIgnoreCase(cols[i].getName()))
                return i;

        return -1;
    }

    /**
     * Parse condition for column.
     *
     * @param colName Column name.
     * @param first First.
     * @param last Last.
     */
    protected ColumnCondition conditionForColumn(String colName, SearchRow first, SearchRow last) {
        return ColumnCondition.forColumn(getColumnIndex(colName), first, last);
    }

    /**
     * Column condition.
     */
    protected static class ColumnCondition {
        /** Is equality. */
        private final boolean isEquality;

        /** Is range. */
        private final boolean isRange;

        /** Value 1. */
        private final Value val1;

        /** Value 2. */
        private final Value val2;

        /**
         * @param isEquality Is equality.
         * @param isRange Is range.
         * @param val1 Value 1.
         * @param val2 Value 2.
         */
        private ColumnCondition(boolean isEquality, boolean isRange, Value val1, Value val2) {
            this.isEquality = isEquality;
            this.isRange = isRange;
            this.val1 = val1;
            this.val2 = val2;
        }

        /**
         * Parse condition for column.
         *
         * @param colIdx Column index.
         * @param start Start row values.
         * @param end End row values.
         */
        public static ColumnCondition forColumn(int colIdx, SearchRow start, SearchRow end) {
            boolean isEquality = false;
            boolean isRange = false;

            Value val1 = null;
            Value val2 = null;

            if (start != null && colIdx >= 0 && colIdx < start.getColumnCount())
                val1 = start.getValue(colIdx);

            if (end != null && colIdx >= 0 && colIdx < end.getColumnCount())
                val2 = end.getValue(colIdx);

            if (val1 != null && val2 != null) {
                if (val1.equals(val2))
                    isEquality = true;
                else
                    isRange = true;
            }
            else if (val1 != null || val2 != null)
                isRange = true;

            return new ColumnCondition(isEquality, isRange, val1, val2);
        }

        /**
         * Checks whether the condition is equality.
         */
        public boolean isEquality() {
            return isEquality;
        }

        /**
         * Checks whether the condition is range.
         */
        public boolean isRange() {
            return isRange;
        }

        /**
         * Gets value, if condition is equality.
         */
        public Value getValue() {
            if (isEquality)
                return val1;

            return null;
        }

        /**
         * Gets start value, if condition is range.
         */
        public Value getMinValue() {
            if (isRange)
                return val1;

            return null;
        }

        /**
         * Gets end value, if condition is range.
         */
        public Value getMaxValue() {
            if (isRange)
                return val2;

            return null;
        }
    }
}
