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

import org.apache.ignite.internal.GridKernalContext;
import org.h2.engine.Session;
import org.h2.result.Row;
import org.h2.result.SearchRow;
import org.h2.table.Column;
import org.h2.value.Value;
import org.h2.value.ValueNull;
import org.h2.value.ValueString;

/**
 *
 */
public abstract class GridH2SysView {
    /** Table name prefix. */
    private static final String TABLE_NAME_PREFIX = "IGNITE_";

    /** Table schema name. */
    public static final String TABLE_SCHEMA_NAME = "INFORMATION_SCHEMA";

    /** Table name. */
    protected final String tblName;

    /** Grid context. */
    protected final GridKernalContext ctx;

    /** Columns. */
    protected final Column[] cols;

    /** Indexed column names. */
    protected final String[] indexedCols;

    /**
     * @param tblName Table name.
     * @param ctx Context.
     * @param indexedCols Indexed columns.
     * @param cols Columns.
     */
    public GridH2SysView(String tblName, GridKernalContext ctx, String[] indexedCols, Column... cols) {
        assert tblName != null;
        assert ctx != null;
        assert cols != null;
        assert indexedCols != null;

        this.tblName = TABLE_NAME_PREFIX + tblName;
        this.ctx = ctx;
        this.cols = cols;
        this.indexedCols = indexedCols;
    }

    /**
     * @param tblName Table name.
     * @param ctx Context.
     * @param indexedCol Indexed column.
     * @param cols Columns.
     */
    public GridH2SysView(String tblName, GridKernalContext ctx, String indexedCol, Column... cols) {
        this(tblName, ctx, new String[] {indexedCol}, cols);
    }

    /**
     * @param tblName Table name.
     * @param ctx Context.
     * @param cols Columns.
     */
    public GridH2SysView(String tblName, GridKernalContext ctx, Column... cols) {
        this(tblName, ctx, new String[] {}, cols);
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
     * @param data Data.
     */
    protected Row createRow(Session ses, long key, Object... data) {
        Value[] values = new Value[data.length];

        for (int i = 0; i < data.length; i++) {
            Object s = data[i];
            Value v = (s == null) ? ValueNull.INSTANCE : ValueString.get(s.toString());
            values[i] = cols[i].convert(v);
        }

        Row row = ses.getDatabase().createRow(values, 1);

        row.setKey(key);

        return row;
    }

    /**
     * @param ses Session.
     * @param first First.
     * @param last Last.
     */
    public abstract Iterable<Row> getRows(Session ses, SearchRow first, SearchRow last);

    /**
     * Gets table name.
     */
    public String getTableName() {
        return tblName;
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
    public String[] getIndexedColumns() {
        return indexedCols;
    }

    /**
     * Gets SQL script for creating table.
     */
    public String getCreateSQL() {
        StringBuilder sql = new StringBuilder();
        sql.append("CREATE TABLE " + TABLE_SCHEMA_NAME + '.' + getTableName() + '(');

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
}
