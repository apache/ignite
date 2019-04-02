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

package org.apache.ignite.internal.processors.query.h2.sys.view;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.function.Predicate;
import org.apache.ignite.internal.GridKernalContext;
import org.apache.ignite.internal.processors.query.QueryUtils;
import org.apache.ignite.internal.processors.query.h2.SchemaManager;
import org.apache.ignite.internal.processors.query.h2.opt.GridH2Table;
import org.apache.ignite.internal.util.typedef.F;
import org.h2.engine.Session;
import org.h2.result.Row;
import org.h2.result.SearchRow;
import org.h2.table.Column;
import org.h2.table.IndexColumn;
import org.h2.value.DataType;
import org.h2.value.Value;

/**
 * View that contains information about all the sql tables' columns in the cluster.
 */
public class SqlSystemViewColumns extends SqlAbstractLocalSystemView {
    /** Name of the column that contains names of sql tables. */
    private static final String TABLE_NAME = "TABLE_NAME";

    /** Name of the column that contains names of sql table's columns. */
    private static final String COLUMN_NAME = "COLUMN_NAME";

    /** Schema manager. */
    private final SchemaManager schemaMgr;

    /**
     * Creates view with columns.
     *
     * @param ctx Kernal context.
     * @param schemaMgr Schema manager.
     */
    public SqlSystemViewColumns(GridKernalContext ctx, SchemaManager schemaMgr) {
        super("COLUMNS", "Ignite columns", ctx, new String[] {TABLE_NAME, COLUMN_NAME},
            newColumn("SCHEMA_NAME"),
            newColumn(TABLE_NAME),
            newColumn("COLUMN_ID", Value.INT),
            newColumn(COLUMN_NAME),
            newColumn("DEFAULT_VALUE"),
            newColumn("IS_NULLABLE", Value.BOOLEAN),
            newColumn("SQL_TYPE", Value.INT),
            newColumn("DATA_TYPE"),
            newColumn("DISPLAY_SIZE", Value.INT),
            newColumn("PRECISION", Value.INT),
            newColumn("SCALE", Value.INT),
            newColumn("AFFINITY_KEY", Value.BOOLEAN)
        );

        this.schemaMgr = schemaMgr;
    }

    /** {@inheritDoc} */
    @Override public Iterator<Row> getRows(Session ses, SearchRow first, SearchRow last) {
        SqlSystemViewColumnCondition tableNameCond = conditionForColumn(TABLE_NAME, first, last);
        SqlSystemViewColumnCondition columnNameCond = conditionForColumn(COLUMN_NAME, first, last);

        Predicate<GridH2Table> tblFilter;
        Predicate<Column> colFilter;

        if (tableNameCond.isEquality()) {
            String tblName = tableNameCond.valueForEquality().getString();

            tblFilter = tbl -> F.eq(tblName, tbl.getName());
        }
        else
            tblFilter = tab -> true;

        if (columnNameCond.isEquality()) {
            String colName = columnNameCond.valueForEquality().getString();

            colFilter = col -> F.eq(colName, col.getName());
        }
        else
            colFilter = tab -> true;

        List<Row> res = new ArrayList<>();

        for (GridH2Table tbl : schemaMgr.dataTables()) {
            if (!tblFilter.test(tbl))
                continue;

            IndexColumn affCol = tbl.getExplicitAffinityKeyColumn();

            for (int i = QueryUtils.DEFAULT_COLUMNS_COUNT; i < tbl.getColumns().length; ++i) {
                Column col = tbl.getColumns()[i];

                if (!colFilter.test(col))
                    continue;

                Object[] data = new Object[] {
                    col.getTable().getSchema().getName(),
                    col.getTable().getName(),
                    col.getColumnId(),
                    col.getName(),
                    col.getDefaultExpression() != null ? col.getDefaultExpression().getValue(ses).toString() : null,
                    col.isNullable(),
                    col.getType(),
                    DataType.getDataType(col.getType()).name,
                    col.getDisplaySize(),
                    col.getPrecision(),
                    col.getScale(),
                    affCol != null && F.eq(col.getColumnId(), affCol.column.getColumnId()),
                };

                res.add(createRow(ses, data));
            }
        }

        return res.iterator();
    }

    /** {@inheritDoc} */
    @Override public boolean canGetRowCount() {
        return true;
    }

    /** {@inheritDoc} */
    @Override public long getRowCount() {
        return schemaMgr.dataTables().stream().mapToInt(tbl -> tbl.getColumns().length).sum();
    }
}
