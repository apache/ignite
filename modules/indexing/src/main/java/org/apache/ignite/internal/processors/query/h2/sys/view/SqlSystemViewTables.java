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

import java.util.Iterator;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Predicate;
import org.apache.ignite.internal.GridKernalContext;
import org.apache.ignite.internal.processors.query.h2.SchemaManager;
import org.apache.ignite.internal.processors.query.h2.opt.GridH2Table;
import org.h2.engine.Session;
import org.h2.result.Row;
import org.h2.result.SearchRow;
import org.h2.table.IndexColumn;
import org.h2.value.Value;
import org.jetbrains.annotations.Nullable;

/**
 * View that contains information about all the sql tables in the cluster.
 */
public class SqlSystemViewTables extends SqlAbstractLocalSystemView {
    /** Name of the column that contains names of sql tables. */
    private static final String TABLE_NAME = "TABLE_NAME";

    /** Api to all known tables. */
    private final SchemaManager mgr;

    /**
     * Creates view with columns.
     *
     * @param ctx kernal context.
     */
    public SqlSystemViewTables(GridKernalContext ctx, SchemaManager schemaMngr) {
        super("TABLES", "Ignite tables", ctx, TABLE_NAME,
            newColumn("SCHEMA_NAME"),
            newColumn(TABLE_NAME),
            newColumn("CACHE_NAME"),
            newColumn("CACHE_ID", Value.INT),
            newColumn("AFFINITY_COLUMN"),
            newColumn("KEY_ALIAS"),
            newColumn("VALUE_ALIAS"),
            newColumn("KEY_TYPE_NAME"),
            newColumn("VALUE_TYPE_NAME")
        );

        mgr = schemaMngr;
    }

    /** {@inheritDoc} */
    @Override public Iterator<Row> getRows(Session ses, SearchRow first, SearchRow last) {
        SqlSystemViewColumnCondition nameCond = conditionForColumn(TABLE_NAME, first, last);

        Predicate<GridH2Table> filter;

        if (nameCond.isEquality()) {
            String fltTabName = nameCond.valueForEquality().getString();

            filter = tab -> fltTabName.equals(tab.getName());
        }
        else
            filter = tab -> true;

        final AtomicLong keys = new AtomicLong();

        return mgr.dataTables().stream()
            .filter(filter)
            .map(tab -> {
                    Object[] data = new Object[] {
                        tab.getSchema().getName(),
                        tab.getName(),
                        tab.cacheName(),
                        tab.cacheId(),
                        computeAffinityColumn(tab),
                        tab.keyAliasName(),
                        tab.valueAliasName(),
                        // We use type descriptor because there is no way to get complex type (custom class Person)
                        // from typeid.
                        tab.rowDescriptor().type().keyTypeName(),
                        tab.rowDescriptor().type().valueTypeName()
                    };

                    return createRow(ses, keys.incrementAndGet(), data);
                }
            ).iterator();
    }

    /**
     * Computes "AFFINITY_COLUMN" for the specified table. Affinity column is a column which value is an Affinity Key
     * of the table's underlying cache.
     *
     * @param tab table compute affinity column for.
     * @return "_KEY" for default (all PK), {@code null} if custom mapper specified or name of the desired column
     * otherwise.
     */
    private @Nullable String computeAffinityColumn(GridH2Table tab) {
        IndexColumn affCol = tab.getAffinityKeyColumn();

        if (affCol == null) // custom mapper is used.
            return null;

        return affCol.columnName;
    }

    /** {@inheritDoc} */
    @Override public boolean canGetRowCount() {
        return true;
    }

    /** {@inheritDoc} */
    @Override public long getRowCount() {
        return mgr.dataTables().size();
    }
}
