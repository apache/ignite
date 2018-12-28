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
import org.apache.ignite.internal.processors.query.QueryUtils;
import org.apache.ignite.internal.processors.query.h2.H2TableDescriptor;
import org.apache.ignite.internal.processors.query.h2.SchemaManager;
import org.apache.ignite.internal.processors.query.h2.opt.GridH2RowDescriptor;
import org.apache.ignite.internal.processors.query.h2.opt.GridH2Table;
import org.h2.engine.Session;
import org.h2.result.Row;
import org.h2.result.SearchRow;
import org.h2.table.Column;
import org.h2.table.IndexColumn;
import org.h2.value.Value;
import org.jetbrains.annotations.Nullable;

import static org.apache.ignite.internal.processors.query.h2.opt.GridH2KeyValueRowOnheap.DEFAULT_COLUMNS_COUNT;

/**
 * View that contains information about all the sql tables in the cluster.
 */
public class SqlSystemViewTables extends SqlAbstractLocalSystemView {
    /**
     * Name of the affinity column. Columns value could be {@code null} if affinity key is not known, "_KEY" for
     * default.
     */
    public static final String AFFINITY_COLUMN = "AFFINITY_COLUMN";

    /** Alias for cache key. {@link QueryUtils#KEY_FIELD_NAME} by default. */
    public static final String KEY_ALIAS = "KEY_ALIAS";

    /** Alias for cache value. {@link QueryUtils#VAL_FIELD_NAME} by default. */
    public static final String VALUE_ALIAS = "VALUE_ALIAS";

    /** Type name of key cache object. */
    public static final String KEY_TYPE_NAME = "KEY_TYPE_NAME";

    /** Type name of value cache object. */
    public static final String VALUE_TYPE_NAME = "VALUE_TYPE_NAME";

    /** Name of the sql table. */
    public static final String TABLE_NAME = "TABLE_NAME";

    /** Sql schema name (database name). */
    public static final String TABLE_SCHEMA = "TABLE_SCHEMA";

    /** Name of the cache holding that table. */
    public static final String OWNING_CACHE_NAME = "OWNING_CACHE_NAME";

    /** Id of the cache holding that table. */
    public static final String OWNING_CACHE_ID = "OWNING_CACHE_ID";

    /** Api to all known tables. */
    private final SchemaManager mngr;

    /**
     * Creates view with columns.
     *
     * @param ctx kernal context.
     */
    public SqlSystemViewTables(GridKernalContext ctx, SchemaManager schemaMngr) {
        super("TABLES", "Ignite tables", ctx, TABLE_NAME,
            newColumn(TABLE_SCHEMA),
            newColumn(TABLE_NAME),
            newColumn(OWNING_CACHE_NAME),
            newColumn(OWNING_CACHE_ID, Value.INT),
            newColumn(AFFINITY_COLUMN),
            newColumn(KEY_ALIAS),
            newColumn(VALUE_ALIAS),
            newColumn(KEY_TYPE_NAME),
            newColumn(VALUE_TYPE_NAME)
        );

        mngr = schemaMngr;
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

        return ctx.cache().publicCacheNames().stream()
            .flatMap(cacheName -> mngr.tablesForCache(cacheName).stream())
            .map(H2TableDescriptor::table)
            .filter(filter)
            .map(tab -> {
                    Object[] data = new Object[] {
                        tab.getSchema().getName(),
                        tab.getName(),
                        tab.cacheName(),
                        tab.cacheId(),
                        getAffinityColumn(tab),
                        getKeyAlias(tab),
                        getValueAlias(tab),
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
     * Returns name of the value column in case of simple value or user defined value alias. If not found - "_val".
     *
     * @param table table to extract value alias.
     * @return alias of the value.
     */
    private static String getValueAlias(GridH2Table table) {
        GridH2RowDescriptor desc = table.rowDescriptor();

        Column[] cols = table.getColumns();

        for (int i = DEFAULT_COLUMNS_COUNT; i < cols.length; i++) {
            if (desc.isValueAliasColumn(i))
                return cols[i].getName();
        }

        return QueryUtils.VAL_FIELD_NAME;
    }

    /**
     * Returns name of the key column in case of simple key or user defined key alias. If not found - "_key".
     *
     * @param table table to extract key alias.
     * @return alias of the key.
     */
    private static String getKeyAlias(GridH2Table table) {
        GridH2RowDescriptor desc = table.rowDescriptor();

        Column[] cols = table.getColumns();

        for (int i = DEFAULT_COLUMNS_COUNT; i < cols.length; i++) {
            if (desc.isKeyAliasColumn(i))
                return cols[i].getName();
        }

        return QueryUtils.KEY_FIELD_NAME;
    }

    /**
     * Returns column name of column that defines affinity distribution of table's cache parts.
     *
     * @param table table by which to get column.
     * @return affinity column name if got one, _KEY if default or null if not known.
     */
    @Nullable private static String getAffinityColumn(GridH2Table table) {
        IndexColumn affCol = table.getAffinityKeyColumn();

        if (affCol == null)
            return null;

        return affCol.columnName;
    }

    /** {@inheritDoc} */
    @Override public boolean canGetRowCount() {
        return true;
    }

    /** {@inheritDoc} */
    @Override public long getRowCount() {
        return ctx.cache().publicAndDsCacheNames().stream().mapToLong(c -> ctx.query().types(c).size()).sum();
    }
}
