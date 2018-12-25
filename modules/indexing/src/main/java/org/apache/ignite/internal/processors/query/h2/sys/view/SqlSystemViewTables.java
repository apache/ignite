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

import java.util.Arrays;
import java.util.Iterator;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Predicate;
import org.apache.ignite.internal.GridKernalContext;
import org.apache.ignite.internal.processors.query.GridQueryTypeDescriptor;
import org.apache.ignite.internal.processors.query.QueryUtils;
import org.h2.engine.Session;
import org.h2.result.Row;
import org.h2.result.SearchRow;
import org.h2.value.Value;

/**
 * View that contains information about all the sql tables in the cluster.
 */
public class SqlSystemViewTables extends SqlAbstractLocalSystemView {
    /** Name of the affinity column. Columns value could be {@code null} if affinity key is not specified. */
    public static final String AFFINITY_COLUMN = "AFFINITY_COLUMN";

    /** Alias for cache key. {@link QueryUtils#KEY_FIELD_NAME} by default. */
    public static final String KEY_ALIAS = "KEY_ALIAS";

    /** Alias for cache value. {@link QueryUtils#VAL_FIELD_NAME} by default. */
    public static final String VALUE_ALIAS = "VALUE_ALIAS";

    /** Name of the sql table. */
    public static final String TABLE_NAME = "TABLE_NAME";

    /** Sql schema name (database name). */
    public static final String TABLE_SCHEMA = "TABLE_SCHEMA";

    /** Name of the cache holding that table. */
    public static final String OWNING_CACHE_NAME = "OWNING_CACHE_NAME";

    /** Id of the cache holding that table. */
    public static final String OWNING_CACHE_ID = "OWNING_CACHE_ID";

    /**
     * Creates view with columns.
     *
     * @param ctx kernal context.
     */
    public SqlSystemViewTables(GridKernalContext ctx) {
        super("TABLES", "Ignite tables", ctx, TABLE_NAME,
            newColumn(TABLE_SCHEMA),
            newColumn(TABLE_NAME),
            newColumn(OWNING_CACHE_NAME),
            newColumn(OWNING_CACHE_ID, Value.INT),
            newColumn(AFFINITY_COLUMN),
            newColumn(KEY_ALIAS),
            newColumn(VALUE_ALIAS)
        );
    }

    /** {@inheritDoc} */
    @Override public Iterator<Row> getRows(Session ses, SearchRow first, SearchRow last) {
        SqlSystemViewColumnCondition nameCond = conditionForColumn(TABLE_NAME, first, last);

        Predicate<GridQueryTypeDescriptor> filter;

        if (nameCond.isEquality()) {
            String fltTabName = nameCond.valueForEquality().getString();

            filter = tab -> fltTabName.equals(tab.tableName());
        }
        else
            filter = tab -> true;

        final AtomicLong keys = new AtomicLong();

        return ctx.cache().publicCacheNames().stream()
            .flatMap(cacheName ->
                ctx.query().types(cacheName).stream()
                    .filter(filter)
                    .map(tab -> Arrays.asList(
                        tab.schemaName(),
                        tab.tableName(),
                        cacheName,
                        ctx.cache().cacheDescriptor(cacheName).cacheId(),
                        tab.affinityKey(),
                        QueryUtils.cacheKeyName(tab),
                        QueryUtils.cacheValueName(tab))
                    )
            )
            .unordered()
            .distinct()
            .map(dataList ->
                createRow(ses, keys.incrementAndGet(), dataList))
            .iterator();
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
