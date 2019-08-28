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
 *
 */

package org.apache.ignite.internal.processors.query.h2.sys.view;

import java.util.Iterator;
import org.apache.ignite.internal.GridKernalContext;
import org.apache.ignite.internal.processors.metric.GridMetricManager;
import org.apache.ignite.internal.processors.metric.list.MonitoringList;
import org.apache.ignite.internal.processors.metric.list.view.SqlIndexView;
import org.apache.ignite.internal.processors.query.h2.SchemaManager;
import org.apache.ignite.internal.util.typedef.F;
import org.h2.engine.Session;
import org.h2.result.Row;
import org.h2.result.SearchRow;
import org.h2.value.Value;

import static org.apache.ignite.internal.processors.metric.impl.MetricUtils.metricName;

/**
 * View that contains information about all the sql tables in the cluster.
 *
 * @deprecated Use {@link GridMetricManager} instead.
 */
@Deprecated
public class SqlSystemViewIndexes extends SqlAbstractLocalSystemView {
    /** Index monitoring list. */
    private MonitoringList<String, SqlIndexView> idxs;

    /**
     * Creates view with columns.
     *
     * @param ctx kernal context.
     */
    public SqlSystemViewIndexes(GridKernalContext ctx, SchemaManager schemaMgr) {
        super("INDEXES", "Ignite SQL indexes", ctx, "TABLE_NAME",
            newColumn("CACHE_GROUP_ID", Value.INT),
            newColumn("CACHE_GROUP_NAME"),
            newColumn("CACHE_ID", Value.INT),
            newColumn("CACHE_NAME"),
            newColumn("SCHEMA_NAME"),
            newColumn("TABLE_NAME"),
            newColumn("INDEX_NAME"),
            newColumn("INDEX_TYPE"),
            newColumn("COLUMNS"),
            newColumn("IS_PK", Value.BOOLEAN),
            newColumn("IS_UNIQUE", Value.BOOLEAN),
            newColumn("INLINE_SIZE", Value.INT)
        );

        this.idxs = ctx.metric().list(metricName("sql", "indexes"), "SQL indexes", SqlIndexView.class);
    }

    /** {@inheritDoc} */
    @Override public Iterator<Row> getRows(Session ses, SearchRow first, SearchRow last) {
        return F.iterator(idxs.iterator(), idx -> createRow(ses,
            idx.cacheGroupId(),
            idx.cacheGroupName(),
            idx.cacheId(),
            idx.cacheName(),
            idx.schemaName(),
            idx.tableName(),
            idx.indexName(),
            idx.indexType(),
            idx.columns(),
            idx.isPk(),
            idx.isUnique(),
            idx.inlineSize()
        ), true);
    }

    /** {@inheritDoc} */
    @Override public boolean canGetRowCount() {
        return true;
    }

    /** {@inheritDoc} */
    @Override public long getRowCount() {
        return idxs.size();
    }
}
