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
import org.apache.ignite.internal.GridKernalContext;
import org.apache.ignite.internal.processors.metric.GridMetricManager;
import org.apache.ignite.internal.processors.metric.list.MonitoringList;
import org.apache.ignite.internal.processors.metric.list.view.SqlTableView;
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
public class SqlSystemViewTables extends SqlAbstractLocalSystemView {
    /** Name of the column that contains names of sql tables. */
    private static final String TABLE_NAME = "TABLE_NAME";

    /** Schema manager. */
    private final MonitoringList<String, SqlTableView> tbls;

    /**
     * Creates view with columns.
     *
     * @param ctx kernal context.
     */
    public SqlSystemViewTables(GridKernalContext ctx, SchemaManager schemaMgr) {
        super("TABLES", "Ignite tables", ctx, TABLE_NAME,
            newColumn("CACHE_GROUP_ID", Value.INT),
            newColumn("CACHE_GROUP_NAME"),
            newColumn("CACHE_ID", Value.INT),
            newColumn("CACHE_NAME"),
            newColumn("SCHEMA_NAME"),
            newColumn(TABLE_NAME),
            newColumn("AFFINITY_KEY_COLUMN"),
            newColumn("KEY_ALIAS"),
            newColumn("VALUE_ALIAS"),
            newColumn("KEY_TYPE_NAME"),
            newColumn("VALUE_TYPE_NAME")
        );

        this.tbls = ctx.metric().list(metricName("sql", "tables"), "SQL tables", SqlTableView.class);
    }

    /** {@inheritDoc} */
    @Override public Iterator<Row> getRows(Session ses, SearchRow first, SearchRow last) {
        return F.iterator(tbls, tbl -> createRow(ses,
            tbl.cacheGroupId(),
            tbl.cacheGroupName(),
            tbl.cacheId(),
            tbl.cacheName(),
            tbl.schemaName(),
            tbl.tableName(),
            tbl.affKeyCol(),
            tbl.keyAlias(),
            tbl.valAlias(),
            tbl.keyTypeName(),
            tbl.valTypeName()),
            true);
    }

    /** {@inheritDoc} */
    @Override public boolean canGetRowCount() {
        return true;
    }

    /** {@inheritDoc} */
    @Override public long getRowCount() {
        return tbls.size();
    }
}
