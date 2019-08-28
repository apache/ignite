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

import java.util.Collections;
import java.util.Iterator;
import org.apache.ignite.internal.GridKernalContext;
import org.apache.ignite.internal.processors.metric.GridMetricManager;
import org.apache.ignite.internal.processors.metric.list.MonitoringList;
import org.apache.ignite.spi.metric.list.QueryView;
import org.h2.engine.Session;
import org.h2.result.Row;
import org.h2.result.SearchRow;
import org.h2.value.Value;

import static org.apache.ignite.internal.processors.metric.impl.MetricUtils.metricName;
import static org.apache.ignite.internal.util.lang.GridFunc.iterator;

/**
 * System view: running queries
 *
 * @deprecated Use {@link GridMetricManager} instead.
 */
@Deprecated
public class SqlSystemViewRunningQueries extends SqlAbstractLocalSystemView {
    /** */
    private MonitoringList<Long, QueryView> sqlQryMonList;

    /**
     * @param ctx Grid context.
     */
    public SqlSystemViewRunningQueries(GridKernalContext ctx) {
        super("LOCAL_SQL_RUNNING_QUERIES", "Running queries", ctx, new String[] {"QUERY_ID"},
            newColumn("QUERY_ID"),
            newColumn("SQL"),
            newColumn("SCHEMA_NAME"),
            newColumn("LOCAL", Value.BOOLEAN),
            newColumn("START_TIME", Value.TIMESTAMP),
            newColumn("DURATION", Value.LONG)
        );

        sqlQryMonList = ctx.metric().list(metricName("query", "sql"), "SQL queries", QueryView.class);
    }

    /** {@inheritDoc} */
    @Override public Iterator<Row> getRows(Session ses, SearchRow first, SearchRow last) {
        SqlSystemViewColumnCondition qryIdCond = conditionForColumn("QUERY_ID", first, last);

        long now = System.currentTimeMillis();

        if (qryIdCond.isEquality()) {
            String qryId = qryIdCond.valueForEquality().getString();

            for (QueryView info : sqlQryMonList) {
                if (!info.globalQueryId().equals(qryId))
                    continue;

                return Collections.singletonList(toRow(ses, info, now)).iterator();
            }

            return Collections.emptyIterator();
        }

        return iterator(sqlQryMonList,
            info -> toRow(ses, info, now),
            true);
    }

    /** */
    private Row toRow(Session ses, QueryView info, long now) {
        return createRow(ses,
            info.globalQueryId(),
            info.query(),
            info.schemaName(),
            info.local(),
            valueTimestampFromMillis(info.startTime()),
            now - info.startTime());
    }

    /** {@inheritDoc} */
    @Override public boolean canGetRowCount() {
        return true;
    }

    /** {@inheritDoc} */
    @Override public long getRowCount() {
        return sqlQryMonList.size();
    }
}
