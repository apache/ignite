/*
 * Copyright 2019 GridGain Systems, Inc. and Contributors.
 * 
 * Licensed under the GridGain Community Edition License (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * 
 *     https://www.gridgain.com/products/software/community-edition/gridgain-community-edition-license
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
import java.util.Map;
import org.apache.ignite.internal.GridKernalContext;
import org.apache.ignite.internal.processors.query.QueryHistoryMetrics;
import org.apache.ignite.internal.processors.query.QueryHistoryMetricsKey;
import org.apache.ignite.internal.processors.query.h2.IgniteH2Indexing;
import org.h2.engine.Session;
import org.h2.result.Row;
import org.h2.result.SearchRow;
import org.h2.value.Value;

/**
 * View that contains query history statistics on local node.
 */
public class SqlSystemViewQueryHistoryMetrics extends SqlAbstractLocalSystemView {

   /**
     * Creates view with columns.
     *
     * @param ctx kernal context.
     */
    public SqlSystemViewQueryHistoryMetrics(GridKernalContext ctx) {
        super("LOCAL_SQL_QUERY_HISTORY", "Ignite SQL query history metrics", ctx,
            newColumn("SCHEMA_NAME"),
            newColumn("SQL"),
            newColumn("LOCAL", Value.BOOLEAN),
            newColumn("EXECUTIONS", Value.LONG),
            newColumn("FAILURES", Value.LONG),
            newColumn("DURATION_MIN", Value.LONG),
            newColumn("DURATION_MAX", Value.LONG),
            newColumn("LAST_START_TIME", Value.TIMESTAMP)
        );

    }

    /** {@inheritDoc} */
    @Override public Iterator<Row> getRows(Session ses, SearchRow first, SearchRow last) {
        Map<QueryHistoryMetricsKey, QueryHistoryMetrics> qryHistMetrics =
            ((IgniteH2Indexing)ctx.query().getIndexing()).runningQueryManager().queryHistoryMetrics();

        List<Row> rows = new ArrayList<>();

        qryHistMetrics.values().forEach(m -> {
            Object[] data = new Object[] {
                m.schema(),
                m.query(),
                m.local(),
                m.executions(),
                m.failures(),
                m.minimumTime(),
                m.maximumTime(),
                valueTimestampFromMillis(m.lastStartTime()),
            };

            rows.add(createRow(ses, data));
        });

        return rows.iterator();
    }

    /** {@inheritDoc} */
    @Override public boolean canGetRowCount() {
        return true;
    }

    /** {@inheritDoc} */
    @Override public long getRowCount() {
        return ((IgniteH2Indexing)ctx.query().getIndexing()).runningQueryManager().queryHistoryMetrics().size();
    }
}
