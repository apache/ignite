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

import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import org.apache.ignite.internal.GridKernalContext;
import org.apache.ignite.internal.processors.query.GridRunningQueryInfo;
import org.apache.ignite.internal.processors.query.h2.IgniteH2Indexing;
import org.h2.engine.Session;
import org.h2.result.Row;
import org.h2.result.SearchRow;
import org.h2.value.Value;

/**
 * System view: running queries
 */
public class SqlSystemViewRunningQueries extends SqlAbstractLocalSystemView {
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
    }

    /** {@inheritDoc} */
    @Override public Iterator<Row> getRows(Session ses, SearchRow first, SearchRow last) {
        SqlSystemViewColumnCondition qryIdCond = conditionForColumn("QUERY_ID", first, last);

        List<GridRunningQueryInfo> runningSqlQueries = ((IgniteH2Indexing)ctx.query().getIndexing()).runningSqlQueries();

        if (qryIdCond.isEquality()) {
            String qryId = qryIdCond.valueForEquality().getString();

            runningSqlQueries = runningSqlQueries.stream()
                .filter((r) -> r.globalQueryId().equals(qryId))
                .findFirst()
                .map(Collections::singletonList)
                .orElse(Collections.emptyList());
        }

        if (runningSqlQueries.isEmpty())
            return Collections.emptyIterator();

        long now = System.currentTimeMillis();

        List<Row> rows = new ArrayList<>(runningSqlQueries.size());

        for (GridRunningQueryInfo info : runningSqlQueries) {
            long duration = now - info.startTime();

            rows.add(
                createRow(ses,
                    info.globalQueryId(),
                    info.query(),
                    info.schemaName(),
                    info.local(),
                    valueTimestampFromMillis(info.startTime()),
                    duration
                )
            );
        }

        return rows.iterator();
    }

    /** {@inheritDoc} */
    @Override public boolean canGetRowCount() {
        return true;
    }

    /** {@inheritDoc} */
    @Override public long getRowCount() {
        return ((IgniteH2Indexing)ctx.query().getIndexing()).runningSqlQueries().size();
    }
}
