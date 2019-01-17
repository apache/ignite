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
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.UUID;
import org.apache.ignite.internal.GridKernalContext;
import org.apache.ignite.internal.processors.query.GridRunningQueryInfo;
import org.h2.engine.Session;
import org.h2.result.Row;
import org.h2.result.SearchRow;
import org.h2.value.Value;

/**
 * System view: running queries
 */
public class SqlSystemViewRunningQueries extends SqlAbstractLocalSystemView {

    /** Query id separator. */
    private static final String QRY_ID_SEPARATOR = "X";

    /**
     * @param ctx Grid context.
     */
    public SqlSystemViewRunningQueries(GridKernalContext ctx) {
        super("RUNNING_QUERIES", "Running queries", ctx, new String[] {"QUERY_ID", "DURATION"},
            newColumn("QUERY_ID"),
            newColumn("NODE_ID", Value.UUID),
            newColumn("SQL"),
            newColumn("SCHEMA_NAME"),
            newColumn("DURATION", Value.LONG)
        );
    }

    /** {@inheritDoc} */
    @Override public Iterator<Row> getRows(Session ses, SearchRow first, SearchRow last) {
        SqlSystemViewColumnCondition durationCond = conditionForColumn("DURATION", first, last);

        SqlSystemViewColumnCondition qryIdCond = conditionForColumn("QUERY_ID", first, last);

        long durationRange = -1;

        if (durationCond.isRange() && durationCond.valueForStartRange() != null)
            durationRange = durationCond.valueForStartRange().getLong();

        Long qryId0 = null;

        if (qryIdCond.isEquality()) {
            String fullQryId = qryIdCond.valueForEquality().getString();

            String[] split = fullQryId.split(QRY_ID_SEPARATOR);

            if (split.length == 2) {
                try {
                    qryId0 = Long.valueOf(split[1], 16);
                }
                catch (NumberFormatException ex) {
                    return Collections.emptyIterator();
                }
            }
            else
                return Collections.emptyIterator();
        }

        Collection<GridRunningQueryInfo> runningQueries = ctx.query().runningQueries(durationRange);

        if (qryId0 != null) {
            final Long qryId = qryId0;

            runningQueries = runningQueries.stream()
                .filter((r) -> r.id().equals(qryId))
                .findFirst()
                .map(Arrays::asList)
                .orElse(Collections.emptyList());
        }

        if (runningQueries.isEmpty())
            return Collections.emptyIterator();

        UUID nodeId = ctx.localNodeId();

        long nodeOrder = ctx.cluster().get().localNode().order();

        String clusterPartOfQryId = Long.toHexString(nodeOrder) + QRY_ID_SEPARATOR;

        long now = System.currentTimeMillis();

        List<Row> rows = new ArrayList<>(runningQueries.size());

        for (GridRunningQueryInfo info : runningQueries) {
            String qryId = clusterPartOfQryId + Long.toHexString(info.id());

            long duration = now - info.startTime();

            rows.add(
                createRow(ses, rows.size(),
                    qryId,
                    nodeId,
                    info.query(),
                    info.schemaName(),
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
        return ctx.query().runningQueries(-1).size();
    }
}
