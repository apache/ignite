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

package org.apache.ignite.internal.processors.query.h2.views;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import org.apache.ignite.compute.ComputeTaskFuture;
import org.apache.ignite.compute.ComputeTaskSession;
import org.apache.ignite.internal.ComputeTaskInternalFuture;
import org.apache.ignite.internal.GridKernalContext;
import org.apache.ignite.internal.processors.cache.transactions.IgniteInternalTx;
import org.apache.ignite.lang.IgniteUuid;
import org.h2.engine.Session;
import org.h2.result.Row;
import org.h2.result.SearchRow;
import org.h2.value.Value;
import org.h2.value.ValueTimestamp;

/**
 * System view: tasks.
 */
public class GridH2SysViewImplTasks extends GridH2SysView {
    /**
     * @param ctx Grid context.
     */
    public GridH2SysViewImplTasks(GridKernalContext ctx) {
        super("TASKS", "Active tasks", ctx,
            newColumn("ID"),
            newColumn("NAME"),
            newColumn("NODE_ID", Value.UUID),
            newColumn("START_TIME", Value.TIMESTAMP),
            newColumn("END_TIME", Value.TIMESTAMP)
        );
    }

    /** {@inheritDoc} */
    @Override public Iterable<Row> getRows(Session ses, SearchRow first, SearchRow last) {
        List<Row> rows = new ArrayList<>();

        Map<IgniteUuid, ComputeTaskFuture<Object>> tasks = ctx.grid().compute().<Object>activeTaskFutures();

        log.debug("Get tasks: full scan");

        for(ComputeTaskFuture fut : tasks.values()) {
            ComputeTaskSession taskSes;

            try {
                taskSes = fut.getTaskSession();
            }
            catch (Exception e) {
                continue;
            }

            rows.add(
                createRow(ses, rows.size(),
                    taskSes.getId(),
                    taskSes.getTaskName(),
                    taskSes.getTaskNodeId(),
                    valueTimestampFromMillis(taskSes.getStartTime()),
                    valueTimestampFromMillis(taskSes.getEndTime())
                )
            );
        }

        return rows;
    }
}
