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

import java.lang.management.ManagementFactory;
import java.lang.management.ThreadInfo;
import java.lang.management.ThreadMXBean;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import org.apache.ignite.internal.GridKernalContext;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.h2.engine.Session;
import org.h2.result.Row;
import org.h2.result.SearchRow;
import org.h2.value.Value;
import org.h2.value.ValueNull;

/**
 * System view: ignite instance.
 */
public class GridH2SysViewImplInstance extends GridH2SysView {
    /**
     * @param ctx Grid context.
     */
    public GridH2SysViewImplInstance(GridKernalContext ctx) {
        super("INSTANCE", "Ignite intance", ctx,
            newColumn("INSTANCE_NAME"),
            newColumn("VERSION"),
            newColumn("CLUSTER_LATEST_VERSION"),
            newColumn("ACTIVE", Value.BOOLEAN),
            newColumn("LOCAL_NODE_ID", Value.UUID),
            newColumn("CONFIGURATION")
        );
    }

    /** {@inheritDoc} */
    @Override public Iterable<Row> getRows(Session ses, SearchRow first, SearchRow last) {
        List<Row> rows = new ArrayList<>(1);

        log.debug("Get instance information");

        rows.add(
            createRow(ses, 1L,
                ctx.igniteInstanceName(),
                ctx.grid().version(),
                ctx.grid().latestVersion(),
                ctx.grid().cluster().active(),
                ctx.grid().localNode().id(),
                ctx.grid().configuration().toString()
            )
        );

        return rows;
    }

    /**
     * Converts stack trace to string.
     *
     * @param stackTrace Stack trace.
     */
    private String stackTraceToString(StackTraceElement[] stackTrace) {
        StringBuilder sb = new StringBuilder();

        for (StackTraceElement element : stackTrace ){
            sb.append(element);
            sb.append(U.nl());
        }

        return sb.toString();
    }

    /** {@inheritDoc} */
    @Override public boolean canGetRowCount() {
        return true;
    }

    /** {@inheritDoc} */
    @Override public long getRowCount() {
        return 1;
    }
}
