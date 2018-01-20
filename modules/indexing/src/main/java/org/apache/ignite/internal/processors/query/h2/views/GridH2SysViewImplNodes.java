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
import java.util.Collections;
import java.util.List;
import java.util.UUID;
import org.apache.ignite.cluster.ClusterNode;
import org.apache.ignite.internal.GridKernalContext;
import org.apache.ignite.internal.processors.cache.IgniteInternalCache;
import org.h2.engine.Session;
import org.h2.result.Row;
import org.h2.result.SearchRow;
import org.h2.value.Value;

/**
 * Sys view caches.
 */
public class GridH2SysViewImplNodes extends GridH2SysView {
    /**
     * @param ctx Grid context.
     */
    public GridH2SysViewImplNodes(GridKernalContext ctx) {
        super("NODES", "Nodes in topology", ctx, new String[] {"ID", "IS_LOCAL"},
            newColumn("ID", Value.UUID),
            newColumn("CONSISTENT_ID"),
            newColumn("IS_LOCAL", Value.BOOLEAN),
            newColumn("IS_CLIENT", Value.BOOLEAN),
            newColumn("IS_DAEMON", Value.BOOLEAN)
        );
    }

    /** {@inheritDoc} */
    @Override public Iterable<Row> getRows(Session ses, SearchRow first, SearchRow last) {
        List<Row> rows = new ArrayList<>();

        Collection<ClusterNode> nodes;

        ColumnCondition locCond = conditionForColumn("IS_LOCAL", first, last);
        ColumnCondition idCond = conditionForColumn("ID", first, last);

        if (locCond.isEquality() && locCond.getValue().getBoolean())
            nodes = Collections.singleton(ctx.discovery().localNode());
        else if (idCond.isEquality()) {
            try {
                UUID nodeId = UUID.fromString(idCond.getValue().getString());

                nodes = Collections.singleton(ctx.discovery().node(nodeId));
            }
            catch (Exception e) {
                nodes = Collections.emptySet();
            }
        }
        else
            nodes = ctx.discovery().allNodes();

        for(ClusterNode node : nodes) {
            if (node != null)
                rows.add(
                    createRow(ses, rows.size(),
                        node.id(),
                        node.consistentId(),
                        node.isLocal(),
                        node.isClient(),
                        node.isDaemon()
                    )
                );
        }

        return rows;
    }

    /** {@inheritDoc} */
    @Override public boolean canGetRowCount() {
        return true;
    }

    /** {@inheritDoc} */
    @Override public long getRowCount() {
        return ctx.discovery().allNodes().size();
    }
}
