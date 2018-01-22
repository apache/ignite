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
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.UUID;
import org.apache.ignite.cluster.ClusterNode;
import org.apache.ignite.internal.GridKernalContext;
import org.h2.engine.Session;
import org.h2.result.Row;
import org.h2.result.SearchRow;
import org.h2.value.Value;

/**
 * System view: node hosts.
 */
public class GridH2SysViewImplNodeHosts extends GridH2SysView {
    /**
     * @param ctx Grid context.
     */
    public GridH2SysViewImplNodeHosts(GridKernalContext ctx) {
        super("NODE_HOSTS", "Node hosts", ctx, "NODE_ID",
            newColumn("NODE_ID", Value.UUID),
            newColumn("HOST_NAME")
        );
    }

    /** {@inheritDoc} */
    @Override public Iterable<Row> getRows(Session ses, SearchRow first, SearchRow last) {
        List<Row> rows = new ArrayList<>();

        Collection<ClusterNode> nodes;

        ColumnCondition idCond = conditionForColumn("NODE_ID", first, last);

        if (idCond.isEquality()) {
            try {
                log.debug("Get node hosts: node id");

                UUID nodeId = UUID.fromString(idCond.getValue().getString());

                nodes = Collections.singleton(ctx.grid().cluster().node(nodeId));
            }
            catch (Exception e) {
                log.warning("Failed to get node by nodeId: " + idCond.getValue().getString(), e);

                nodes = Collections.emptySet();
            }
        }
        else {
            log.debug("Get node hosts: full scan");

            nodes = ctx.grid().cluster().nodes();
        }

        for (ClusterNode node : nodes) {
            if (node != null) {
                // Get unique hosts per node
                Set<String> hosts = new HashSet<>();

                hosts.addAll(node.hostNames());

                for (String host : hosts)
                    rows.add(
                        createRow(ses, rows.size(),
                            node.id(),
                            host
                        )
                    );
            }
        }

        return rows;
    }
}
