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

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Set;
import org.apache.ignite.cluster.ClusterNode;
import org.apache.ignite.internal.GridKernalContext;
import org.apache.ignite.internal.processors.cluster.BaselineTopology;
import org.apache.ignite.internal.util.typedef.F;
import org.h2.engine.Session;
import org.h2.result.Row;
import org.h2.result.SearchRow;
import org.h2.value.Value;

/**
 * System view: baseline nodes.
 */
public class SqlSystemViewBaselineNodes extends SqlAbstractLocalSystemView {
    /**
     * @param ctx Grid context.
     */
    public SqlSystemViewBaselineNodes(GridKernalContext ctx) {
        super("BASELINE_NODES", "Baseline topology nodes", ctx,
            newColumn("CONSISTENT_ID"),
            newColumn("ONLINE", Value.BOOLEAN)
        );
    }

    /** {@inheritDoc} */
    @Override public Iterator<Row> getRows(Session ses, SearchRow first, SearchRow last) {
        List<Row> rows = new ArrayList<>();

        BaselineTopology blt = ctx.state().clusterState().baselineTopology();

        if (blt == null)
            return rows.iterator();

        Set<Object> consistentIds = blt.consistentIds();

        Collection<ClusterNode> srvNodes = ctx.discovery().aliveServerNodes();

        Set<Object> aliveNodeIds = new HashSet<>(F.nodeConsistentIds(srvNodes));

        for (Object consistentId : consistentIds) {
            rows.add(
                createRow(ses, rows.size(),
                    consistentId,
                    aliveNodeIds.contains(consistentId)
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
        BaselineTopology blt = ctx.state().clusterState().baselineTopology();

        return blt == null ? 0 : blt.consistentIds().size();
    }
}
