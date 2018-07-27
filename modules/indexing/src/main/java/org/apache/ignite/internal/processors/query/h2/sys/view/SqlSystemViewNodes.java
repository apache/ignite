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
import java.util.Collections;
import java.util.List;
import java.util.UUID;
import org.apache.ignite.cluster.ClusterNode;
import org.apache.ignite.internal.GridKernalContext;
import org.apache.ignite.internal.util.typedef.F;
import org.h2.engine.Session;
import org.h2.result.Row;
import org.h2.result.SearchRow;
import org.h2.value.Value;

/**
 * Meta view: nodes.
 */
public class SqlSystemViewNodes extends SqlAbstractLocalSystemView {
    /**
     * @param ctx Grid context.
     */
    public SqlSystemViewNodes(GridKernalContext ctx) {
        super("NODES", "Topology nodes", ctx, new String[] {"ID", "IS_LOCAL"},
            newColumn("ID", Value.UUID),
            newColumn("CONSISTENT_ID"),
            newColumn("VERSION"),
            newColumn("IS_LOCAL", Value.BOOLEAN),
            newColumn("IS_CLIENT", Value.BOOLEAN),
            newColumn("IS_DAEMON", Value.BOOLEAN),
            newColumn("NODE_ORDER", Value.INT),
            newColumn("ADDRESSES"),
            newColumn("HOSTNAMES")
        );
    }

    /** {@inheritDoc} */
    @Override public Iterable<Row> getRows(Session ses, SearchRow first, SearchRow last) {
        List<Row> rows = new ArrayList<>();

        Collection<ClusterNode> nodes;

        SqlSystemViewColumnCondition locCond = conditionForColumn("IS_LOCAL", first, last);
        SqlSystemViewColumnCondition idCond = conditionForColumn("ID", first, last);

        if (locCond.isEquality() && locCond.valueForEquality().getBoolean())
            nodes = Collections.singleton(ctx.discovery().localNode());
        else if (idCond.isEquality()) {
            UUID nodeId = uuidFromString(idCond.valueForEquality().getString());

            nodes = nodeId == null ? Collections.emptySet() : Collections.singleton(ctx.discovery().node(nodeId));
        }
        else
            nodes = F.concat(false, ctx.discovery().allNodes(), ctx.discovery().daemonNodes());

        for (ClusterNode node : nodes) {
            if (node != null)
                rows.add(
                    createRow(ses, rows.size(),
                        node.id(),
                        node.consistentId(),
                        node.version(),
                        node.isLocal(),
                        node.isClient(),
                        node.isDaemon(),
                        node.order(),
                        node.addresses(),
                        node.hostNames()
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
        return ctx.discovery().allNodes().size() + ctx.discovery().daemonNodes().size();
    }

    /**
     * Converts string to UUID safe (suppressing exceptions).
     *
     * @param val UUID in string format.
     */
    private static UUID uuidFromString(String val) {
        try {
            return UUID.fromString(val);
        }
        catch (RuntimeException e) {
            return null;
        }
    }
}
