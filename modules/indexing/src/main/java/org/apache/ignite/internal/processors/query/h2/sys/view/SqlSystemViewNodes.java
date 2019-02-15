/*
 *                   GridGain Community Edition Licensing
 *                   Copyright 2019 GridGain Systems, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License") modified with Commons Clause
 * Restriction; you may not use this file except in compliance with the License. You may obtain a
 * copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the
 * License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied. See the License for the specific language governing permissions
 * and limitations under the License.
 *
 * Commons Clause Restriction
 *
 * The Software is provided to you by the Licensor under the License, as defined below, subject to
 * the following condition.
 *
 * Without limiting other conditions in the License, the grant of rights under the License will not
 * include, and the License does not grant to you, the right to Sell the Software.
 * For purposes of the foregoing, “Sell” means practicing any or all of the rights granted to you
 * under the License to provide to third parties, for a fee or other consideration (including without
 * limitation fees for hosting or consulting/ support services related to the Software), a product or
 * service whose value derives, entirely or substantially, from the functionality of the Software.
 * Any license notice or attribution required by the License must also include this Commons Clause
 * License Condition notice.
 *
 * For purposes of the clause above, the “Licensor” is Copyright 2019 GridGain Systems, Inc.,
 * the “License” is the Apache License, Version 2.0, and the Software is the GridGain Community
 * Edition software provided with this notice.
 */

package org.apache.ignite.internal.processors.query.h2.sys.view;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Iterator;
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
 * System view: nodes.
 */
public class SqlSystemViewNodes extends SqlAbstractLocalSystemView {
    /**
     * @param ctx Grid context.
     */
    public SqlSystemViewNodes(GridKernalContext ctx) {
        super("NODES", "Topology nodes", ctx, "ID",
            newColumn("ID", Value.UUID),
            newColumn("CONSISTENT_ID"),
            newColumn("VERSION"),
            newColumn("IS_CLIENT", Value.BOOLEAN),
            newColumn("IS_DAEMON", Value.BOOLEAN),
            newColumn("NODE_ORDER", Value.INT),
            newColumn("ADDRESSES"),
            newColumn("HOSTNAMES")
        );
    }

    /** {@inheritDoc} */
    @Override public Iterator<Row> getRows(Session ses, SearchRow first, SearchRow last) {
        List<Row> rows = new ArrayList<>();

        Collection<ClusterNode> nodes;

        SqlSystemViewColumnCondition idCond = conditionForColumn("ID", first, last);

        if (idCond.isEquality()) {
            UUID nodeId = uuidFromValue(idCond.valueForEquality());

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
                        node.isClient(),
                        node.isDaemon(),
                        node.order(),
                        node.addresses(),
                        node.hostNames()
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
        return ctx.discovery().allNodes().size() + ctx.discovery().daemonNodes().size();
    }
}
