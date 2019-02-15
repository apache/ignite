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
import java.util.concurrent.atomic.AtomicLong;
import org.apache.ignite.cluster.ClusterNode;
import org.apache.ignite.internal.GridKernalContext;
import org.apache.ignite.internal.util.typedef.F;
import org.h2.engine.Session;
import org.h2.result.Row;
import org.h2.result.SearchRow;
import org.h2.value.Value;

/**
 * System view: node attributes.
 */
public class SqlSystemViewNodeAttributes extends SqlAbstractLocalSystemView {
    /**
     * @param ctx Grid context.
     */
    public SqlSystemViewNodeAttributes(GridKernalContext ctx) {
        super("NODE_ATTRIBUTES", "Node attributes", ctx, new String[] {"NODE_ID,NAME", "NAME"},
            newColumn("NODE_ID", Value.UUID),
            newColumn("NAME"),
            newColumn("VALUE")
        );
    }

    /** {@inheritDoc} */
    @SuppressWarnings("unchecked")
    @Override public Iterator<Row> getRows(Session ses, SearchRow first, SearchRow last) {
        Collection<ClusterNode> nodes;

        SqlSystemViewColumnCondition idCond = conditionForColumn("NODE_ID", first, last);
        SqlSystemViewColumnCondition nameCond = conditionForColumn("NAME", first, last);

        if (idCond.isEquality()) {
            try {
                UUID nodeId = uuidFromValue(idCond.valueForEquality());

                ClusterNode node = nodeId == null ? null : ctx.discovery().node(nodeId);

                if (node != null)
                    nodes = Collections.singleton(node);
                else
                    nodes = Collections.emptySet();
            }
            catch (Exception e) {
                nodes = Collections.emptySet();
            }
        }
        else
            nodes = F.concat(false, ctx.discovery().allNodes(), ctx.discovery().daemonNodes());

        if (nameCond.isEquality()) {
            String attrName = nameCond.valueForEquality().getString();

            List<Row> rows = new ArrayList<>();

            for (ClusterNode node : nodes) {
                if (node.attributes().containsKey(attrName))
                    rows.add(
                        createRow(ses, rows.size(),
                            node.id(),
                            attrName,
                            node.attribute(attrName)
                        )
                    );
            }

            return rows.iterator();
        }
        else {
            AtomicLong rowKey = new AtomicLong();

            return F.concat(F.iterator(nodes,
                node -> F.iterator(node.attributes().entrySet(),
                    attr -> createRow(ses,
                        rowKey.incrementAndGet(),
                        node.id(),
                        attr.getKey(),
                        attr.getValue()),
                    true).iterator(),
                true));
        }
    }
}
