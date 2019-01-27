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
                if (node.attributes().containsKey(attrName)) {
                    rows.add(
                        createRow(
                            ses,
                            node.id(),
                            attrName,
                            node.attribute(attrName)
                        )
                    );
                }
            }

            return rows.iterator();
        }
        else {
            return F.concat(F.iterator(nodes,
                node -> F.iterator(node.attributes().entrySet(),
                    attr -> createRow(
                        ses,
                        node.id(),
                        attr.getKey(),
                        attr.getValue()),
                    true).iterator(),
                true)
            );
        }
    }
}
