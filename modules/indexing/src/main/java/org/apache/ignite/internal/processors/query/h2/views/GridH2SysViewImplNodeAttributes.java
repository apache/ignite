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
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import org.apache.ignite.cluster.ClusterNode;
import org.apache.ignite.internal.GridKernalContext;
import org.apache.ignite.lang.IgniteBiClosure;
import org.apache.ignite.lang.IgniteClosure;
import org.h2.engine.Session;
import org.h2.result.Row;
import org.h2.result.SearchRow;
import org.h2.value.Value;

/**
 * System view: node attributes.
 */
public class GridH2SysViewImplNodeAttributes extends GridH2SysView {
    /**
     * @param ctx Grid context.
     */
    public GridH2SysViewImplNodeAttributes(GridKernalContext ctx) {
        super("NODE_ATTRIBUTES", "Node attributes", ctx, new String[] {"NODE_ID,NAME", "NAME"},
            newColumn("NODE_ID", Value.UUID),
            newColumn("NAME"),
            newColumn("VALUE")
        );
    }

    /** {@inheritDoc} */
    @Override public Iterable<Row> getRows(Session ses, SearchRow first, SearchRow last) {
        Collection<ClusterNode> nodes;

        ColumnCondition idCond = conditionForColumn("NODE_ID", first, last);
        ColumnCondition nameCond = conditionForColumn("NAME", first, last);

        if (idCond.isEquality()) {
            try {
                log.debug("Get node attributes: node id");

                UUID nodeId = UUID.fromString(idCond.getValue().getString());

                ClusterNode node = ctx.grid().cluster().node(nodeId);

                if (node != null)
                    nodes = Collections.singleton(node);
                else
                    nodes = Collections.emptySet();
            }
            catch (Exception e) {
                log.warning("Failed to get node by nodeId: " + idCond.getValue().getString(), e);

                nodes = Collections.emptySet();
            }
        }
        else {
            log.debug("Get node attributes: nodes full scan");

            nodes = ctx.grid().cluster().nodes();
        }

        if (nameCond.isEquality()) {
            log.debug("Get node attributes: attribute name");

            String attrName = nameCond.getValue().getString();

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

            return rows;
        }
        else {
            log.debug("Get node attributes: attributes full scan");

            return new ParentChildRowIterable<ClusterNode, Map.Entry<String, Object>>(ses, nodes,
                new IgniteClosure<ClusterNode, Iterator<Map.Entry<String, Object>>>() {
                    @Override public Iterator<Map.Entry<String, Object>> apply(ClusterNode node) {
                        return node.attributes().entrySet().iterator();
                    }
                },
                new IgniteBiClosure<ClusterNode, Map.Entry<String, Object>, Object[]>() {
                @Override public Object[] apply(ClusterNode node, Map.Entry<String, Object> attr) {
                    return new Object[] {
                        node.id(),
                        attr.getKey(),
                        attr.getValue()
                    };
                }
            });
        }
    }
}
