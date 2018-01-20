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

import java.util.Collection;
import java.util.Collections;
import java.util.Iterator;
import java.util.Map;
import java.util.UUID;
import org.apache.ignite.cluster.ClusterNode;
import org.apache.ignite.internal.GridKernalContext;
import org.h2.engine.Session;
import org.h2.result.Row;
import org.h2.result.SearchRow;
import org.h2.value.Value;
import org.jetbrains.annotations.NotNull;

/**
 * System view: node attributes.
 */
public class GridH2SysViewImplNodeAttributes extends GridH2SysView {
    /**
     * @param ctx Grid context.
     */
    public GridH2SysViewImplNodeAttributes(GridKernalContext ctx) {
        super("NODE_ATTRIBUTES", "Node attributes", ctx, "NODE_ID",
            newColumn("NODE_ID", Value.UUID),
            newColumn("NAME"),
            newColumn("VALUE")
        );
    }

    /** {@inheritDoc} */
    @Override public Iterable<Row> getRows(Session ses, SearchRow first, SearchRow last) {
        Collection<ClusterNode> nodes;

        ColumnCondition idCond = conditionForColumn("NODE_ID", first, last);

        if (idCond.isEquality()) {
            try {
                log.debug("Get node attributes: node id");

                UUID nodeId = UUID.fromString(idCond.getValue().getString());

                nodes = Collections.singleton(ctx.discovery().node(nodeId));
            }
            catch (Exception e) {
                log.warning("Failed to get node by nodeId: " + idCond.getValue().getString(), e);

                nodes = Collections.emptySet();
            }
        }
        else {
            log.debug("Get node attributes: full scan");

            nodes = ctx.discovery().allNodes();
        }

        return new NodeAttributesIterable(ses, nodes);
    }

    /**
     * Node attributes iterable.
     */
    private class NodeAttributesIterable implements Iterable<Row> {
        /** Session. */
        private final Session ses;

        /** Nodes. */
        private final Iterable<ClusterNode> nodes;

        /**
         * @param ses Session.
         * @param nodes Nodes.
         */
        public NodeAttributesIterable(Session ses, Iterable<ClusterNode> nodes) {
            this.ses = ses;
            this.nodes = nodes;
        }

        /** {@inheritDoc} */
        @NotNull @Override public Iterator<Row> iterator() {
            return new NodeAttributesIterator(ses, nodes.iterator());
        }
    }

    /**
     * Node attributes iterator.
     */
    private class NodeAttributesIterator implements Iterator<Row> {
        /** Session. */
        private final Session ses;

        /** Node iterator. */
        private final Iterator<ClusterNode> nodeIter;

        /** Attribute iterator. */
        private Iterator<Map.Entry<String, Object>> attrIter;

        /** Counter. */
        private long rowCnt = 0;

        /** Next node. */
        private ClusterNode nextNode;

        /** Next attribute. */
        private Map.Entry<String, Object> nextAttr;

        /**
         * @param nodeIter Node iterator.
         */
        public NodeAttributesIterator(Session ses, Iterator<ClusterNode> nodeIter) {
            this.ses = ses;
            this.nodeIter = nodeIter;

            moveAttr();
        }

        /**
         * Move to next node.
         */
        private void moveNode() {
            nextNode = nodeIter.next();

            attrIter = nextNode.attributes().entrySet().iterator();
        }

        /**
         * Move to next attribute.
         */
        private void moveAttr() {
            // First iteration.
            if (nextNode == null && nodeIter.hasNext())
                moveNode();

            // Empty nodes at first iteration.
            if (attrIter == null)
                return;

            while (attrIter.hasNext() || nodeIter.hasNext()) {
                if (attrIter.hasNext()) {
                    nextAttr = attrIter.next();
                    rowCnt++;

                    return;
                }
                else
                    moveNode();
            }

            nextAttr = null;
        }

        /** {@inheritDoc} */
        @Override public boolean hasNext() {
            return nextAttr != null;
        }

        /** {@inheritDoc} */
        @Override public Row next() {
            if (nextAttr == null)
                return null;

            Row row = GridH2SysViewImplNodeAttributes.this.createRow(ses, rowCnt,
                nextNode.id(),
                nextAttr.getKey(),
                nextAttr.getValue()
            );

            moveAttr();

            return row;
        }

        /** {@inheritDoc} */
        @Override public void remove() {
            throw new UnsupportedOperationException();
        }
    }
}
