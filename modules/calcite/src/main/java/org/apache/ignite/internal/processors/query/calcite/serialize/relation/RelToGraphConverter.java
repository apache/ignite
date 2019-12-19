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

package org.apache.ignite.internal.processors.query.calcite.serialize.relation;

import java.util.ArrayDeque;
import java.util.Collections;
import java.util.Deque;
import java.util.List;
import org.apache.ignite.internal.processors.query.calcite.rel.IgniteExchange;
import org.apache.ignite.internal.processors.query.calcite.rel.IgniteFilter;
import org.apache.ignite.internal.processors.query.calcite.rel.IgniteJoin;
import org.apache.ignite.internal.processors.query.calcite.rel.IgniteProject;
import org.apache.ignite.internal.processors.query.calcite.rel.IgniteReceiver;
import org.apache.ignite.internal.processors.query.calcite.rel.IgniteRel;
import org.apache.ignite.internal.processors.query.calcite.rel.IgniteRelVisitor;
import org.apache.ignite.internal.processors.query.calcite.rel.IgniteSender;
import org.apache.ignite.internal.processors.query.calcite.rel.IgniteTableScan;
import org.apache.ignite.internal.processors.query.calcite.rel.RelOp;
import org.apache.ignite.internal.processors.query.calcite.serialize.expression.RexToExpTranslator;
import org.apache.ignite.internal.processors.query.calcite.util.Commons;
import org.apache.ignite.internal.util.typedef.F;

/**
 * Converts RelNode tree to RelGraph.
 */
public class RelToGraphConverter implements RelOp<IgniteRel, RelGraph> {
    /** */
    private final RexToExpTranslator rexTranslator = new RexToExpTranslator();

    /** */
    private RelGraph graph;

    /** Parent ID of currently processed node. */
    private int curParent;

    /** */
    private static final class Item {
        /** */
        private final int parentId;

        /** */
        private final List<IgniteRel> children;

        /** */
        private Item(int parentId, List<IgniteRel> children) {
            this.parentId = parentId;
            this.children = children;
        }
    }

    /**
     * Adds a representation of input node to the graph and returns node id in the graph and its children for future processing.
     */
    private final class ItemTranslator implements IgniteRelVisitor<Item> {
        /** {@inheritDoc} */
        @Override public Item visit(IgniteFilter rel) {
            return new Item(graph.addNode(curParent, FilterNode.create(rel, rexTranslator)), Commons.cast(rel.getInputs()));
        }

        /** {@inheritDoc} */
        @Override public Item visit(IgniteJoin rel) {
            return new Item(graph.addNode(curParent, JoinNode.create(rel, rexTranslator)), Commons.cast(rel.getInputs()));
        }

        /** {@inheritDoc} */
        @Override public Item visit(IgniteProject rel) {
            return new Item(graph.addNode(curParent, ProjectNode.create(rel, rexTranslator)), Commons.cast(rel.getInputs()));
        }

        /** {@inheritDoc} */
        @Override public Item visit(IgniteTableScan rel) {
            return new Item(graph.addNode(curParent, TableScanNode.create(rel)), Commons.cast(rel.getInputs()));
        }

        /** {@inheritDoc} */
        @Override public Item visit(IgniteReceiver rel) {
            return new Item(graph.addNode(curParent, ReceiverNode.create(rel)), Collections.emptyList());
        }

        /** {@inheritDoc} */
        @Override public Item visit(IgniteSender rel) {
            return new Item(graph.addNode(curParent, SenderNode.create(rel)), Commons.cast(rel.getInputs()));
        }

        /** {@inheritDoc} */
        @Override public Item visit(IgniteRel rel) {
            return rel.accept(this);
        }

        /** {@inheritDoc} */
        @Override public Item visit(IgniteExchange rel) {
            throw new AssertionError("Unexpected node: " + rel);
        }
    }

    /** {@inheritDoc} */
    @Override public RelGraph go(IgniteRel root) {
        graph = new RelGraph();

        ItemTranslator itemTranslator = new ItemTranslator();
        Deque<Item> stack = new ArrayDeque<>();
        stack.push(new Item(-1, F.asList(root)));

        while (!stack.isEmpty()) {
            Item item = stack.pop();

            curParent = item.parentId;

            for (IgniteRel child : item.children)
                stack.push(itemTranslator.visit(child));
        }

        return graph;
    }
}
