/*
 * Copyright 2019 GridGain Systems, Inc. and Contributors.
 *
 * Licensed under the GridGain Community Edition License (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     https://www.gridgain.com/products/software/community-edition/gridgain-community-edition-license
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.ignite.internal.processors.query.calcite.serialize;

import java.util.ArrayDeque;
import java.util.Collections;
import java.util.Deque;
import java.util.List;
import org.apache.ignite.internal.processors.query.calcite.rel.IgniteExchange;
import org.apache.ignite.internal.processors.query.calcite.rel.IgniteFilter;
import org.apache.ignite.internal.processors.query.calcite.rel.IgniteJoin;
import org.apache.ignite.internal.processors.query.calcite.rel.IgniteProject;
import org.apache.ignite.internal.processors.query.calcite.rel.IgniteRel;
import org.apache.ignite.internal.processors.query.calcite.rel.IgniteTableScan;
import org.apache.ignite.internal.processors.query.calcite.rel.Receiver;
import org.apache.ignite.internal.processors.query.calcite.rel.Sender;
import org.apache.ignite.internal.processors.query.calcite.util.Commons;
import org.apache.ignite.internal.processors.query.calcite.util.RelImplementor;
import org.apache.ignite.internal.util.typedef.F;

/**
 *
 */
public class RelToGraphConverter {
    private final RexToExpTranslator rexTranslator = new RexToExpTranslator();

    private RelGraph graph;
    private int curParent;

    private static final class Item {
        final int parentId;
        final List<IgniteRel> children;

        private Item(int parentId, List<IgniteRel> children) {
            this.parentId = parentId;
            this.children = children;
        }
    }

    private final class Implementor implements RelImplementor<Item> {
        @Override public Item implement(IgniteFilter rel) {
            return new Item(graph.addNode(curParent, FilterNode.create(rel, rexTranslator)), Commons.cast(rel.getInputs()));
        }

        @Override public Item implement(IgniteJoin rel) {
            return new Item(graph.addNode(curParent, JoinNode.create(rel, rexTranslator)), Commons.cast(rel.getInputs()));
        }

        @Override public Item implement(IgniteProject rel) {
            return new Item(graph.addNode(curParent, ProjectNode.create(rel, rexTranslator)), Commons.cast(rel.getInputs()));
        }

        @Override public Item implement(IgniteTableScan rel) {
            return new Item(graph.addNode(curParent, TableScanNode.create(rel)), Commons.cast(rel.getInputs()));
        }

        @Override public Item implement(Receiver rel) {
            return new Item(graph.addNode(curParent, ReceiverNode.create(rel)), Collections.emptyList());
        }

        @Override public Item implement(Sender rel) {
            return new Item(graph.addNode(curParent, SenderNode.create(rel)), Commons.cast(rel.getInputs()));
        }

        @Override public Item implement(IgniteExchange rel) {
            throw new UnsupportedOperationException();
        }

        @Override public Item implement(IgniteRel other) {
            throw new AssertionError();
        }
    }

    public RelGraph convert(IgniteRel root) {
        graph = new RelGraph();

        Implementor implementor = new Implementor();
        Deque<Item> stack = new ArrayDeque<>();
        stack.push(new Item(-1, F.asList(root)));

        while (!stack.isEmpty()) {
            Item item = stack.pop();

            curParent = item.parentId;

            for (IgniteRel child : item.children) {
                stack.push(child.implement(implementor));
            }
        }

        return graph;
    }
}
