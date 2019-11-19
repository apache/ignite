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
import java.util.Deque;
import java.util.List;
import org.apache.calcite.util.Pair;
import org.apache.ignite.internal.processors.query.calcite.rel.IgniteExchange;
import org.apache.ignite.internal.processors.query.calcite.rel.IgniteFilter;
import org.apache.ignite.internal.processors.query.calcite.rel.IgniteJoin;
import org.apache.ignite.internal.processors.query.calcite.rel.IgniteProject;
import org.apache.ignite.internal.processors.query.calcite.rel.IgniteRel;
import org.apache.ignite.internal.processors.query.calcite.rel.IgniteTableScan;
import org.apache.ignite.internal.processors.query.calcite.rel.Receiver;
import org.apache.ignite.internal.processors.query.calcite.rel.Sender;
import org.apache.ignite.internal.processors.query.calcite.util.RelImplementor;

/**
 *
 */
public class RelToGraphConverter implements RelImplementor<Pair<Integer, List<IgniteRel>>> {
    private Deque<Pair<Integer, List<IgniteRel>>> stack = new ArrayDeque<>();
    private Graph graph;
    private int parentId;

    public Graph convert(IgniteRel root) {
        stack = new ArrayDeque<>();
        graph = new Graph();
        parentId = -1;

        stack.push(root.implement(this));

        while (!stack.isEmpty()) {
            Pair<Integer, List<IgniteRel>> pair = stack.pop();

            parentId = pair.left;

            for (IgniteRel child : pair.right) {
                stack.push(child.implement(this));
            }
        }

        return graph;
    }

    @Override public Pair<Integer, List<IgniteRel>> implement(IgniteFilter rel) {
        return null;
    }

    @Override public Pair<Integer, List<IgniteRel>> implement(IgniteJoin rel) {
        return null;
    }

    @Override public Pair<Integer, List<IgniteRel>> implement(IgniteProject rel) {
        return null;
    }

    @Override public Pair<Integer, List<IgniteRel>> implement(IgniteTableScan rel) {
        return null;
    }

    @Override public Pair<Integer, List<IgniteRel>> implement(Receiver rel) {
        return null;
    }

    @Override public Pair<Integer, List<IgniteRel>> implement(Sender rel) {
        assert parentId == -1;

        return null;
    }

    @Override public Pair<Integer, List<IgniteRel>> implement(IgniteExchange rel) {
        throw new UnsupportedOperationException();
    }

    @Override public Pair<Integer, List<IgniteRel>> implement(IgniteRel other) {
        throw new AssertionError();
    }
}
