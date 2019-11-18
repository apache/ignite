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
import org.apache.calcite.rel.RelNode;
import org.apache.ignite.internal.processors.query.calcite.rel.IgniteExchange;
import org.apache.ignite.internal.processors.query.calcite.rel.IgniteFilter;
import org.apache.ignite.internal.processors.query.calcite.rel.IgniteJoin;
import org.apache.ignite.internal.processors.query.calcite.rel.IgniteProject;
import org.apache.ignite.internal.processors.query.calcite.rel.IgniteRel;
import org.apache.ignite.internal.processors.query.calcite.rel.IgniteTableScan;
import org.apache.ignite.internal.processors.query.calcite.rel.Receiver;
import org.apache.ignite.internal.processors.query.calcite.rel.Sender;
import org.apache.ignite.internal.processors.query.calcite.util.Implementor;

/**
 *
 */
public class RelToGraphConverter implements Implementor<List<RelNode>> {
    private Deque<List<RelNode>> stack1 = new ArrayDeque<>();
    private Deque<Integer> stack2 = new ArrayDeque<>();
    private Graph graph;

    public Graph convert(RelNode root) {
        stack1 = new ArrayDeque<>();
        stack2 = new ArrayDeque<>();

        graph = new Graph();

        return null;
    }

    @Override public List<RelNode> implement(IgniteExchange rel) {
        return null;
    }

    @Override public List<RelNode> implement(IgniteFilter rel) {
        return null;
    }

    @Override public List<RelNode> implement(IgniteJoin rel) {
        return null;
    }

    @Override public List<RelNode> implement(IgniteProject rel) {
        return null;
    }

    @Override public List<RelNode> implement(IgniteTableScan rel) {
        return null;
    }

    @Override public List<RelNode> implement(Receiver rel) {
        return null;
    }

    @Override public List<RelNode> implement(Sender rel) {
        return null;
    }

    @Override public List<RelNode> implement(IgniteRel other) {
        return null;
    }
}
