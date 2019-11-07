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

package org.apache.ignite.internal.processors.query.calcite.util;

import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.RelShuttleImpl;
import org.apache.ignite.internal.processors.query.calcite.rel.IgniteExchange;
import org.apache.ignite.internal.processors.query.calcite.rel.IgniteFilter;
import org.apache.ignite.internal.processors.query.calcite.rel.IgniteHashJoin;
import org.apache.ignite.internal.processors.query.calcite.rel.IgniteProject;
import org.apache.ignite.internal.processors.query.calcite.rel.IgniteTableScan;
import org.apache.ignite.internal.processors.query.calcite.rel.Receiver;
import org.apache.ignite.internal.processors.query.calcite.rel.Sender;

/**
 *
 */
public class IgniteRelShuttle extends RelShuttleImpl {
    public RelNode visit(IgniteExchange rel) {
        return visitChild(rel, 0, rel.getInput());
    }

    public RelNode visit(IgniteFilter rel) {
        return visitChild(rel, 0, rel.getInput());
    }

    public RelNode visit(IgniteProject rel) {
        return visitChild(rel, 0, rel.getInput());
    }

    public RelNode visit(Receiver rel) {
        return visitChild(rel, 0, rel.getInput());
    }

    public RelNode visit(Sender rel) {
        return visitChild(rel, 0, rel.getInput());
    }

    public RelNode visit(IgniteTableScan rel) {
        return rel;
    }

    public RelNode visit(IgniteHashJoin rel) {
        return visitChildren(rel);
    }

    @Override public RelNode visit(RelNode rel) {
        if (rel instanceof IgniteExchange)
            return visit((IgniteExchange)rel);
        if (rel instanceof IgniteFilter)
            return visit((IgniteFilter)rel);
        if (rel instanceof IgniteProject)
            return visit((IgniteProject)rel);
        if (rel instanceof Receiver)
            return visit((Receiver)rel);
        if (rel instanceof Sender)
            return visit((Sender)rel);
        if (rel instanceof IgniteTableScan)
            return visit((IgniteTableScan)rel);
        if (rel instanceof IgniteHashJoin)
            return visit((IgniteHashJoin)rel);

        return visitOther(rel);
    }

    protected RelNode visitOther(RelNode rel) {
        return super.visit(rel);
    }
}
