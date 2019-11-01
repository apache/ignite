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

package org.apache.ignite.internal.processors.query.calcite.splitter;

import com.google.common.collect.ImmutableList;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.RelShuttleImpl;
import org.apache.ignite.internal.processors.query.calcite.exchange.Receiver;
import org.apache.ignite.internal.processors.query.calcite.exchange.Sender;
import org.apache.ignite.internal.processors.query.calcite.rel.IgniteRel;
import org.apache.ignite.internal.processors.query.calcite.rel.logical.IgniteLogicalExchange;

/**
 *
 */
public class Splitter extends RelShuttleImpl {
    private ImmutableList.Builder<Fragment> b;

    public QueryPlan go(IgniteRel root) {
        b = ImmutableList.builder();

        return new QueryPlan(b.add(new Fragment(root.accept(this))).build());
    }

    @Override public RelNode visit(RelNode rel) {
        if (!(rel instanceof IgniteRel))
            throw new AssertionError("Unexpected node: " + rel);
        else if (rel instanceof Sender || rel instanceof Receiver)
            throw new AssertionError("An attempt to split an already split task.");
        else if (rel instanceof IgniteLogicalExchange) {
            IgniteLogicalExchange exchange = (IgniteLogicalExchange) rel;

            RelOptCluster cluster = exchange.getCluster();
            RelTraitSet traitSet = exchange.getTraitSet();

            Sender sender = new Sender(cluster, traitSet, visit(exchange.getInput()));

            b.add(new Fragment(sender));

            return new Receiver(cluster, traitSet, sender);
        }

        return super.visit(rel);
    }
}
