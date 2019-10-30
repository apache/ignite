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
import java.util.List;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.RelNode;
import org.apache.ignite.internal.processors.query.calcite.exchange.Receiver;
import org.apache.ignite.internal.processors.query.calcite.exchange.Sender;
import org.apache.ignite.internal.processors.query.calcite.rel.IgniteRel;
import org.apache.ignite.internal.processors.query.calcite.rel.IgniteVisitor;
import org.apache.ignite.internal.processors.query.calcite.rel.logical.IgniteLogicalExchange;
import org.apache.ignite.internal.processors.query.calcite.rel.logical.IgniteLogicalFilter;
import org.apache.ignite.internal.processors.query.calcite.rel.logical.IgniteLogicalJoin;
import org.apache.ignite.internal.processors.query.calcite.rel.logical.IgniteLogicalProject;
import org.apache.ignite.internal.processors.query.calcite.rel.logical.IgniteLogicalTableScan;

/**
 *
 */
public class Splitter implements IgniteVisitor<IgniteRel> {
    private ImmutableList.Builder<Fragment> b;

    public QueryPlan go(IgniteRel root) {
        b = ImmutableList.builder();

        return new QueryPlan(b.add(new Fragment(root.accept(this))).build());
    }

    @Override public IgniteRel visitExchange(IgniteLogicalExchange exchange) {
        RelOptCluster cluster = exchange.getCluster();
        RelTraitSet traitSet = exchange.getTraitSet();

        IgniteRel input = visitChildren(exchange.getInput());

        Sender sender = new Sender(cluster, traitSet, input);

        b.add(new Fragment(sender));

        return new Receiver(cluster, traitSet, sender);
    }

    @Override public IgniteRel visitFilter(IgniteLogicalFilter filter) {
        return visitChildren(filter);
    }

    @Override public IgniteRel visitJoin(IgniteLogicalJoin join) {
        return visitChildren(join);
    }

    @Override public IgniteRel visitProject(IgniteLogicalProject project) {
        return visitChildren(project);
    }

    @Override public IgniteRel visitTableScan(IgniteLogicalTableScan tableScan) {
        return tableScan;
    }

    @Override public IgniteRel visitReceiver(Receiver receiver) {
        throw new AssertionError("An attempt to split an already split task.");
    }

    @Override public IgniteRel visitSender(Sender sender) {
        throw new AssertionError("An attempt to split an already split task.");
    }

    private IgniteRel visitChildren(RelNode parent) {
        List<RelNode> inputs = parent.getInputs();

        for (int i = 0; i < inputs.size(); i++) {
            IgniteRel input = (IgniteRel) inputs.get(i);
            IgniteRel rel = input.accept(this);

            if (rel != input)
                parent.replaceInput(i, rel);
        }
        return (IgniteRel) parent;
    }
}
