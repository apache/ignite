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

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.RelNode;
import org.apache.ignite.internal.processors.query.calcite.rel.IgniteExchange;
import org.apache.ignite.internal.processors.query.calcite.rel.IgniteRel;
import org.apache.ignite.internal.processors.query.calcite.rel.Receiver;
import org.apache.ignite.internal.processors.query.calcite.rel.Sender;
import org.apache.ignite.internal.processors.query.calcite.util.IgniteRelShuttle;

/**
 *
 */
public class Splitter extends IgniteRelShuttle {
    private List<Fragment> fragments;

    public QueryPlan go(IgniteRel root) {
        fragments = new ArrayList<>();

        fragments.add(new Fragment(root.accept(this)));

        Collections.reverse(fragments);

        return new QueryPlan(fragments);
    }

    @Override public RelNode visit(IgniteExchange rel) {
        RelNode input = rel.getInput();
        RelOptCluster cluster = input.getCluster();
        RelTraitSet inputTraits = input.getTraitSet();
        RelTraitSet outputTraits = rel.getTraitSet();

        Sender sender = new Sender(cluster, inputTraits, visit(input));
        Fragment fragment = new Fragment(sender);
        fragments.add(fragment);

        return new Receiver(cluster, outputTraits, sender.getRowType(), fragment);
    }

    @Override public RelNode visit(Receiver rel) {
        throw new AssertionError("An attempt to split an already split task.");
    }

    @Override public RelNode visit(Sender rel) {
        throw new AssertionError("An attempt to split an already split task.");
    }

    @Override protected RelNode visitOther(RelNode rel) {
        throw new AssertionError("Unexpected node: " + rel);
    }
}
