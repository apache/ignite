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

package org.apache.ignite.internal.processors.query.calcite.exchange;

import java.util.List;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.SingleRel;
import org.apache.ignite.internal.processors.query.calcite.rel.IgniteRel;

/**
 *
 */
public class Receiver extends SingleRel implements IgniteRel {
    /**
     * @param cluster Cluster this relational expression belongs to
     * @param traits Trait set.
     * @param sender Corresponding sender.
     */
    protected Receiver(RelOptCluster cluster, RelTraitSet traits, Sender sender) {
        super(cluster, traits, sender);
    }

    /** {@inheritDoc} */
    @Override public Sender getInput() {
        return (Sender) input;
    }

    /** {@inheritDoc} */
    @Override public RelNode copy(RelTraitSet traitSet, List<RelNode> inputs) {
        return new Receiver(getCluster(), traitSet, (Sender) sole(inputs));
    }
}
