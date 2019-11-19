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

package org.apache.ignite.internal.processors.query.calcite.rel;

import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.SingleRel;
import org.apache.ignite.internal.processors.query.calcite.metadata.NodesMapping;
import org.apache.ignite.internal.processors.query.calcite.trait.DestinationFunction;
import org.apache.ignite.internal.processors.query.calcite.trait.DistributionTrait;
import org.apache.ignite.internal.processors.query.calcite.util.RelImplementor;
import org.jetbrains.annotations.NotNull;

/**
 *
 */
public final class Sender extends SingleRel implements IgniteRel {
    private final DistributionTrait targetDistr;

    private NodesMapping targetMapping;

    /**
     * Creates a <code>SingleRel</code>.
     *  @param cluster Cluster this relational expression belongs to
     * @param traits Trait set.
     * @param input Input relational expression
     * @param targetDistr Target distribution
     */
    public Sender(RelOptCluster cluster, RelTraitSet traits, RelNode input, @NotNull DistributionTrait targetDistr) {
        super(cluster, traits, input);

        this.targetDistr = targetDistr;
    }

    /** {@inheritDoc} */
    @Override public <T> T implement(RelImplementor<T> implementor) {
        return implementor.implement(this);
    }

    public void init(NodesMapping mapping) {
        targetMapping = mapping;
    }

    public DistributionTrait targetDistribution() {
        return targetDistr;
    }

    public NodesMapping targetMapping() {
        return targetMapping;
    }

    public DestinationFunction targetFunction(org.apache.calcite.plan.Context ctx) {
        return targetDistr.destinationFunctionFactory().create(ctx, targetMapping, targetDistr.keys());
    }
}
