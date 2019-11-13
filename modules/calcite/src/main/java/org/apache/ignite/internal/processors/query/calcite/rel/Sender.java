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

import java.util.List;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.SingleRel;
import org.apache.calcite.rel.metadata.RelMetadataQuery;
import org.apache.ignite.internal.processors.query.calcite.metadata.FragmentLocation;
import org.apache.ignite.internal.processors.query.calcite.metadata.IgniteMdFragmentLocation;
import org.apache.ignite.internal.processors.query.calcite.trait.DestinationFunction;
import org.apache.ignite.internal.processors.query.calcite.trait.DistributionTrait;
import org.apache.ignite.internal.processors.query.calcite.util.Implementor;

/**
 *
 */
public final class Sender extends SingleRel implements IgniteRel {
    private FragmentLocation location;
    private FragmentLocation targetLocation;
    private DistributionTrait targetDistribution;
    private DestinationFunction destinationFunction;

    /**
     * Creates a <code>SingleRel</code>.
     *
     * @param cluster Cluster this relational expression belongs to
     * @param traits Trait set.
     * @param input Input relational expression
     */
    public Sender(RelOptCluster cluster, RelTraitSet traits, RelNode input) {
        super(cluster, traits, input);
    }

    @Override public RelNode copy(RelTraitSet traitSet, List<RelNode> inputs) {
        return new Sender(getCluster(), traitSet, sole(inputs));
    }

    /** {@inheritDoc} */
    @Override public <T> T implement(Implementor<T> implementor) {
        return implementor.implement(this);
    }

    public void init(FragmentLocation targetLocation, DistributionTrait targetDistribution) {
        this.targetLocation = targetLocation;
        this.targetDistribution = targetDistribution;
    }

    public DestinationFunction targetFunction() {
        if (destinationFunction == null) {
            assert targetLocation != null && targetLocation.mapping() != null && targetDistribution != null;

            destinationFunction = targetDistribution.destinationFunctionFactory().create(targetLocation, targetDistribution.keys());
        }

        return destinationFunction;
    }

    public FragmentLocation location(RelMetadataQuery mq) {
        if (location == null)
            location = IgniteMdFragmentLocation.location(getInput(), mq);

        return location;
    }

    public void reset() {
        location = null;
        targetLocation = null;
        targetDistribution = null;
        destinationFunction = null;
    }
}
