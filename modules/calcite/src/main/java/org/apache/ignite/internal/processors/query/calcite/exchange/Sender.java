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
import org.apache.calcite.rel.metadata.RelMetadataQuery;
import org.apache.ignite.internal.processors.query.calcite.metadata.RelMetadataQueryEx;
import org.apache.ignite.internal.processors.query.calcite.rel.IgniteRel;
import org.apache.ignite.internal.processors.query.calcite.rel.IgniteVisitor;
import org.apache.ignite.internal.processors.query.calcite.splitter.SourceDistribution;
import org.apache.ignite.internal.processors.query.calcite.trait.DistributionFunction;
import org.apache.ignite.internal.processors.query.calcite.trait.DistributionTrait;
import org.apache.ignite.internal.processors.query.calcite.trait.DistributionTraitDef;

/**
 *
 */
public class Sender extends SingleRel implements IgniteRel {
    private SourceDistribution sourceDistribution;
    private SourceDistribution targetDistribution;
    private DistributionFunction targetFunction;

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

    @Override public <T> T accept(IgniteVisitor<T> visitor) {
        return visitor.visitSender(this);
    }

    public void init(SourceDistribution targetDistribution) {
        this.targetDistribution = targetDistribution;
    }

    public DistributionFunction targetFunction() {
        if (targetFunction == null) {
            assert targetDistribution != null && targetDistribution.partitionMapping != null;

            DistributionTrait distribution = getTraitSet().getTrait(DistributionTraitDef.INSTANCE);

            targetFunction = distribution.functionFactory().create(targetDistribution, distribution.keys());
        }

        return targetFunction;
    }

    public SourceDistribution sourceDistribution(RelMetadataQuery mq) {
        if (sourceDistribution == null)
            sourceDistribution = RelMetadataQueryEx.wrap(mq).getSourceDistribution(getInput());

        return sourceDistribution;
    }
}
