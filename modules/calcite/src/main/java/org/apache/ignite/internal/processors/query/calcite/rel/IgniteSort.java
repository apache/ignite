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
import org.apache.calcite.rel.RelCollation;
import org.apache.calcite.rel.RelInput;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.Sort;
import org.apache.calcite.rex.RexNode;
import org.apache.ignite.internal.processors.query.calcite.util.Commons;

/**
 * Ignite sort operator.
 */
public class IgniteSort extends Sort implements IgniteRel {

    /**
     * Constructor.
     *
     * @param cluster Cluster.
     * @param traits Trait set.
     * @param child Input node.
     * @param collation Collation.
     * @param offset Offset.
     * @param fetch Limit.
     */
    public IgniteSort(
        RelOptCluster cluster,
        RelTraitSet traits,
        RelNode child,
        RelCollation collation,
        RexNode offset,
        RexNode fetch) {
        super(cluster, traits, child, collation, offset, fetch);
    }

    public IgniteSort(RelInput input) {
        super(Commons.changeTraits(input, IgniteConvention.INSTANCE));
    }

    /** {@inheritDoc} */
    @Override  public Sort copy(
        RelTraitSet traitSet,
        RelNode newInput,
        RelCollation newCollation,
        RexNode offset,
        RexNode fetch) {
        return new IgniteSort(getCluster(), traitSet, newInput,newCollation, offset, fetch);
    }

    @Override public <T> T accept(IgniteRelVisitor<T> visitor) {
        return visitor.visit(this);
    }
}
