/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.ignite.internal.processors.query.calcite.rel;

import java.util.List;
import org.apache.calcite.adapter.enumerable.EnumerableConvention;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.RelCollationTraitDef;
import org.apache.calcite.rel.RelDistributionTraitDef;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.RelWriter;
import org.apache.calcite.rel.SingleRel;
import org.apache.calcite.rel.metadata.RelMdCollation;
import org.apache.calcite.rel.metadata.RelMdDistribution;
import org.apache.calcite.rel.metadata.RelMetadataQuery;
import org.apache.calcite.rex.RexNode;

/**
 * Relational expression that applies a limit and/or offset to its input.
 */
public class IgniteLimit extends SingleRel implements IgniteRel {
    /** Offset. */
    public final RexNode offset;

    /** Fetches rows expression (limit) */
    public final RexNode fetch;

    /**
     * Constructor.
     *
     * @param cluster Cluster.
     * @param traits Trait set.
     * @param offset Offset.
     * @param fetch Limit.
     */
    public IgniteLimit(
        RelOptCluster cluster,
        RelTraitSet traits,
        RelNode input,
        RexNode offset,
        RexNode fetch) {
        super(cluster, traits, input);

        this.offset = offset;
        this.fetch = fetch;
    }

    /** Creates an EnumerableLimit. */
    public static IgniteLimit create(final RelNode input, RexNode offset,
        RexNode fetch) {
        final RelOptCluster cluster = input.getCluster();
        final RelMetadataQuery mq = cluster.getMetadataQuery();
        final RelTraitSet traitSet =
            cluster.traitSetOf(IgniteConvention.INSTANCE)
                .replaceIfs(
                    RelCollationTraitDef.INSTANCE,
                    () -> RelMdCollation.limit(mq, input))
                .replaceIf(RelDistributionTraitDef.INSTANCE,
                    () -> RelMdDistribution.limit(mq, input));
        return new IgniteLimit(cluster, traitSet, input, offset, fetch);
    }

    /** {@inheritDoc} */
    @Override public IgniteLimit copy(
        RelTraitSet traitSet,
        List<RelNode> newInputs) {
        return new IgniteLimit(
            getCluster(),
            traitSet,
            sole(newInputs),
            offset,
            fetch);
    }

    /** {@inheritDoc} */
    @Override public RelWriter explainTerms(RelWriter pw) {
        return super.explainTerms(pw)
            .itemIf("offset", offset, offset != null)
            .itemIf("fetch", fetch, fetch != null);
    }

    /** {@inheritDoc} */
    @Override public <T> T accept(IgniteRelVisitor<T> visitor) {
        return visitor.visit(this);
    }
}
