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

import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.RelDistributions;
import org.apache.calcite.rel.RelInput;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.RelWriter;
import org.apache.calcite.rel.SingleRel;
import org.apache.calcite.sql.SqlExplainLevel;
import org.apache.calcite.util.Pair;
import org.apache.ignite.internal.processors.query.calcite.trait.DistributionTraitDef;
import org.apache.ignite.internal.processors.query.calcite.trait.IgniteDistribution;

/**
 * Relational expression that iterates over its input
 * and sends elements to remote {@link IgniteReceiver}
 */
public class IgniteSender extends SingleRel implements IgniteRel {
    /** */
    private final long exchangeId;

    /** */
    private long targetFragmentId;

    /** */
    private final IgniteDistribution distribution;

    /**
     * Creates a Sender.
     * @param cluster  Cluster that this relational expression belongs to
     * @param traits   Traits of this relational expression
     * @param input    input relational expression
     * @param exchangeId Exchange ID.
     * @param targetFragmentId Target fragment ID.
     */
    public IgniteSender(RelOptCluster cluster, RelTraitSet traits, RelNode input, long exchangeId,
        long targetFragmentId, IgniteDistribution distribution) {
        super(cluster, traits, input);

        assert traitSet.containsIfApplicable(distribution)
            : "traits=" + traitSet + ", distribution" + distribution;
        assert distribution != RelDistributions.ANY;

        this.exchangeId = exchangeId;
        this.targetFragmentId = targetFragmentId;
        this.distribution = distribution;
    }

    public IgniteSender(RelInput input) {
        this(
            input.getCluster(),
            input.getTraitSet()
                .replace(input.getDistribution())
                .replace(IgniteConvention.INSTANCE),
            input.getInput(),
            ((Number)input.get("exchangeId")).longValue(),
            ((Number)input.get("targetFragmentId")).longValue(),
            (IgniteDistribution)input.getDistribution());
    }

    /** */
    public long exchangeId() {
        return exchangeId;
    }

    /** */
    public long targetFragmentId() {
        return targetFragmentId;
    }

    /** */
    public void targetFragmentId(long targetFragmentId) {
        this.targetFragmentId = targetFragmentId;
    }

    /** {@inheritDoc} */
    @Override public RelNode copy(RelTraitSet traitSet, List<RelNode> inputs) {
        return new IgniteSender(getCluster(), traitSet, sole(inputs), exchangeId, targetFragmentId, distribution);
    }

    /** {@inheritDoc} */
    @Override public <T> T accept(IgniteRelVisitor<T> visitor) {
        return visitor.visit(this);
    }

    /** {@inheritDoc} */
    @Override public IgniteDistribution distribution() {
        return distribution;
    }

    /**
     * @return Node distribution.
     */
    public IgniteDistribution sourceDistribution() {
        return input.getTraitSet().getTrait(DistributionTraitDef.INSTANCE);
    }

    /** {@inheritDoc} */
    @Override public RelWriter explainTerms(RelWriter pw) {
        RelWriter writer = super.explainTerms(pw);

        if (pw.getDetailLevel() != SqlExplainLevel.ALL_ATTRIBUTES)
            return writer;

        return writer
            .item("exchangeId", exchangeId)
            .item("targetFragmentId", targetFragmentId)
            .item("distribution", distribution());
    }

    /** {@inheritDoc} */
    @Override public Pair<RelTraitSet, List<RelTraitSet>> passThroughTraits(
        RelTraitSet required) {
        throw new RuntimeException(getClass().getName()
            + "#passThroughTraits() is not implemented.");
    }

    /** {@inheritDoc} */
    @Override public Pair<RelTraitSet, List<RelTraitSet>> deriveTraits(
        RelTraitSet childTraits, int childId) {
        throw new RuntimeException(getClass().getName()
            + "#deriveTraits() is not implemented.");
    }
}
