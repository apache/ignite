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
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.SingleRel;
import org.apache.ignite.internal.processors.query.calcite.prepare.RelTarget;
import org.apache.ignite.internal.processors.query.calcite.prepare.RelTargetAware;
import org.apache.ignite.internal.processors.query.calcite.trait.DistributionTraitDef;
import org.apache.ignite.internal.processors.query.calcite.trait.IgniteDistribution;

/**
 * Relational expression that iterates over its input
 * and sends elements to remote {@link IgniteReceiver}
 */
public class IgniteSender extends SingleRel implements IgniteRel, RelTargetAware {
    /** */
    private RelTarget target;

    /**
     * Creates a Sender.
     *
     * @param cluster  Cluster that this relational expression belongs to
     * @param traits   Traits of this relational expression
     * @param input    input relational expression
     */
    public IgniteSender(RelOptCluster cluster, RelTraitSet traits, RelNode input) {
        this(cluster, traits, input, null);
    }

    /**
     * Creates a Sender.
     *
     * @param cluster  Cluster that this relational expression belongs to
     * @param traits   Traits of this relational expression
     * @param input    input relational expression
     * @param target   Remote targets information
     */
    private IgniteSender(RelOptCluster cluster, RelTraitSet traits, RelNode input, RelTarget target) {
        super(cluster, traits, input);

        this.target = target;
    }

    /** {@inheritDoc} */
    @Override public RelNode copy(RelTraitSet traitSet, List<RelNode> inputs) {
        return new IgniteSender(getCluster(), traitSet, sole(inputs), target);
    }

    /** {@inheritDoc} */
    @Override public <T> T accept(IgniteRelVisitor<T> visitor) {
        return visitor.visit(this);
    }

    /** {@inheritDoc} */
    @Override public void target(RelTarget target) {
        this.target = target;
    }

    /**
     * @return Remote targets information.
     */
    public RelTarget target() {
        return target;
    }

    /**
     * @return Node distribution.
     */
    public IgniteDistribution sourceDistribution() {
        return input.getTraitSet().getTrait(DistributionTraitDef.INSTANCE);
    }
}
