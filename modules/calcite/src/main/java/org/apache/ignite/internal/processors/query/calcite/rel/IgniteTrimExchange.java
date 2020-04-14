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

import static org.apache.calcite.rel.RelDistribution.Type.BROADCAST_DISTRIBUTED;
import static org.apache.calcite.rel.RelDistribution.Type.HASH_DISTRIBUTED;

/**
 *
 */
public class IgniteTrimExchange extends SingleRel implements IgniteRel, RelTargetAware {
    /** */
    private RelTarget target;

    /** */
    public IgniteTrimExchange(RelOptCluster cluster, RelTraitSet traits, RelNode input) {
        super(cluster, traits, input);

        assert input.getTraitSet().getTrait(DistributionTraitDef.INSTANCE).getType() == BROADCAST_DISTRIBUTED;
        assert traits.getTrait(DistributionTraitDef.INSTANCE).getType() == HASH_DISTRIBUTED;
    }

    /** */
    @Override public RelNode copy(RelTraitSet traits, List<RelNode> inputs) {
        RelNode input = sole(inputs);

        assert input.getTraitSet().getTrait(DistributionTraitDef.INSTANCE).getType() == BROADCAST_DISTRIBUTED;
        assert traits.getTrait(DistributionTraitDef.INSTANCE).getType() == HASH_DISTRIBUTED;

        return new IgniteTrimExchange(getCluster(), traits, input);
    }

    /** {@inheritDoc} */
    @Override public <T> T accept(IgniteRelVisitor<T> visitor) {
        return visitor.visit(this);
    }

    /** */
    public RelTarget target() {
        return target;
    }

    /** {@inheritDoc} */
    @Override public void target(RelTarget target) {
        this.target = target;
    }
}
