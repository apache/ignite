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

import java.util.Set;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.CorrelationId;
import org.apache.calcite.rel.core.Join;
import org.apache.calcite.rel.core.JoinRelType;
import org.apache.calcite.rex.RexNode;

/**
 *
 */
public class IgniteJoin extends Join implements IgniteRel {
    public IgniteJoin(RelOptCluster cluster, RelTraitSet traitSet, RelNode left, RelNode right, RexNode condition, Set<CorrelationId> variablesSet, JoinRelType joinType) {
        super(cluster, traitSet, left, right, condition, variablesSet, joinType);
    }

    @Override public Join copy(RelTraitSet traitSet, RexNode condition, RelNode left, RelNode right, JoinRelType joinType, boolean semiJoinDone) {
        return new IgniteJoin(getCluster(), traitSet, left, right, condition, variablesSet, joinType);
    }

    @Override public <T> T accept(IgniteRelVisitor<T> visitor) {
        return visitor.visit(this);
    }
}
