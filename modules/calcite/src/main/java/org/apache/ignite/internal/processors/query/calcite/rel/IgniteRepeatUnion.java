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
import org.apache.calcite.rel.RelInput;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.RepeatUnion;

/** Coordinator-side iterative union for a recursive CTE. */
public class IgniteRepeatUnion extends RepeatUnion implements IgniteRel {
    /** */
    public IgniteRepeatUnion(
        RelOptCluster cluster,
        RelTraitSet traits,
        RelNode seed,
        RelNode iterative,
        boolean all,
        int iterationLimit
    ) {
        super(cluster, traits, seed, iterative, all, iterationLimit, null);
    }

    /** Constructor used for deserialization. */
    public IgniteRepeatUnion(RelInput input) {
        this(
            input.getCluster(),
            input.getTraitSet().replace(IgniteConvention.INSTANCE),
            input.getInputs().get(0),
            input.getInputs().get(1),
            input.getBoolean("all", true),
            iterationLimit(input)
        );
    }

    /** Maximum number of recursive iterations, or a negative value for no limit. */
    public int iterationLimit() {
        return iterationLimit;
    }

    /** {@inheritDoc} */
    @Override public RelNode copy(RelTraitSet traitSet, List<RelNode> inputs) {
        assert inputs.size() == 2;

        return new IgniteRepeatUnion(getCluster(), traitSet, inputs.get(0), inputs.get(1), all, iterationLimit);
    }

    /** {@inheritDoc} */
    @Override public <T> T accept(IgniteRelVisitor<T> visitor) {
        return visitor.visit(this);
    }

    /** {@inheritDoc} */
    @Override public IgniteRel clone(RelOptCluster cluster, List<IgniteRel> inputs) {
        assert inputs.size() == 2;

        return new IgniteRepeatUnion(cluster, getTraitSet(), inputs.get(0), inputs.get(1), all, iterationLimit);
    }

    /** Reads the optional iteration limit from a serialized plan. */
    private static int iterationLimit(RelInput input) {
        Number iterationLimit = (Number)input.get("iterationLimit");

        return iterationLimit == null ? -1 : iterationLimit.intValue();
    }
}
