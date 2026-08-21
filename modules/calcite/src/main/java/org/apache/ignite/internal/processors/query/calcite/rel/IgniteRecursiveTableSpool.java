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
import org.apache.calcite.rel.RelWriter;
import org.apache.calcite.rel.SingleRel;

import static java.util.Objects.requireNonNull;

/** Spool that replaces the current recursive CTE delta after its input is exhausted. */
public class IgniteRecursiveTableSpool extends SingleRel implements IgniteRel {
    /** Query-local recursive state identifier. */
    private final String stateId;

    /** */
    public IgniteRecursiveTableSpool(
        RelOptCluster cluster,
        RelTraitSet traits,
        RelNode input,
        String stateId
    ) {
        super(cluster, traits, input);

        this.stateId = stateId;
    }

    /** Constructor used for deserialization. */
    public IgniteRecursiveTableSpool(RelInput input) {
        this(
            input.getCluster(),
            input.getTraitSet().replace(IgniteConvention.INSTANCE),
            input.getInput(),
            requireNonNull(input.getString("stateId"), "stateId")
        );
    }

    /** Query-local recursive state identifier. */
    public String stateId() {
        return stateId;
    }

    /** {@inheritDoc} */
    @Override public RelNode copy(RelTraitSet traitSet, List<RelNode> inputs) {
        return new IgniteRecursiveTableSpool(getCluster(), traitSet, sole(inputs), stateId);
    }

    /** {@inheritDoc} */
    @Override public <T> T accept(IgniteRelVisitor<T> visitor) {
        return visitor.visit(this);
    }

    /** {@inheritDoc} */
    @Override public IgniteRel clone(RelOptCluster cluster, List<IgniteRel> inputs) {
        return new IgniteRecursiveTableSpool(cluster, getTraitSet(), sole(inputs), stateId);
    }

    /** {@inheritDoc} */
    @Override public RelWriter explainTerms(RelWriter pw) {
        return super.explainTerms(pw).item("stateId", stateId);
    }
}
