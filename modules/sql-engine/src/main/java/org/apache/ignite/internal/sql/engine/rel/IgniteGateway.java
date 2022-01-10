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

package org.apache.ignite.internal.sql.engine.rel;

import java.util.List;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelOptCost;
import org.apache.calcite.plan.RelOptPlanner;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.RelInput;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.RelWriter;
import org.apache.calcite.rel.SingleRel;
import org.apache.calcite.rel.metadata.RelMetadataQuery;

/**
 * A gateway node connecting relational trees with different conventions.
 */
public class IgniteGateway extends SingleRel implements SourceAwareIgniteRel {
    private static final String EXTENSION_NAME_TERM = "name";

    private final String extensionName;

    private final long sourceId;

    /**
     * Constructor.
     *
     * @param extensionName A name of the extension the input node belongs to.
     * @param cluster       Cluster this node belongs to.
     * @param traits        A set of the traits this node satisfy.
     * @param input         An input relation.
     */
    public IgniteGateway(String extensionName, RelOptCluster cluster, RelTraitSet traits, RelNode input) {
        this(extensionName, -1L, cluster, traits, input);
    }

    /**
     * Constructor.
     *
     * @param extensionName A name of the extension the input node belongs to.
     * @param sourceId      An id of the source this gateway belongs to.
     * @param cluster       Cluster this node belongs to.
     * @param traits        A set of the traits this node satisfy.
     * @param input         An input relation.
     */
    private IgniteGateway(String extensionName, long sourceId, RelOptCluster cluster, RelTraitSet traits, RelNode input) {
        super(cluster, traits, input);

        this.sourceId = sourceId;
        this.extensionName = extensionName;
    }

    /**
     * Constructor.
     *
     * @param input Context to recover this relation from.
     */
    public IgniteGateway(RelInput input) {
        super(input.getCluster(), input.getTraitSet().replace(IgniteConvention.INSTANCE), input.getInput());

        extensionName = input.getString(EXTENSION_NAME_TERM);

        Object srcIdObj = input.get("sourceId");
        if (srcIdObj != null) {
            sourceId = ((Number) srcIdObj).longValue();
        } else {
            sourceId = -1;
        }
    }

    /**
     * Returns the name of the extension the input node belongs to.
     *
     * @return The name of the extension.
     */
    public String extensionName() {
        return extensionName;
    }

    /** {@inheritDoc} */
    @Override
    public RelWriter explainTerms(RelWriter pw) {
        return super.explainTerms(pw)
                .item(EXTENSION_NAME_TERM, extensionName);
    }

    /** {@inheritDoc} */
    @Override
    public boolean isEnforcer() {
        return true;
    }

    /** {@inheritDoc} */
    @Override
    public RelOptCost computeSelfCost(RelOptPlanner planner, RelMetadataQuery mq) {
        return planner.getCostFactory().makeZeroCost();
    }

    /** {@inheritDoc} */
    @Override
    public <T> T accept(IgniteRelVisitor<T> visitor) {
        return visitor.visit(this);
    }

    /** {@inheritDoc} */
    @Override
    public IgniteRel clone(RelOptCluster cluster, List<IgniteRel> inputs) {
        return new IgniteGateway(extensionName, cluster, getTraitSet(), sole(inputs));
    }

    /** {@inheritDoc} */
    @Override
    public IgniteRel clone(long sourceId) {
        return new IgniteGateway(extensionName, sourceId, getCluster(), getTraitSet(), getInput());
    }

    /** {@inheritDoc} */
    @Override
    public RelNode copy(RelTraitSet traitSet, List<RelNode> inputs) {
        return new IgniteGateway(extensionName, getCluster(), traitSet, sole(inputs));
    }

    /** {@inheritDoc} */
    @Override
    public long sourceId() {
        return sourceId;
    }
}
