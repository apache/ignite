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
import org.apache.calcite.rel.AbstractRelNode;
import org.apache.calcite.rel.RelCollation;
import org.apache.calcite.rel.RelCollations;
import org.apache.calcite.rel.RelInput;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.RelWriter;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.sql.SqlExplainLevel;
import org.apache.calcite.util.Pair;

/**
 * Relational expression that receives elements from remote {@link IgniteSender}
 */
public class IgniteReceiver extends AbstractRelNode implements IgniteRel {
    /** */
    private final long exchangeId;

    /** */
    private final long sourceFragmentId;

    /** */
    private final RelCollation collation;

    /**
     * Creates a Receiver
     */
    public IgniteReceiver(RelOptCluster cluster, RelTraitSet traits, RelDataType rowType, long exchangeId,
        long sourceFragmentId) {
        super(cluster, traits);

        this.exchangeId = exchangeId;
        this.sourceFragmentId = sourceFragmentId;
        this.rowType = rowType;

        collation = traits.getCollation();
    }

    /** */
    public IgniteReceiver(RelInput input) {
        super(input.getCluster(), input.getTraitSet().replace(IgniteConvention.INSTANCE));

        exchangeId = ((Number)input.get("exchangeId")).longValue();
        sourceFragmentId = ((Number)input.get("sourceFragmentId")).longValue();
        rowType = input.getRowType("rowType");
        collation = input.getCollation();
    }

    /** */
    public long exchangeId() {
        return exchangeId;
    }

    /** */
    public long sourceFragmentId() {
        return sourceFragmentId;
    }

    /** */
    @Override public RelCollation collation() {
        return collation;
    }

    /** {@inheritDoc} */
    @Override public RelNode copy(RelTraitSet traitSet, List<RelNode> inputs) {
        return new IgniteReceiver(getCluster(), traitSet, rowType, exchangeId, sourceFragmentId);
    }

    /** {@inheritDoc} */
    @Override public <T> T accept(IgniteRelVisitor<T> visitor) {
        return visitor.visit(this);
    }

    /** {@inheritDoc} */
    @Override public RelWriter explainTerms(RelWriter pw) {
        RelWriter writer = super.explainTerms(pw);

        if (pw.getDetailLevel() != SqlExplainLevel.ALL_ATTRIBUTES)
            return writer;

        return writer
            .item("rowType", rowType)
            .item("exchangeId", exchangeId)
            .item("sourceFragmentId", sourceFragmentId)
            .itemIf("collation", collation, collation != null && collation != RelCollations.EMPTY);
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
