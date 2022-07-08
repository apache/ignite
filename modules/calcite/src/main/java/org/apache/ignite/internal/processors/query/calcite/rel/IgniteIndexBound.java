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
import java.util.stream.Collectors;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelOptCost;
import org.apache.calcite.plan.RelOptPlanner;
import org.apache.calcite.plan.RelOptTable;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.AbstractRelNode;
import org.apache.calcite.rel.RelCollation;
import org.apache.calcite.rel.RelFieldCollation;
import org.apache.calcite.rel.RelInput;
import org.apache.calcite.rel.RelWriter;
import org.apache.calcite.rel.metadata.RelMetadataQuery;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.util.ImmutableBitSet;
import org.apache.ignite.internal.processors.query.calcite.metadata.cost.IgniteCost;
import org.apache.ignite.internal.processors.query.calcite.schema.IgniteTable;
import org.apache.ignite.internal.processors.query.calcite.util.Commons;

/**
 * Extracts first or last not null index value.
 */
public class IgniteIndexBound extends AbstractRelNode implements SourceAwareIgniteRel {
    /** */
    private static final double INDEX_NULLS_EXPECTED_RATIO = 0.05f;

    /** */
    private final String idxName;

    /** */
    private final long sourceId;

    /** */
    private final boolean first;

    /** */
    private final RelOptTable tbl;

    /** */
    private final RelCollation collation;

    /**
     * Ctor.
     *
     * @param tbl Table definition.
     * @param cluster Cluster that this relational expression belongs to.
     * @param traits Traits of this relational expression.
     * @param idxName Index name.
     * @param first Take-first / take-last not null idex record flag.
     */
    public IgniteIndexBound(
        RelOptTable tbl,
        RelOptCluster cluster,
        RelTraitSet traits,
        String idxName,
        boolean first,
        RelCollation collation
    ) {
        this(-1, tbl, cluster, traits, idxName, first, collation);
    }

    /**
     * Ctor.
     *
     * @param sourceId Source id.
     * @param tbl Table definition.
     * @param cluster Cluster that this relational expression belongs to.
     * @param traits Traits of this relational expression.
     * @param idxName Index name.
     * @param first Take-first / take-last not null idex record flag.
     * @param collation Collation.
     */
    private IgniteIndexBound(
        long sourceId,
        RelOptTable tbl,
        RelOptCluster cluster,
        RelTraitSet traits,
        String idxName,
        boolean first,
        RelCollation collation
    ) {
        super(cluster, traits);

        this.sourceId = sourceId;
        this.tbl = tbl;
        this.idxName = idxName;
        this.first = first;
        this.collation = collation;
    }

    /**
     * Constructor for deserialization.
     *
     * @param input Serialized representation.
     */
    public IgniteIndexBound(RelInput input) {
        super(input.getCluster(), input.getTraitSet());

        Object srcIdObj = input.get("sourceId");
        if (srcIdObj != null)
            sourceId = ((Number)srcIdObj).longValue();
        else
            sourceId = -1L;

        tbl = input.getTable("table");

        idxName = input.getString("index");

        first = input.getBoolean("first", true);

        collation = input.getCollation();
    }

    /** {@inheritDoc} */
    @Override public RelOptCost computeSelfCost(RelOptPlanner planner, RelMetadataQuery mq) {
        //Taking first or last value supposes scan until not null.
        return planner.getCostFactory().makeCost(1.0, tbl.getRowCount() * INDEX_NULLS_EXPECTED_RATIO
            * IgniteCost.ROW_PASS_THROUGH_COST, 0);
    }

    /** {@inheritDoc} */
    @Override public RelDataType deriveRowType() {
        return tbl.unwrap(IgniteTable.class).getRowType(Commons.typeFactory(getCluster()), requiredColumns());
    }

    /** {@inheritDoc} */
    @Override public RelWriter explainTerms(RelWriter pw) {
        return super.explainTerms(pw)
            .itemIf("sourceId", sourceId, sourceId != -1L)
            .item("table", tbl.getQualifiedName())
            .item("index", idxName)
            .item("first", first)
            .item("collation", collation);
    }

    /** {@inheritDoc} */
    @Override public <T> T accept(IgniteRelVisitor<T> visitor) {
        return visitor.visit(this);
    }

    /** {@inheritDoc} */
    @Override public IgniteRel clone(RelOptCluster cluster, List<IgniteRel> inputs) {
        return new IgniteIndexBound(sourceId, tbl, cluster, traitSet, idxName, first, collation);
    }

    /** {@inheritDoc} */
    @Override public long sourceId() {
        return sourceId;
    }

    /** {@inheritDoc} */
    @Override public IgniteRel clone(long sourceId) {
        return new IgniteIndexBound(sourceId, tbl, getCluster(), traitSet, idxName, first, collation);
    }

    /** {@inheritDoc} */
    @Override public RelOptTable getTable() {
        return tbl;
    }

    /** */
    public String indexName() {
        return idxName;
    }

    /**
     * @return {@code True}, if first index record is required. {@code False}, if last index record is required.
     */
    public boolean first() {
        return first;
    }

    /** */
    public ImmutableBitSet requiredColumns() {
        return ImmutableBitSet.of(collation.getFieldCollations().stream().map(RelFieldCollation::getFieldIndex)
            .collect(Collectors.toList()));
    }

    /** {@inheritDoc} */
    @Override public RelCollation collation() {
        return collation;
    }
}
