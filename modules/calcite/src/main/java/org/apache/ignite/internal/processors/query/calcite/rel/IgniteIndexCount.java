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
import org.apache.calcite.plan.RelOptCost;
import org.apache.calcite.plan.RelOptPlanner;
import org.apache.calcite.plan.RelOptTable;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.AbstractRelNode;
import org.apache.calcite.rel.RelInput;
import org.apache.calcite.rel.RelWriter;
import org.apache.calcite.rel.metadata.RelMetadataQuery;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.sql.type.SqlTypeName;

import static java.util.Collections.singletonList;

/**
 * Returns number of index records.
 */
public class IgniteIndexCount extends AbstractRelNode implements SourceAwareIgniteRel {
    /** */
    private static final double INDEX_TRAVERSE_COST_DIVIDER = 1000;

    /** */
    private final RelOptTable tbl;

    /** */
    private final String idxName;

    /** */
    private final long sourceId;

    /** */
    private final boolean notNull;

    /** */
    private final int fieldIdx;

    /**
     * Constructor for deserialization.
     *
     * @param input Serialized representation.
     */
    public IgniteIndexCount(RelInput input) {
        super(input.getCluster(), input.getTraitSet());

        idxName = input.getString("index");
        tbl = input.getTable("table");

        Object srcIdObj = input.get("sourceId");
        if (srcIdObj != null)
            sourceId = ((Number)srcIdObj).longValue();
        else
            sourceId = -1L;

        notNull = input.getBoolean("notNull", false);
        fieldIdx = ((Number)input.get("fieldIdx")).intValue();
    }

    /**
     * Ctor.
     *
     * @param cluster Cluster that this relational expression belongs to.
     * @param traits Traits of this relational expression.
     * @param tbl Table definition.
     * @param idxName Index name.
     * @param notNull Count only not-null values.
     */
    public IgniteIndexCount(
        RelOptCluster cluster,
        RelTraitSet traits,
        RelOptTable tbl,
        String idxName,
        boolean notNull,
        int fieldIdx
    ) {
        this(-1, cluster, traits, tbl, idxName, notNull, fieldIdx);
    }

    /**
     * Ctor.
     *
     * @param sourceId Source id.
     * @param cluster Cluster that this relational expression belongs to.
     * @param traits Traits of this relational expression.
     * @param tbl Table definition.
     * @param idxName Index name.
     * @param notNull Count only not-null values.
     */
    private IgniteIndexCount(
        long sourceId,
        RelOptCluster cluster,
        RelTraitSet traits,
        RelOptTable tbl,
        String idxName,
        boolean notNull,
        int fieldIdx
    ) {
        super(cluster, traits);

        this.idxName = idxName;
        this.tbl = tbl;
        this.sourceId = sourceId;
        this.notNull = notNull;
        this.fieldIdx = fieldIdx;
    }

    /** {@inheritDoc} */
    @Override protected RelDataType deriveRowType() {
        RelDataTypeFactory tf = getCluster().getTypeFactory();

        return tf.createStructType(singletonList(tf.createSqlType(SqlTypeName.BIGINT)), singletonList("COUNT"));
    }

    /** */
    public String indexName() {
        return idxName;
    }

    /** {@inheritDoc} */
    @Override public double estimateRowCount(RelMetadataQuery mq) {
        // Requesting index count always produces just one record.
        return 1.0;
    }

    /** {@inheritDoc} */
    @Override public RelOptCost computeSelfCost(RelOptPlanner planner, RelMetadataQuery mq) {
        return planner.getCostFactory().makeCost(1.0, tbl.getRowCount() / INDEX_TRAVERSE_COST_DIVIDER, 0);
    }

    /** {@inheritDoc} */
    @Override public long sourceId() {
        return sourceId;
    }

    /** {@inheritDoc} */
    @Override public RelOptTable getTable() {
        return tbl;
    }

    /** {@inheritDoc} */
    @Override public RelWriter explainTerms(RelWriter pw) {
        return super.explainTerms(pw)
            .item("index", idxName)
            .item("table", tbl.getQualifiedName())
            .itemIf("sourceId", sourceId, sourceId != -1L)
            .item("notNull", notNull)
            .item("fieldIdx", fieldIdx);
    }

    /** {@inheritDoc} */
    @Override public <T> T accept(IgniteRelVisitor<T> visitor) {
        return visitor.visit(this);
    }

    /** {@inheritDoc} */
    @Override public IgniteRel clone(RelOptCluster cluster, List<IgniteRel> inputs) {
        return new IgniteIndexCount(sourceId, cluster, traitSet, tbl, idxName, notNull, fieldIdx);
    }

    /** {@inheritDoc} */
    @Override public IgniteRel clone(long srcId) {
        return new IgniteIndexCount(srcId, getCluster(), traitSet, tbl, idxName, notNull, fieldIdx);
    }

    /** */
    public boolean notNull() {
        return notNull;
    }

    /** */
    public int fieldIndex() {
        return fieldIdx;
    }
}
