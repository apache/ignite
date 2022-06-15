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
import org.apache.calcite.sql.SqlKind;

import static org.apache.ignite.internal.processors.query.calcite.metadata.cost.IgniteCost.ROW_PASS_THROUGH_COST;

/**
 * Returns number of index records.
 */
public class IgniteIndexProbe extends AbstractRelNode implements SourceAwareIgniteRel {
    /** */
    private final RelOptTable tbl;

    /** */
    private final SqlKind operator;

    /** */
    private final String idxName;

    /**
     * Constructor for deserialization.
     *
     * @param input Serialized representation.
     */
    public IgniteIndexProbe(RelInput input) {
        super(input.getCluster(), input.getTraitSet());

        idxName = input.getString("index");
        tbl = input.getTable("table");
        rowType = input.getRowType("type");
        operator = SqlKind.valueOf(input.getString("operator"));
    }

    /**
     * Ctor.
     *
     * @param cluster Cluster that this relational expression belongs to
     * @param traits Traits of this relational expression
     * @param tbl Table definition.
     * @param idxName Index name.
     * @param operator Operator.
     * @param type Data type.
     */
    public IgniteIndexProbe(
        RelOptCluster cluster,
        RelTraitSet traits,
        RelOptTable tbl,
        String idxName,
        SqlKind operator,
        RelDataType type
    ) {
        super(cluster, traits);

        this.idxName = idxName;
        this.tbl = tbl;
        this.operator = operator;
        this.rowType = type;
    }

    /** */
    public String indexName() {
        return idxName;
    }

    /** {@inheritDoc} */
    @Override public double estimateRowCount(RelMetadataQuery mq) {
        // Requesting index count always produces just one record.
        return 1.0d;
    }

    /** {@inheritDoc} */
    @Override public RelOptCost computeSelfCost(RelOptPlanner planner, RelMetadataQuery mq) {
        return planner.getCostFactory().makeCost(1.0, 1.0, tbl.getRowCount() * ROW_PASS_THROUGH_COST);
    }

    /** {@inheritDoc} */
    @Override public long sourceId() {
        return -1L;
    }

    /** {@inheritDoc} */
    @Override public RelOptTable getTable() {
        return tbl;
    }

    /** {@inheritDoc} */
    @Override public RelWriter explainTerms(RelWriter pw) {
        return super.explainTerms(pw)
            .item("index", idxName)
            .item("type", rowType)
            .item("operator", operator.toString())
            .item("table", tbl.getQualifiedName());
    }

    /** {@inheritDoc} */
    @Override public <T> T accept(IgniteRelVisitor<T> visitor) {
        return visitor.visit(this);
    }

    /** {@inheritDoc} */
    @Override public IgniteRel clone(RelOptCluster cluster, List<IgniteRel> inputs) {
        return new IgniteIndexProbe(cluster, traitSet, tbl, idxName, operator, rowType);
    }

    /** {@inheritDoc} */
    @Override public IgniteRel clone(long sourceId) {
        return new IgniteIndexProbe(getCluster(), traitSet, tbl, idxName, operator, rowType);
    }
}
