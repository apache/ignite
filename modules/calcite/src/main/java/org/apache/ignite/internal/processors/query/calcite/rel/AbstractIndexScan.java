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
import org.apache.calcite.rel.RelInput;
import org.apache.calcite.rel.RelWriter;
import org.apache.calcite.rel.hint.RelHint;
import org.apache.calcite.rel.metadata.RelMetadataQuery;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.util.ImmutableBitSet;
import org.apache.calcite.util.mapping.Mappings;
import org.apache.ignite.internal.processors.query.calcite.util.Commons;
import org.apache.ignite.internal.processors.query.calcite.util.RexUtils;
import org.apache.ignite.internal.util.typedef.F;
import org.jetbrains.annotations.Nullable;

/**
 * Class with index conditions info.
 */
public abstract class AbstractIndexScan extends ProjectableFilterableTableScan {
    /** */
    protected final String idxName;

    /** */
    protected List<RexNode> lowerCond;

    /** */
    protected List<RexNode> upperCond;

    /** */
    protected List<RexNode> lowerBound;

    /** */
    protected List<RexNode> upperBound;

    /**
     * Constructor used for deserialization.
     *
     * @param input Serialized representation.
     */
    protected AbstractIndexScan(RelInput input) {
        super(input);
        idxName = input.getString("index");
        lowerBound = input.get("lower") == null ? null : input.getExpressionList("lower");
        upperBound = input.get("upper") == null ? null : input.getExpressionList("upper");
    }

    /** */
    protected AbstractIndexScan(
        RelOptCluster cluster,
        RelTraitSet traitSet,
        List<RelHint> hints,
        RelOptTable table,
        String idxName,
        @Nullable List<RexNode> proj,
        @Nullable RexNode cond,
        @Nullable List<RexNode> lowerCond,
        @Nullable List<RexNode> upperCond,
        @Nullable ImmutableBitSet reqColunms
    ) {
        super(cluster, traitSet, hints, table, proj, cond, reqColunms);
        this.idxName = idxName;
        this.lowerCond = lowerCond;
        this.upperCond = upperCond;
    }

    /** {@inheritDoc} */
    @Override protected RelWriter explainTerms0(RelWriter pw) {
        pw = pw.item("index", idxName);
        pw = super.explainTerms0(pw);
        return pw
            .itemIf("lower", lowerBound, !F.isEmpty(lowerBound()))
            .itemIf("upper", upperBound, !F.isEmpty(lowerBound()));
    }

    /**
     *
     */
    public String indexName() {
        return idxName;
    }

    /**
     * @return Lower index condition.
     */
    public List<RexNode> lowerCondition() {
        return lowerCond;
    }

    /**
     * @return Lower index condition.
     */
    public List<RexNode> lowerBound() {
        if (lowerBound == null && lowerCond != null) {
            RelDataType rowType = getTable().getRowType();
            Mappings.TargetMapping mapping = null;
            if (requiredColunms() != null)
                mapping = Commons.inverceMapping(requiredColunms(), rowType.getFieldCount());
            lowerBound = RexUtils.asBound(getCluster(), lowerCond, rowType, mapping);
        }

        return lowerBound;
    }

    /**
     * @return Upper index condition.
     */
    public List<RexNode> upperCondition() {
        return upperCond;
    }

    /**
     * @return Upper index condition.
     */
    public List<RexNode> upperBound() {
        if (upperBound == null && upperCond != null) {
            RelDataType rowType = getTable().getRowType();
            Mappings.TargetMapping mapping = null;
            if (requiredColunms() != null)
                mapping = Commons.inverceMapping(requiredColunms(), rowType.getFieldCount());
            upperBound = RexUtils.asBound(getCluster(), upperCond, rowType, mapping);
        }

        return upperBound;
    }

    /** {@inheritDoc} */
    @Override public RelOptCost computeSelfCost(RelOptPlanner planner, RelMetadataQuery mq) {
        return super.computeSelfCost(planner, mq).plus(planner.getCostFactory().makeTinyCost());
    }
}
