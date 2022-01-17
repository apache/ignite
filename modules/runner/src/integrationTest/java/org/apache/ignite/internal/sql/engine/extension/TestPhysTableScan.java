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

package org.apache.ignite.internal.sql.engine.extension;

import static org.apache.calcite.sql.SqlExplainLevel.EXPPLAN_ATTRIBUTES;

import java.util.List;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelOptCost;
import org.apache.calcite.plan.RelOptPlanner;
import org.apache.calcite.plan.RelOptTable;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.RelInput;
import org.apache.calcite.rel.RelWriter;
import org.apache.calcite.rel.core.TableScan;
import org.apache.calcite.rel.hint.RelHint;
import org.apache.calcite.rel.metadata.RelMetadataQuery;
import org.apache.ignite.internal.sql.engine.externalize.RelInputEx;
import org.apache.ignite.internal.sql.engine.rel.IgniteRel;
import org.apache.ignite.internal.sql.engine.rel.IgniteRelVisitor;
import org.apache.ignite.internal.sql.engine.schema.IgniteTable;
import org.checkerframework.checker.nullness.qual.Nullable;

/**
 * A scan over test table.
 */
public class TestPhysTableScan extends TableScan implements IgniteRel {
    /**
     * Constructor.
     *
     * @param cluster  Cluster this node belongs to.
     * @param traitSet A set of the traits this node satisfy.
     * @param hints    A list of hints applicable to the current node.
     * @param table    The actual table to be scanned.
     */
    protected TestPhysTableScan(RelOptCluster cluster, RelTraitSet traitSet,
            List<RelHint> hints, RelOptTable table) {
        super(cluster, traitSet, hints, table);
    }

    /**
     * Constructor.
     *
     * @param input Context to recover this relation from.
     */
    public TestPhysTableScan(RelInput input) {
        super(
                input.getCluster(),
                input.getTraitSet(),
                List.of(),
                ((RelInputEx) input).getTableById("tableId")
        );
    }

    /** {@inheritDoc} */
    @Override
    public <T> T accept(IgniteRelVisitor<T> visitor) {
        return (T) this;
    }

    /** {@inheritDoc} */
    @Override
    public IgniteRel clone(RelOptCluster cluster, List<IgniteRel> inputs) {
        return new TestPhysTableScan(cluster, getTraitSet(), getHints(), getTable());
    }

    /** {@inheritDoc} */
    @Override
    public RelWriter explainTerms(RelWriter pw) {
        return pw
                .itemIf("table", table.getQualifiedName(), pw.getDetailLevel() == EXPPLAN_ATTRIBUTES)
                .itemIf("tableId", table.unwrap(IgniteTable.class).id().toString(),
                        pw.getDetailLevel() != EXPPLAN_ATTRIBUTES);
    }

    @Override
    public @Nullable RelOptCost computeSelfCost(RelOptPlanner planner, RelMetadataQuery mq) {
        return super.computeSelfCost(planner, mq);
    }
}
