/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to you under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.ignite.internal.processors.query.calcite.rule;

import java.util.Set;
import org.apache.calcite.plan.RelOptPlanner;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.PhysicalNode;
import org.apache.calcite.rel.core.CorrelationId;
import org.apache.calcite.rel.metadata.RelMetadataQuery;
import org.apache.ignite.internal.processors.query.calcite.rel.IgniteConvention;
import org.apache.ignite.internal.processors.query.calcite.rel.IgniteIndexScan;
import org.apache.ignite.internal.processors.query.calcite.rel.IgniteTableScan;
import org.apache.ignite.internal.processors.query.calcite.rel.ProjectableFilterableTableScan;
import org.apache.ignite.internal.processors.query.calcite.rel.logical.IgniteLogicalIndexScan;
import org.apache.ignite.internal.processors.query.calcite.rel.logical.IgniteLogicalTableScan;
import org.apache.ignite.internal.processors.query.calcite.trait.CorrelationTrait;
import org.apache.ignite.internal.processors.query.calcite.util.RexUtils;

/** */
public abstract class LogicalScanConverterRule<T extends ProjectableFilterableTableScan> extends AbstractIgniteConverterRule<T> {
    /** Instance. */
    public static final LogicalScanConverterRule<IgniteLogicalIndexScan> INDEX_SCAN =
        new LogicalScanConverterRule<IgniteLogicalIndexScan>(IgniteLogicalIndexScan.class, "LogicalIndexScanConverterRule") {
            /** {@inheritDoc} */
            @Override protected PhysicalNode convert(
                RelOptPlanner planner,
                RelMetadataQuery mq,
                IgniteLogicalIndexScan rel
            ) {
                return new IgniteIndexScan(
                    rel.getCluster(),
                    rel.getTraitSet().replace(IgniteConvention.INSTANCE),
                    rel.getTable(),
                    rel.indexName(),
                    rel.projects(),
                    rel.condition(),
                    rel.indexConditions(),
                    rel.requiredColumns()
                );
            }
        };

    /** Instance. */
    public static final LogicalScanConverterRule<IgniteLogicalTableScan> TABLE_SCAN =
        new LogicalScanConverterRule<IgniteLogicalTableScan>(IgniteLogicalTableScan.class, "LogicalTableScanConverterRule") {
            /** {@inheritDoc} */
            @Override protected PhysicalNode convert(
                RelOptPlanner planner,
                RelMetadataQuery mq,
                IgniteLogicalTableScan rel
            ) {
                RelTraitSet traits = rel.getTraitSet().replace(IgniteConvention.INSTANCE);

                Set<CorrelationId> corrIds = RexUtils.extractCorrelationIds(rel.condition());
                if (!corrIds.isEmpty())
                    traits = traits.replace(CorrelationTrait.correlations(corrIds));

                return new IgniteTableScan(rel.getCluster(), traits,
                    rel.getTable(), rel.projects(), rel.condition(), rel.requiredColumns());
            }
        };

    /** */
    private LogicalScanConverterRule(Class<T> clazz, String descPrefix) {
        super(clazz, descPrefix);
    }
}
