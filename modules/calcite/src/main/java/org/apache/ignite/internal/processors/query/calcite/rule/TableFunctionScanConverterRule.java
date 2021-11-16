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

import static org.apache.ignite.internal.util.CollectionUtils.nullOrEmpty;

import java.util.List;
import java.util.Set;
import org.apache.calcite.plan.RelOptPlanner;
import org.apache.calcite.plan.RelOptRule;
import org.apache.calcite.plan.RelOptUtil;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.PhysicalNode;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.CorrelationId;
import org.apache.calcite.rel.logical.LogicalTableFunctionScan;
import org.apache.calcite.rel.metadata.RelMetadataQuery;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.sql2rel.DeduplicateCorrelateVariables;
import org.apache.calcite.util.Util;
import org.apache.ignite.internal.processors.query.calcite.rel.IgniteConvention;
import org.apache.ignite.internal.processors.query.calcite.rel.IgniteTableFunctionScan;
import org.apache.ignite.internal.processors.query.calcite.trait.CorrelationTrait;
import org.apache.ignite.internal.processors.query.calcite.trait.IgniteDistributions;
import org.apache.ignite.internal.processors.query.calcite.trait.RewindabilityTrait;
import org.apache.ignite.internal.processors.query.calcite.util.RexUtils;

/**
 * Rule to convert a {@link LogicalTableFunctionScan} to an {@link IgniteTableFunctionScan}.
 */
public class TableFunctionScanConverterRule extends AbstractIgniteConverterRule<LogicalTableFunctionScan> {
    public static final RelOptRule INSTANCE = new TableFunctionScanConverterRule();

    /** Default constructor. */
    private TableFunctionScanConverterRule() {
        super(LogicalTableFunctionScan.class, "TableFunctionScanConverterRule");
    }

    /** {@inheritDoc} */
    @Override
    protected PhysicalNode convert(RelOptPlanner planner, RelMetadataQuery mq, LogicalTableFunctionScan rel) {
        assert nullOrEmpty(rel.getInputs());

        RelTraitSet traitSet = rel.getTraitSet()
                .replace(IgniteConvention.INSTANCE)
                .replace(RewindabilityTrait.REWINDABLE)
                .replace(IgniteDistributions.broadcast());

        RexBuilder rexBuilder = rel.getCluster().getRexBuilder();

        Set<CorrelationId> corrIds = RexUtils.extractCorrelationIds(rel.getCall());

        // TODO: remove all near 'if' scope after https://issues.apache.org/jira/browse/CALCITE-4673 will be merged.
        if (corrIds.size() > 1) {
            final List<CorrelationId> correlNames = List.copyOf(corrIds);

            RelNode rel0 = DeduplicateCorrelateVariables.go(rexBuilder, correlNames.get(0), Util.skip(correlNames), rel);

            corrIds = RelOptUtil.getVariablesUsed(rel0);

            assert corrIds.size() == 1 : "Multiple correlates are applied: " + corrIds;

            rel = (LogicalTableFunctionScan) rel0;
        }

        if (!corrIds.isEmpty()) {
            traitSet = traitSet.replace(CorrelationTrait.correlations(corrIds));
        }

        return new IgniteTableFunctionScan(rel.getCluster(), traitSet, rel.getCall(), rel.getRowType());
    }
}
