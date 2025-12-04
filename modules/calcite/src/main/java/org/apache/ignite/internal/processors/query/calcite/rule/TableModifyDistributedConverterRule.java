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

package org.apache.ignite.internal.processors.query.calcite.rule;

import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelOptPlanner;
import org.apache.calcite.plan.RelOptRule;
import org.apache.calcite.plan.RelOptUtil;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.PhysicalNode;
import org.apache.calcite.rel.RelCollations;
import org.apache.calcite.rel.RelDistribution;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.logical.LogicalTableModify;
import org.apache.calcite.rel.metadata.RelMetadataQuery;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeField;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.apache.calcite.tools.RelBuilder;
import org.apache.calcite.util.ImmutableIntList;
import org.apache.ignite.internal.processors.query.calcite.rel.IgniteConvention;
import org.apache.ignite.internal.processors.query.calcite.rel.IgniteTableModify;
import org.apache.ignite.internal.processors.query.calcite.schema.IgniteTable;
import org.apache.ignite.internal.processors.query.calcite.trait.IgniteDistribution;
import org.apache.ignite.internal.processors.query.calcite.trait.IgniteDistributions;
import org.apache.ignite.internal.processors.query.calcite.trait.RewindabilityTrait;
import org.apache.ignite.internal.processors.query.calcite.util.Commons;
import org.jetbrains.annotations.Nullable;

/**
 * Converts LogicalTableModify to distributed IgniteTableModify (Perform table modify on remote nodes,
 * aggregate affected rows count and send result to the initiator node).
 */
public class TableModifyDistributedConverterRule extends AbstractIgniteConverterRule<LogicalTableModify> {
    /** */
    public static final RelOptRule INSTANCE = new TableModifyDistributedConverterRule();

    /**
     * Creates a ConverterRule.
     */
    public TableModifyDistributedConverterRule() {
        super(LogicalTableModify.class, TableModifyDistributedConverterRule.class.getSimpleName());
    }

    /** {@inheritDoc} */
    @Override protected @Nullable PhysicalNode convert(
        RelOptPlanner planner,
        RelMetadataQuery mq,
        LogicalTableModify rel
    ) {
        RelOptCluster cluster = rel.getCluster();

        // If transaction is explicitly started it's only allowed to perform table modify on initiator node.
        if (Commons.queryTransactionVersion(planner.getContext()) != null)
            return null;

        RelDataType rowType = rel.getRowType();
        IgniteTable table = rel.getTable().unwrap(IgniteTable.class);
        IgniteDistribution inputDistribution = table.distribution();
        boolean affectsSrc = false;

        // Single distribution table modify is prefered in this case.
        if (inputDistribution == IgniteDistributions.single())
            return null;

        switch (rel.getOperation()) {
            case MERGE:
                // Merge contains insert fields as well as update fields, it's impossible to check input distribution
                // over these two fields sets in common case, only corner cases can be implemented, skip it for now.
                return null;

            case INSERT:
                affectsSrc = RelOptUtil.findTables(rel).contains(rel.getTable());

                if (inputDistribution.getType() != RelDistribution.Type.HASH_DISTRIBUTED) {
                    if (affectsSrc)
                        return null;
                    else
                        inputDistribution = IgniteDistributions.hash(ImmutableIntList.range(0, rowType.getFieldCount()));
                }

                break;

            case UPDATE:
                if (inputDistribution.getType() != RelDistribution.Type.HASH_DISTRIBUTED)
                    inputDistribution = IgniteDistributions.hash(ImmutableIntList.of(0));

                break;

            case DELETE:
                inputDistribution = IgniteDistributions.hash(ImmutableIntList.of(0));

                break;

            default:
                throw new IllegalStateException("Unknown operation type: " + rel.getOperation());
        }

        // Create distributed table modify.
        RelBuilder relBuilder = relBuilderFactory.create(rel.getCluster(), null);

        RelTraitSet outputTraits = cluster.traitSetOf(IgniteConvention.INSTANCE)
            .replace(IgniteDistributions.random())
            .replace(RewindabilityTrait.ONE_WAY)
            .replace(RelCollations.EMPTY);

        RelTraitSet inputTraits = outputTraits.replace(inputDistribution);

        RelNode input = convert(rel.getInput(), inputTraits);

        RelNode tableModify = new IgniteTableModify(cluster, outputTraits, rel.getTable(), input, rel.getOperation(),
            rel.getUpdateColumnList(), rel.getSourceExpressionList(), rel.isFlattened(), affectsSrc);

        // Create aggregate to pass affected rows count to initiator node.
        RelDataTypeField outFld = rowType.getFieldList().get(0);

        relBuilder.push(tableModify);
        relBuilder.aggregate(relBuilder.groupKey(),
            relBuilder.aggregateCall(SqlStdOperatorTable.SUM0, relBuilder.field(0)).as(outFld.getName()));

        PhysicalNode agg = (PhysicalNode)HashAggregateConverterRule.MAP_REDUCE.convert(relBuilder.build());

        if (agg == null)
            return null;

        // Create cast to original data type, since SUM aggregate extends type (i.e. sum(INT) -> BIGINT).
        relBuilder.push(agg);
        relBuilder.project(relBuilder.cast(relBuilder.fields().get(0), outFld.getType().getSqlTypeName()));

        return (PhysicalNode)ProjectConverterRule.INSTANCE.convert(relBuilder.build());
    }
}
