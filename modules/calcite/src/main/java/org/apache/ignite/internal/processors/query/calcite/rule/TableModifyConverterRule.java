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

import com.google.common.collect.ImmutableMap;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelOptRule;
import org.apache.calcite.plan.RelOptRuleCall;
import org.apache.calcite.plan.RelRule;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.RelCollations;
import org.apache.calcite.rel.RelDistribution;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.logical.LogicalTableModify;
import org.apache.calcite.rel.type.RelDataTypeField;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.apache.calcite.tools.RelBuilder;
import org.apache.ignite.internal.processors.query.calcite.rel.IgniteConvention;
import org.apache.ignite.internal.processors.query.calcite.rel.IgniteTableModify;
import org.apache.ignite.internal.processors.query.calcite.schema.IgniteTable;
import org.apache.ignite.internal.processors.query.calcite.trait.IgniteDistribution;
import org.apache.ignite.internal.processors.query.calcite.trait.IgniteDistributions;
import org.apache.ignite.internal.processors.query.calcite.trait.RewindabilityTrait;
import org.apache.ignite.internal.processors.query.calcite.util.Commons;
import org.immutables.value.Value;

/**
 * Converts LogicalTableModify to physical relation operators.
 * There are two options:
 *  - Perform table modify on initiator node. In this case IgniteTableModify with single distribution is inserted.
 *  - Perform table modify on remote nodes. In this case IgniteTableModify with random distribution is inserted and
 *    sum aggregate on top if this table modify (to aggregate and send to initiator node affected rows count)
 */
@Value.Enclosing
public class TableModifyConverterRule extends RelRule<TableModifyConverterRule.Config> {
    /** */
    public static final RelOptRule INSTANCE = Config.DFLT.toRule();

    /** Rule configuration. */
    @Value.Immutable
    public interface Config extends RelRule.Config {
        /** Default config. */
        TableModifyConverterRule.Config DFLT = ImmutableTableModifyConverterRule.Config.of()
            .withOperandSupplier(b ->
                b.operand(LogicalTableModify.class).anyInputs());

        /** {@inheritDoc} */
        @Override default TableModifyConverterRule toRule() {
            return new TableModifyConverterRule(this);
        }
    }

    /**
     * Creates a TableModifyRule.
     */
    public TableModifyConverterRule(Config cfg) {
        super(cfg);
    }

    /** {@inheritDoc} */
    @Override public void onMatch(RelOptRuleCall call) {
        LogicalTableModify rel = call.rel(0);

        RelBuilder relBuilder = relBuilderFactory.create(rel.getCluster(), null);

        RelNode singleNodeTableModify = convertTableModify(rel, IgniteDistributions.single(), IgniteDistributions.single());

        if (Commons.queryTransactionVersion(call.getPlanner().getContext()) != null) {
            // If excplicit transaction is started, table modify can only be executed on initiator node.
            call.transformTo(singleNodeTableModify);

            return;
        }

        IgniteTable table = rel.getTable().unwrap(IgniteTable.class);
        IgniteDistribution inputDistribution = table.distribution();

        switch (rel.getOperation()) {
            case MERGE:
                // Merge contains insert fields as well as _key field, it's impossible to generate input distribution.
                inputDistribution = null;

                break;

            case INSERT:
            case UPDATE:
                // Can only safely proceed, if modified values don't affect remote nodes data sources for the same query.
                if (inputDistribution.getType() != RelDistribution.Type.HASH_DISTRIBUTED)
                    inputDistribution = null;

                break;

            case DELETE:
                inputDistribution = IgniteDistributions.random();

                break;

            default:
                throw new IllegalStateException("Unknown operation type: " + rel.getOperation());
        }

        if (inputDistribution == null) {
            call.transformTo(singleNodeTableModify);

            return;
        }

        RelDataTypeField outFld = rel.getRowType().getFieldList().get(0);

        relBuilder.push(convertTableModify(rel, IgniteDistributions.random(), inputDistribution));
        relBuilder.aggregate(relBuilder.groupKey(),
            relBuilder.aggregateCall(SqlStdOperatorTable.SUM0,
                relBuilder.field(0)).as(outFld.getName()));
        relBuilder.project(relBuilder.cast(relBuilder.fields().get(0), outFld.getType().getSqlTypeName()));

        RelNode distributedTableModify = relBuilder.build();

        call.transformTo(singleNodeTableModify, ImmutableMap.of(distributedTableModify, rel));
    }

    /** */
    private IgniteTableModify convertTableModify(
        LogicalTableModify rel,
        IgniteDistribution outputDistribution,
        IgniteDistribution inputDistribution
    ) {
        RelOptCluster cluster = rel.getCluster();

        RelTraitSet traits = cluster.traitSetOf(IgniteConvention.INSTANCE)
            .replace(outputDistribution)
            .replace(RewindabilityTrait.ONE_WAY)
            .replace(RelCollations.EMPTY);

        RelTraitSet inputTraits = traits.replace(inputDistribution);

        RelNode input = convert(rel.getInput(), inputTraits);

        return new IgniteTableModify(cluster, traits, rel.getTable(), input,
            rel.getOperation(), rel.getUpdateColumnList(), rel.getSourceExpressionList(), rel.isFlattened());
    }
}
