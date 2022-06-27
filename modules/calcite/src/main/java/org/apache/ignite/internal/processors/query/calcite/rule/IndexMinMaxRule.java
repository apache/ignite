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

import java.util.Collections;
import org.apache.calcite.plan.RelOptRuleCall;
import org.apache.calcite.plan.RelRule;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.RelDistribution;
import org.apache.calcite.rel.logical.LogicalAggregate;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.sql.SqlAggFunction;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.tools.RelBuilder;
import org.apache.ignite.internal.processors.query.QueryUtils;
import org.apache.ignite.internal.processors.query.calcite.rel.IgniteConvention;
import org.apache.ignite.internal.processors.query.calcite.rel.IgniteIndexScan;
import org.apache.ignite.internal.processors.query.calcite.rel.logical.IgniteLogicalTableScan;
import org.apache.ignite.internal.processors.query.calcite.schema.CacheTableDescriptor;
import org.apache.ignite.internal.processors.query.calcite.schema.ColumnDescriptor;
import org.apache.ignite.internal.processors.query.calcite.schema.IgniteIndex;
import org.apache.ignite.internal.processors.query.calcite.schema.IgniteTable;
import org.apache.ignite.internal.processors.query.calcite.trait.IgniteDistributions;
import org.apache.ignite.internal.processors.query.calcite.trait.RewindabilityTrait;
import org.apache.ignite.internal.processors.query.calcite.util.IndexConditions;
import org.apache.ignite.internal.processors.query.calcite.util.RexUtils;
import org.immutables.value.Value;

/**
 * Tries to optimize MIN() and MAX() to use first/last record index records.
 */
@Value.Enclosing
public class IndexMinMaxRule extends RelRule<IndexMinMaxRule.Config> {
    /** */
    public static final IndexMinMaxRule INSTANCE = Config.DEFAULT.toRule();

    /** Ctor. */
    private IndexMinMaxRule(IndexMinMaxRule.Config cfg) {
        super(cfg);
    }

    /** */
    @Override public void onMatch(RelOptRuleCall call) {
        LogicalAggregate aggr = call.rel(0);
        IgniteLogicalTableScan scan = call.rel(1);
        IgniteTable table = scan.getTable().unwrap(IgniteTable.class);

        if (
            table.isIndexRebuildInProgress() ||
                scan.condition() != null ||
                scan.projects() != null ||
                aggr.getGroupCount() > 0 ||
                aggr.getAggCallList().stream().anyMatch(a -> a.getAggregation().getKind() != SqlKind.MIN &&
                    a.getAggregation().getKind() != SqlKind.MAX)
        )
            return;

        // TODO: several columns
        int columnNum = scan.requiredColumns().asList().get(0);

        String tableName = scan.getTable().unwrap(CacheTableDescriptor.class).typeDescription().name();

        IgniteIndex idx = null;

        for (ColumnDescriptor fieldDesc : table.descriptor().columnDescriptors()) {
            if (fieldDesc.fieldIndex() == columnNum) {
                idx = table.getIndex(QueryUtils.normalizeObjectName(QueryUtils.indexName(tableName, fieldDesc.name()),
                    false));

                break;
            }
        }

        if (idx == null)
            return;

        RelTraitSet idxTraits = aggr.getTraitSet()
            .replace(IgniteConvention.INSTANCE)
            .replace(table.distribution().getType() == RelDistribution.Type.HASH_DISTRIBUTED ?
                    IgniteDistributions.random() : table.distribution())
            .replace(RewindabilityTrait.REWINDABLE);

        RexBuilder rexb = RexUtils.builder(scan.getCluster());

        RelDataTypeFactory tf = scan.getCluster().getTypeFactory();

        RelDataType tableType = table.getRowType(tf);

        SqlAggFunction aggFun = aggr.getAggCallList().get(0).getAggregation();

        //TODO: sevelar aggregations
        IndexConditions idxConditions = RexUtils.buildSortedIndexConditions(
            scan.getCluster(),
            idx.collation(),
            rexb.makeCall(aggFun, rexb.makeLocalRef(tableType.getFieldList().get(columnNum).getType(), 0)),
            tableType,
            scan.requiredColumns());

        IgniteIndexScan replacement = new IgniteIndexScan(
            scan.getCluster(),
            idxTraits,
            scan.getTable(),
            idx.name(),
            null,
            null,
            idxConditions,
            scan.requiredColumns(),
            idx.collation());

        RelBuilder relb = call.builder();

        call.transformTo(relb.push(replacement)
            .aggregate(relb.groupKey(), Collections.nCopies(aggr.getAggCallList().size(),
                relb.aggregateCall(aggFun, relb.field(0))))
            .build()
        );
    }

    /** The rule config. */
    @Value.Immutable
    public interface    Config extends RelRule.Config {
        /** */
        IndexMinMaxRule.Config DEFAULT = ImmutableIndexMinMaxRule.Config.of()
            .withDescription("IndexMinMaxRule")
            .withOperandSupplier(r -> r.operand(LogicalAggregate.class)
                .oneInput(i -> i.operand(IgniteLogicalTableScan.class).anyInputs()));

        /** {@inheritDoc} */
        @Override default IndexMinMaxRule toRule() {
            return new IndexMinMaxRule(this);
        }
    }
}
