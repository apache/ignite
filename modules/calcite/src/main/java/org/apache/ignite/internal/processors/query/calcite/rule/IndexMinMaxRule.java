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
import org.apache.calcite.rel.RelCollation;
import org.apache.calcite.rel.core.Aggregate;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.sql.SqlAggFunction;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.tools.RelBuilder;
import org.apache.ignite.internal.processors.query.QueryUtils;
import org.apache.ignite.internal.processors.query.calcite.rel.AbstractIndexScan;
import org.apache.ignite.internal.processors.query.calcite.rel.IgniteAggregate;
import org.apache.ignite.internal.processors.query.calcite.rel.IgniteIndexScan;
import org.apache.ignite.internal.processors.query.calcite.rel.agg.IgniteMapHashAggregate;
import org.apache.ignite.internal.processors.query.calcite.schema.CacheTableDescriptor;
import org.apache.ignite.internal.processors.query.calcite.schema.ColumnDescriptor;
import org.apache.ignite.internal.processors.query.calcite.schema.IgniteTable;
import org.apache.ignite.internal.processors.query.calcite.trait.RewindabilityTrait;
import org.apache.ignite.internal.processors.query.calcite.util.IndexConditions;
import org.apache.ignite.internal.processors.query.calcite.util.RexUtils;
import org.immutables.value.Value;

/**
 * Tries to optimize MIN() and MAX() to usage of firs or last index record.
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
        IgniteAggregate aggr = call.rel(0);
        IgniteIndexScan scan = call.rel(1);
        IgniteTable table = scan.getTable().unwrap(IgniteTable.class);

        //TODO: check
        if (
            table.isIndexRebuildInProgress() ||
                scan.condition() != null ||
                aggr.getGroupCount() > 0 ||
                scan.requiredColumns().asList().size() > 1 ||
                aggr.getAggCallList().stream().anyMatch(a -> a.getAggregation().getKind() != SqlKind.MIN &&
                    a.getAggregation().getKind() != SqlKind.MAX)
        )
            return;

        String tableName = scan.getTable().unwrap(CacheTableDescriptor.class).typeDescription().name();
        int columnNum = scan.requiredColumns().asList().get(0);

        for (ColumnDescriptor fieldDesc : table.descriptor().columnDescriptors()) {
            if (fieldDesc.fieldIndex() != columnNum)
                continue;

            String columnIdxName = QueryUtils.normalizeObjectName(QueryUtils.indexName(tableName, fieldDesc.name()),
                false);

            if (!scan.indexName().equals(columnIdxName))
                return;

            break;
        }

        RexBuilder rexb = RexUtils.builder(scan.getCluster());
        RelBuilder relb = call.builder();
        SqlAggFunction aggFun = aggr.getAggCallList().get(0).getAggregation();
        RelCollation collation = table.getIndex(scan.indexName()).collation();

        IndexConditions idxConditions = RexUtils.buildSortedIndexConditions(
            scan.getCluster(),
            collation,
            rexb.makeCall(aggFun, rexb.makeLocalRef(scan.getRowType(), 0)),
            null,
            scan.requiredColumns());

        IgniteIndexScan replacement = new IgniteIndexScan(
            scan.getCluster(),
            scan.getTraitSet().replace(RewindabilityTrait.REWINDABLE),
            scan.getTable(),
            scan.indexName(),
            null,
            null,
            idxConditions,
            scan.requiredColumns(),
            collation);

        call.transformTo(relb.push(replacement).aggregate(relb.groupKey(),
            relb.aggregateCall(aggFun, relb.field(0))).build());
    }

    /** The rule config. */
    @Value.Immutable
    public interface Config extends RelRule.Config {
        /** */
        IndexMinMaxRule.Config DEFAULT = ImmutableIndexMinMaxRule.Config.of()
            .withDescription("IndexMinMaxRule")
            .withOperandSupplier(r -> r.operand(Aggregate.class)
                .oneInput(i -> i.operand(AbstractIndexScan.class).anyInputs()));

        /** {@inheritDoc} */
        @Override default IndexMinMaxRule toRule() {
            return new IndexMinMaxRule(this);
        }
    }
}
