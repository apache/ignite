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

import org.apache.calcite.plan.RelOptRuleCall;
import org.apache.calcite.plan.RelRule;
import org.apache.calcite.rel.RelCollation;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.sql.SqlAggFunction;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.fun.SqlFirstLastValueAggFunction;
import org.apache.ignite.internal.processors.query.GridQueryIndexDescriptor;
import org.apache.ignite.internal.processors.query.QueryUtils;
import org.apache.ignite.internal.processors.query.calcite.rel.AbstractIndexScan;
import org.apache.ignite.internal.processors.query.calcite.rel.IgniteAggregate;
import org.apache.ignite.internal.processors.query.calcite.rel.IgniteIndexScan;
import org.apache.ignite.internal.processors.query.calcite.schema.CacheTableDescriptor;
import org.apache.ignite.internal.processors.query.calcite.schema.IgniteTable;
import org.apache.ignite.internal.processors.query.calcite.trait.RewindabilityTrait;
import org.apache.ignite.internal.processors.query.calcite.util.IndexConditions;
import org.apache.ignite.internal.processors.query.calcite.util.RexUtils;
import org.apache.ignite.internal.util.typedef.F;
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
        IgniteIndexScan idxScanRel = call.rel(1);
        IgniteTable igniteTable = idxScanRel.getTable().unwrap(IgniteTable.class);
        GridQueryIndexDescriptor queryIdxDesc = idxScanRel.getTable().unwrap(CacheTableDescriptor.class).typeDescription()
            .indexes().get(idxScanRel.indexName());

        if (
            queryIdxDesc == null ||
                igniteTable.isIndexRebuildInProgress() ||
                idxScanRel.condition() != null ||
                aggr.getGroupCount() > 0 ||
                aggr.getAggCallList().size() > 1 ||
                (aggr.getAggCallList().get(0).getAggregation().getKind() != SqlKind.MIN &&
                    aggr.getAggCallList().get(0).getAggregation().getKind() != SqlKind.MAX)
        )
            return;

        //TODO: test with several indexed values
        if (!igniteTable.descriptor().columnDescriptors().stream()
            .anyMatch(cd -> queryIdxDesc.fields().contains(QueryUtils.normalizeObjectName(cd.name(), false))))
            return;

        RexBuilder rexb = RexUtils.builder(idxScanRel.getCluster());
        SqlAggFunction aggFun = aggr.getAggCallList().get(0).getAggregation();
        RelCollation collation = igniteTable.getIndex(idxScanRel.indexName()).collation();

        boolean firstIdxValue = (aggFun.getKind() == SqlKind.MIN) !=
            collation.getFieldCollations().get(0).getDirection().isDescending();

        IndexConditions idxConditions = RexUtils.buildSortedIndexConditions(
            idxScanRel.getCluster(),
            collation,
            rexb.makeCall(new SqlFirstLastValueAggFunction(firstIdxValue ? SqlKind.FIRST_VALUE : SqlKind.LAST_VALUE),
                rexb.makeLocalRef(idxScanRel.getRowType(), 0)),
            idxScanRel.getRowType(),
            idxScanRel.requiredColumns());

        IgniteIndexScan newAggrInput = new IgniteIndexScan(
            idxScanRel.getCluster(),
            idxScanRel.getTraitSet().replace(RewindabilityTrait.REWINDABLE),
            idxScanRel.getTable(),
            idxScanRel.indexName(),
            null,
            null,
            idxConditions,
            idxScanRel.requiredColumns(),
            collation);

        call.transformTo(aggr.clone(aggr.getCluster(), F.asList(newAggrInput)));
    }

    /** The rule config. */
    @Value.Immutable
    public interface Config extends RelRule.Config {
        /** */
        IndexMinMaxRule.Config DEFAULT = ImmutableIndexMinMaxRule.Config.of()
            .withDescription("IndexMinMaxRule")
            .withOperandSupplier(r -> r.operand(IgniteAggregate.class)
                .oneInput(i -> i.operand(AbstractIndexScan.class).anyInputs()));

        /** {@inheritDoc} */
        @Override default IndexMinMaxRule toRule() {
            return new IndexMinMaxRule(this);
        }
    }
}
