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
import org.apache.calcite.sql.SqlAggFunction;
import org.apache.calcite.sql.SqlKind;
import org.apache.ignite.internal.processors.query.calcite.rel.IgniteAggregate;
import org.apache.ignite.internal.processors.query.calcite.rel.IgniteIndexBound;
import org.apache.ignite.internal.processors.query.calcite.rel.IgniteIndexScan;
import org.apache.ignite.internal.processors.query.calcite.schema.IgniteIndex;
import org.apache.ignite.internal.processors.query.calcite.schema.IgniteTable;
import org.apache.ignite.internal.processors.query.calcite.trait.RewindabilityTrait;
import org.apache.ignite.internal.util.typedef.F;
import org.immutables.value.Value;

/**
 * Tries to optimize MIN() and MAX() so that taking only first or last index record is engaged.
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
        IgniteIndexScan idxScan = call.rel(1);
        IgniteTable table = idxScan.getTable().unwrap(IgniteTable.class);
        IgniteIndex idx = table.getIndex(idxScan.indexName());

        if (
            table.isIndexRebuildInProgress() ||
                idxScan.condition() != null ||
                idxScan.projects()  != null ||
                aggr.getGroupCount() > 0 ||
                aggr.getAggCallList().stream().filter(a -> a.getAggregation().getKind() == SqlKind.MIN
                    || a.getAggregation().getKind() == SqlKind.MAX).count() != 1 ||
                idx.collation().getFieldCollations().isEmpty() ||
                idx.collation().getFieldCollations().get(0).getFieldIndex() != idxScan.requiredColumns().asList().get(0)
        )
            return;

        SqlAggFunction aggFun = aggr.getAggCallList().get(0).getAggregation();
        boolean firstIdxVal = (aggFun.getKind() == SqlKind.MIN) !=
            idx.collation().getFieldCollations().get(0).getDirection().isDescending();

        IgniteIndexBound newAggrInput = new IgniteIndexBound(
            idxScan.getTable(),
            idxScan.getCluster(),
            idxScan.getTraitSet().replace(RewindabilityTrait.REWINDABLE),
            idxScan.indexName(),
            firstIdxVal,
            idx.collation()
        );

        call.transformTo(aggr.clone(aggr.getCluster(), F.asList(newAggrInput)));
    }

    /** The rule config. */
    @Value.Immutable
    public interface Config extends RelRule.Config {
        /** */
        IndexMinMaxRule.Config DEFAULT = ImmutableIndexMinMaxRule.Config.of()
            .withDescription("IndexMinMaxRule")
            .withOperandSupplier(r -> r.operand(IgniteAggregate.class)
                .oneInput(i -> i.operand(IgniteIndexScan.class).anyInputs()));

        /** {@inheritDoc} */
        @Override default IndexMinMaxRule toRule() {
            return new IndexMinMaxRule(this);
        }
    }
}
