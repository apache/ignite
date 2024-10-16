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
import java.util.List;
import org.apache.calcite.linq4j.Ord;
import org.apache.calcite.plan.RelOptRuleCall;
import org.apache.calcite.plan.RelRule;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.RelDistribution;
import org.apache.calcite.rel.RelFieldCollation;
import org.apache.calcite.rel.core.AggregateCall;
import org.apache.calcite.rel.logical.LogicalAggregate;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.apache.calcite.tools.RelBuilder;
import org.apache.ignite.internal.processors.query.QueryUtils;
import org.apache.ignite.internal.processors.query.calcite.prepare.BaseQueryContext;
import org.apache.ignite.internal.processors.query.calcite.rel.IgniteConvention;
import org.apache.ignite.internal.processors.query.calcite.rel.IgniteIndexCount;
import org.apache.ignite.internal.processors.query.calcite.rel.logical.IgniteLogicalTableScan;
import org.apache.ignite.internal.processors.query.calcite.schema.IgniteIndex;
import org.apache.ignite.internal.processors.query.calcite.schema.IgniteTable;
import org.apache.ignite.internal.processors.query.calcite.trait.IgniteDistributions;
import org.apache.ignite.internal.processors.query.calcite.trait.RewindabilityTrait;
import org.apache.ignite.internal.processors.query.calcite.util.Commons;
import org.immutables.value.Value;

/** Tries to optimize 'COUNT()' to use number of index records. */
@Value.Enclosing
public class IndexCountRule extends RelRule<IndexCountRule.Config> {
    /** */
    public static final IndexCountRule INSTANCE = Config.DEFAULT.toRule();

    /** Ctor. */
    private IndexCountRule(IndexCountRule.Config cfg) {
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
            aggr.getGroupCount() > 0 ||
            aggr.getAggCallList().size() != 1
        )
            return;

        AggregateCall agg = aggr.getAggCallList().get(0);

        if (agg.getAggregation().getKind() != SqlKind.COUNT || agg.hasFilter() || agg.isDistinct())
            return;

        List<Integer> argList = agg.getArgList();

        IgniteIndex idx = null;
        boolean notNull = false;
        int fieldIdx = 0;

        if (argList.isEmpty())
            idx = table.getIndex(QueryUtils.PRIMARY_KEY_INDEX);
        else {
            if (scan.projects() != null || argList.size() > 1)
                return;

            notNull = true;
            fieldIdx = argList.get(0);

            if (!scan.requiredColumns().isEmpty())
                fieldIdx = scan.requiredColumns().nth(fieldIdx);

            for (IgniteIndex idx0 : table.indexes().values()) {
                List<RelFieldCollation> fieldCollations = idx0.collation().getFieldCollations();

                if (!fieldCollations.isEmpty() && fieldCollations.get(0).getFieldIndex() == fieldIdx) {
                    idx = idx0;
                    break;
                }
            }
        }

        if (idx == null)
            return;

        RelDistribution distribution;
        BaseQueryContext baseQryCtx = call.getPlanner().getContext().unwrap(BaseQueryContext.class);
        if (baseQryCtx.isLocal())
            distribution = IgniteDistributions.single();
        else if (table.distribution().getType() == RelDistribution.Type.HASH_DISTRIBUTED)
            distribution = IgniteDistributions.random();
        else
            distribution = table.distribution();

        RelTraitSet idxTraits = aggr.getTraitSet()
            .replace(IgniteConvention.INSTANCE)
            .replace(distribution)
            .replace(RewindabilityTrait.REWINDABLE);

        IgniteIndexCount idxCnt = new IgniteIndexCount(
            scan.getCluster(),
            idxTraits,
            scan.getTable(),
            idx.name(),
            notNull,
            fieldIdx
        );

        RelBuilder b = call.builder();

        // Also cast DECIMAL of SUM0 to BIGINT(Long) of COUNT().
        call.transformTo(b.push(idxCnt)
            .aggregate(b.groupKey(), Collections.nCopies(aggr.getAggCallList().size(),
                b.aggregateCall(SqlStdOperatorTable.SUM0, b.field(0))))
            .project(Commons.transform(Ord.zip(b.fields()),
                f -> b.cast(f.e, aggr.getRowType().getFieldList().get(f.i).getType().getSqlTypeName())))
            .build()
        );
    }

    /** The rule config. */
    @Value.Immutable
    public interface Config extends RelRule.Config {
        /** */
        IndexCountRule.Config DEFAULT = ImmutableIndexCountRule.Config.of()
            .withDescription("IndexCountRule")
            .withOperandSupplier(r -> r.operand(LogicalAggregate.class)
                .oneInput(i -> i.operand(IgniteLogicalTableScan.class).anyInputs()));

        /** {@inheritDoc} */
        @Override default IndexCountRule toRule() {
            return new IndexCountRule(this);
        }
    }
}
