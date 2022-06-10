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

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import com.google.common.collect.ImmutableMap;
import org.apache.calcite.plan.RelOptRuleCall;
import org.apache.calcite.plan.RelRule;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.RelCollations;
import org.apache.calcite.rel.core.AggregateCall;
import org.apache.calcite.rel.logical.LogicalAggregate;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.apache.calcite.util.ImmutableIntList;
import org.apache.ignite.internal.processors.query.QueryUtils;
import org.apache.ignite.internal.processors.query.calcite.rel.IgniteConvention;
import org.apache.ignite.internal.processors.query.calcite.rel.IgniteIndexCount;
import org.apache.ignite.internal.processors.query.calcite.rel.IgniteProject;
import org.apache.ignite.internal.processors.query.calcite.rel.logical.IgniteLogicalTableScan;
import org.apache.ignite.internal.processors.query.calcite.schema.IgniteIndex;
import org.apache.ignite.internal.processors.query.calcite.schema.IgniteTable;
import org.apache.ignite.internal.processors.query.calcite.trait.IgniteDistributions;
import org.apache.ignite.internal.processors.query.calcite.trait.RewindabilityTrait;
import org.immutables.value.Value;

/** Tries to optimize 'COUNT(*)' to use number of index records. */
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

        IgniteIndex idx = table.indexes().get(QueryUtils.PRIMARY_KEY_INDEX);

        if (idx == null)
            idx = table.indexes().values().stream().findFirst().orElse(null);

        if (
            idx == null ||
                scan.condition() != null ||
                scan.projects() != null ||
                aggr.getGroupCount() > 0 ||
                table.isIndexRebuildInProgress() ||
                aggr.getAggCallList().stream().anyMatch(a -> a.getAggregation().getKind() != SqlKind.COUNT ||
                    !a.getArgList().isEmpty())
        )
            return;

        RelTraitSet idxTraits = aggr.getTraitSet()
            .replace(IgniteConvention.INSTANCE)
            .replace(IgniteDistributions.random())
            .replace(RewindabilityTrait.REWINDABLE);

        IgniteIndexCount idxCnt = new IgniteIndexCount(
            scan.getCluster(),
            idxTraits,
            scan.getTable(),
            idx.name(),
            aggr.getRowType()
        );

        AggregateCall idxSumAggCall = AggregateCall.create(
            SqlStdOperatorTable.SUM0,
            false,
            false,
            false,
            ImmutableIntList.of(0),
            -1,
            null,
            RelCollations.EMPTY,
            0,
            idxCnt,
            null,
            null);

        List<AggregateCall> indCntSumFunLst = Stream.generate(() -> idxSumAggCall).limit(aggr.getAggCallList().size())
            .collect(Collectors.toList());

        LogicalAggregate newRel = new LogicalAggregate(
            aggr.getCluster(),
            aggr.getTraitSet(),
            Collections.emptyList(),
            idxCnt,
            aggr.getGroupSet(),
            aggr.getGroupSets(),
            indCntSumFunLst
        );

        // SUM0/DECIMAL to COUNT()/Long converter.
        List<RexNode> proj = new ArrayList<>();
        RexBuilder rexBuilder = scan.getCluster().getRexBuilder();

        for (int i = 0; i < aggr.getAggCallList().size(); ++i)
            proj.add(rexBuilder.makeCast(aggr.getAggCallList().get(i).getType(), rexBuilder.makeInputRef(newRel, i)));

        IgniteProject castToLongNode = new IgniteProject(
            newRel.getCluster(),
            aggr.getTraitSet().replace(IgniteConvention.INSTANCE),
            newRel,
            proj, aggr.getRowType());

        call.transformTo(castToLongNode, ImmutableMap.of(castToLongNode, aggr));
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
