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
import java.util.stream.Collectors;
import java.util.stream.Stream;
import com.google.common.collect.ImmutableMap;
import org.apache.calcite.plan.RelOptRuleCall;
import org.apache.calcite.plan.RelRule;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.RelCollations;
import org.apache.calcite.rel.core.AggregateCall;
import org.apache.calcite.rel.logical.LogicalAggregate;
import org.apache.calcite.rex.RexLiteral;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.apache.calcite.util.ImmutableIntList;
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
import org.immutables.value.Value;

import static org.apache.calcite.sql.fun.SqlStdOperatorTable.FIRST_VALUE;

/** Tries to optimize MIN() and MAX() to use first/last record index records. */
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
            .replace(IgniteDistributions.random())
            .replace(RewindabilityTrait.REWINDABLE);
//
//        // TODO: read multiple aggregates.
//        IgniteIndexProbe idxCnt = new IgniteIndexProbe(
//            scan.getCluster(),
//            idxTraits,
//            scan.getTable(),
//            idx.name(),
//            aggr.getAggCallList().get(0).getAggregation().getKind(),
//            aggr.getRowType()
//        );

        RexLiteral limit = scan.getCluster().getRexBuilder().makeLiteral(FIRST_VALUE, aggr.getRowType());

        IndexConditions idxConditions = new IndexConditions(null, null, null, Collections.singletonList(limit));

        IgniteIndexScan idxProbe = new IgniteIndexScan(scan.getCluster(), idxTraits, scan.getTable(), idx.name(),
            null, limit, idxConditions, scan.requiredColumns(), idx.collation());

        // TODO: read multiple aggregates.
        AggregateCall idxSumAggCall = AggregateCall.create(
            //TODO: aggergate call kind
            SqlStdOperatorTable.MIN,
            false,
            false,
            false,
            ImmutableIntList.of(0),
            -1,
            null,
            RelCollations.EMPTY,
            0,
            idxProbe,
            null,
            null);

        LogicalAggregate newRel = new LogicalAggregate(
            aggr.getCluster(),
            aggr.getTraitSet(),
            Collections.emptyList(),
            idxProbe,
            aggr.getGroupSet(),
            aggr.getGroupSets(),
            Stream.generate(() -> idxSumAggCall).limit(aggr.getAggCallList().size()).collect(Collectors.toList())
        );
//
//        // SUM0/DECIMAL to COUNT()/Long converter.
//        List<RexNode> proj = new ArrayList<>();
//        RexBuilder rexBuilder = scan.getCluster().getRexBuilder();
//
//        for (int i = 0; i < aggr.getAggCallList().size(); ++i)
//            proj.add(rexBuilder.makeCast(aggr.getAggCallList().get(i).getType(), rexBuilder.makeInputRef(newRel, i)));
//
//        IgniteProject castToLongNode = new IgniteProject(
//            newRel.getCluster(),
//            aggr.getTraitSet().replace(IgniteConvention.INSTANCE),
//            newRel,
//            proj, aggr.getRowType());
//
        call.transformTo(newRel, ImmutableMap.of(newRel, aggr));
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
