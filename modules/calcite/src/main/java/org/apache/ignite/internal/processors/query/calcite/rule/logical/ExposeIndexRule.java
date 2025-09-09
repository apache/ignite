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

package org.apache.ignite.internal.processors.query.calcite.rule.logical;

import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelOptRule;
import org.apache.calcite.plan.RelOptRuleCall;
import org.apache.calcite.plan.RelOptTable;
import org.apache.calcite.plan.RelRule;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.TableScan;
import org.apache.calcite.rel.hint.RelHint;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.util.ImmutableBitSet;
import org.apache.ignite.internal.processors.query.calcite.hint.HintDefinition;
import org.apache.ignite.internal.processors.query.calcite.hint.HintUtils;
import org.apache.ignite.internal.processors.query.calcite.rel.AbstractIndexScan;
import org.apache.ignite.internal.processors.query.calcite.rel.logical.IgniteLogicalIndexScan;
import org.apache.ignite.internal.processors.query.calcite.rel.logical.IgniteLogicalTableScan;
import org.apache.ignite.internal.processors.query.calcite.schema.IgniteTable;
import org.apache.ignite.internal.util.typedef.F;
import org.apache.ignite.lang.IgniteBiTuple;
import org.immutables.value.Value;

import static org.apache.calcite.util.Util.last;

/**
 *
 */
@Value.Enclosing
public class ExposeIndexRule extends RelRule<ExposeIndexRule.Config> {
    /** */
    public static final RelOptRule INSTANCE = Config.DEFAULT.toRule();

    /**
     * Constructor
     *
     * @param config Expose index rule config.
     */
    public ExposeIndexRule(Config config) {
        super(config);
    }

    /** */
    private static boolean preMatch(IgniteLogicalTableScan scan) {
        return !scan.getTable().unwrap(IgniteTable.class).indexes().isEmpty(); // has indexes to expose
    }

    /** {@inheritDoc} */
    @Override public void onMatch(RelOptRuleCall call) {
        IgniteLogicalTableScan scan = call.rel(0);
        RelOptCluster cluster = scan.getCluster();

        RelOptTable optTable = scan.getTable();
        IgniteTable igniteTable = optTable.unwrap(IgniteTable.class);
        List<RexNode> proj = scan.projects();
        RexNode condition = scan.condition();
        ImmutableBitSet requiredCols = scan.requiredColumns();

        if (igniteTable.isIndexRebuildInProgress())
            return;

        List<IgniteLogicalIndexScan> indexes = igniteTable.indexes().values().stream()
            .map(idx -> idx.toRel(cluster, optTable, proj, condition, requiredCols))
            .collect(Collectors.toList());

        assert !indexes.isEmpty();

        IgniteBiTuple<List<IgniteLogicalIndexScan>, Boolean> hintedIndexes = processHints(scan, indexes);

        indexes = hintedIndexes.get1();

        if (indexes.isEmpty())
            return;

        if (hintedIndexes.get2())
            cluster.getPlanner().prune(scan);

        Map<RelNode, RelNode> equivMap = new HashMap<>(indexes.size());
        for (int i = 1; i < indexes.size(); i++)
            equivMap.put(indexes.get(i), scan);

        call.transformTo(F.first(indexes), equivMap);
    }

    /**
     * @return Actual indixes list and prune-table-scan flag if any index is forced to use.
     */
    private IgniteBiTuple<List<IgniteLogicalIndexScan>, Boolean> processHints(
        TableScan scan,
        List<IgniteLogicalIndexScan> indexes
    ) {
        assert !F.isEmpty(indexes);

        Set<String> tblIdxNames = indexes.stream().map(AbstractIndexScan::indexName).collect(Collectors.toSet());
        Set<String> idxToSkip = new HashSet<>();
        Set<String> idxToUse = new HashSet<>();

        for (RelHint hint : HintUtils.hints(scan, HintDefinition.NO_INDEX, HintDefinition.FORCE_INDEX)) {
            boolean skip = !hint.hintName.equals(HintDefinition.FORCE_INDEX.name());

            Collection<String> hintIdxNames = hint.listOptions.isEmpty() ? tblIdxNames : hint.listOptions;

            for (String hintIdxName : hintIdxNames) {
                if (!tblIdxNames.contains(hintIdxName))
                    continue;

                if (idxToSkip.contains(hintIdxName) || idxToUse.contains(hintIdxName)) {
                    HintUtils.skippedHint(scan, hint, "Index '" + hintIdxName
                        + "' of table '" + last(scan.getTable().getQualifiedName())
                        + "' has already been excluded or forced to use by other options or other hints before.");

                    continue;
                }

                if (skip)
                    idxToSkip.add(hintIdxName);
                else
                    idxToUse.add(hintIdxName);
            }
        }

        return new IgniteBiTuple<>(indexes.stream().filter(idx -> !idxToSkip.contains(idx.indexName())
            && (idxToUse.isEmpty() || idxToUse.contains(idx.indexName()))).collect(Collectors.toList()),
            !idxToUse.isEmpty());
    }

    /** */
    @SuppressWarnings("ClassNameSameAsAncestorName")
    @Value.Immutable
    public interface Config extends RelRule.Config {
        /** */
        Config DEFAULT = ImmutableExposeIndexRule.Config.of()
            .withOperandSupplier(b ->
                b.operand(IgniteLogicalTableScan.class)
                    .predicate(ExposeIndexRule::preMatch)
                    .anyInputs());

        /** {@inheritDoc} */
        @Override default ExposeIndexRule toRule() {
            return new ExposeIndexRule(this);
        }
    }
}
