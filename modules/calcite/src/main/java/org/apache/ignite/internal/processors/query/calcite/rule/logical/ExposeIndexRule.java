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

import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;
import javax.annotation.Nullable;
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
import org.apache.ignite.internal.processors.query.calcite.hint.Hint;
import org.apache.ignite.internal.processors.query.calcite.hint.HintDefinition;
import org.apache.ignite.internal.processors.query.calcite.hint.HintOptions;
import org.apache.ignite.internal.processors.query.calcite.rel.AbstractIndexScan;
import org.apache.ignite.internal.processors.query.calcite.rel.logical.IgniteLogicalIndexScan;
import org.apache.ignite.internal.processors.query.calcite.rel.logical.IgniteLogicalTableScan;
import org.apache.ignite.internal.processors.query.calcite.schema.IgniteTable;
import org.apache.ignite.internal.processors.query.calcite.util.Commons;
import org.apache.ignite.internal.util.typedef.F;
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

        indexes = processHints(scan, indexes);

        if (indexes.isEmpty())
            return;

        Map<RelNode, RelNode> equivMap = new HashMap<>(indexes.size());
        for (int i = 1; i < indexes.size(); i++)
            equivMap.put(indexes.get(i), scan);

        call.transformTo(F.first(indexes), equivMap);
    }

    /** */
    private List<IgniteLogicalIndexScan> processHints(TableScan scan, List<IgniteLogicalIndexScan> indexes) {
        assert !F.isEmpty(indexes);

        List<String> qTblName = scan.getTable().getQualifiedName();
        Set<String> tblIdxNames = indexes.stream().map(AbstractIndexScan::indexName).collect(Collectors.toSet());
        Set<String> idxToSkip = new HashSet<>();

        for (RelHint hint : Hint.hints(scan, HintDefinition.NO_INDEX)) {
            if (idxToSkip.size() == indexes.size()) {
                Commons.planContext(scan).skippedHint(scan, hint, "Any index of table '" + last(qTblName) +
                    "' has already been skipped by the hints before.");

                continue;
            }

            HintOptions opts = Hint.options(hint);

            if (!opts.plain().isEmpty()) {
                storeIdxNamesToSkip(scan, hint, tblIdxNames, idxToSkip, null, opts.plain());

                assert opts.kv().isEmpty();

                continue;
            }

            opts.kv().forEach((hintTblName, hintIdxNames) -> {
                List<String> hintTblQName = Commons.qualifiedName(hintTblName);

                if (checkTblName(qTblName, hintTblQName))
                    storeIdxNamesToSkip(scan, hint, tblIdxNames, idxToSkip, hintTblName, hintIdxNames);
                else {
                    Commons.planContext(scan).skippedHint(scan, hint, hintTblName, "Incorrect table name: '"
                        + hintTblName + "'.");
                }
            });
        }

        return indexes.stream().filter(idx -> !idxToSkip.contains(idx.indexName())).collect(Collectors.toList());
    }

    /** */
    private void storeIdxNamesToSkip(TableScan scan, RelHint hint, Set<String> tblIdxNames, Set<String> idxToSkip,
        @Nullable String hintTableName, List<String> hintIdxNames) {
        for (String hintIdxName : hintIdxNames) {
            if (!tblIdxNames.contains(hintIdxName)) {
                Commons.planContext(scan).skippedHint(scan, hint, hintTableName, hintIdxName, "Table '" +
                    last(scan.getTable().getQualifiedName()) + "' has no index '" + hintIdxName + "'.");

                continue;
            }

            idxToSkip.add(hintIdxName);
        }
    }

    /** */
    private static boolean checkTblName(List<String> tblQName, List<String> hintTblQName) {
        assert !hintTblQName.isEmpty();

        return hintTblQName.size() == 1 && last(tblQName).equals(hintTblQName.get(0)) || F.eq(tblQName, hintTblQName);
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
