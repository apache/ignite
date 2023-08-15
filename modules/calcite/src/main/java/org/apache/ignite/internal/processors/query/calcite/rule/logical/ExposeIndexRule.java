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

import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
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
    private List<IgniteLogicalIndexScan> processHints(IgniteLogicalTableScan scan, List<IgniteLogicalIndexScan> indexes) {
        List<RelHint> hints = Hint.hints(scan, HintDefinition.NO_INDEX, HintDefinition.USE_INDEX);

        if (hints.isEmpty())
            return indexes;

        boolean disabled = !hints.get(0).hintName.equals(HintDefinition.USE_INDEX.name());

        HintOptions opts = Hint.options(hints, disabled ? HintDefinition.NO_INDEX : HintDefinition.USE_INDEX);

        if (opts == null)
            return indexes;

        if (disabled)
            removeIndexes(scan, indexes, opts);
        else if (indexes.size() > 1) {
            keepIndex(scan, indexes, opts);

            if (indexes.size() == 1)
                indexes = Collections.singletonList(indexes.get(0).setForced());
        }

        return indexes;
    }

    /** */
    private void keepIndex(TableScan scan, List<? extends AbstractIndexScan> indexes, HintOptions opts) {
        assert !opts.empty();

        // If there is no index with passed name, no other index should be removed.
        if (indexes.stream().noneMatch(idx -> idx.indexName().equals(opts.plain().iterator().next())))
            return;

        if (!opts.plain().isEmpty()) {
            indexes.removeIf(idx -> !opts.plain().contains(idx.indexName()));

            return;
        }

        List<String> qtname = scan.getTable().getQualifiedName();

        opts.kv().forEach((hintTblName, hintIdxNames) -> {
            List<String> qHintTblName = Commons.qualifiedName(hintTblName);

            indexes.removeIf(idx -> !hintIdxNames.contains(idx.indexName())
                && (last(qHintTblName).equals(last(qtname)) && qHintTblName.size() == 1 || F.eq(qHintTblName, qtname)));
        });
    }

    /** */
    private void removeIndexes(TableScan scan, List<? extends AbstractIndexScan> indexes, HintOptions opts) {
        if (opts.empty()) {
            indexes.clear();

            return;
        }

        if (!opts.plain().isEmpty()) {
            indexes.removeIf(idx -> opts.plain().contains(idx.indexName()));

            return;
        }

        List<String> qtname = scan.getTable().getQualifiedName();

        opts.kv().forEach((hintTblName, hintIdxNames) -> {
            List<String> qHintTblName = Commons.qualifiedName(hintTblName);

            indexes.removeIf(idx -> hintIdxNames.contains(idx.indexName())
                && (last(qHintTblName).equals(last(qtname)) && qHintTblName.size() == 1) || F.eq(qHintTblName, qtname));
        });
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
