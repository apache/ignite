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
import java.util.Collection;
import java.util.EnumMap;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashSet;
import java.util.Map;
import java.util.Set;
import java.util.stream.Stream;
import org.apache.calcite.plan.RelOptRuleCall;
import org.apache.calcite.plan.volcano.RelSubset;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.Join;
import org.apache.calcite.rel.hint.RelHint;
import org.apache.calcite.rel.logical.LogicalJoin;
import org.apache.ignite.internal.processors.query.calcite.hint.HintDefinition;
import org.apache.ignite.internal.processors.query.calcite.hint.HintUtils;

import static org.apache.calcite.util.Util.last;
import static org.apache.ignite.internal.processors.query.calcite.hint.HintDefinition.CNL_JOIN;
import static org.apache.ignite.internal.processors.query.calcite.hint.HintDefinition.MERGE_JOIN;
import static org.apache.ignite.internal.processors.query.calcite.hint.HintDefinition.NL_JOIN;
import static org.apache.ignite.internal.processors.query.calcite.hint.HintDefinition.NO_CNL_JOIN;
import static org.apache.ignite.internal.processors.query.calcite.hint.HintDefinition.NO_MERGE_JOIN;
import static org.apache.ignite.internal.processors.query.calcite.hint.HintDefinition.NO_NL_JOIN;

/** */
abstract class AbstractIgniteJoinConverterRule extends AbstractIgniteConverterRule<LogicalJoin> {
    /** Known join type hints and the opposite hints. */
    private static final EnumMap<HintDefinition, HintDefinition> HINTS = new EnumMap<>(HintDefinition.class);

    /** Known join type hints as flat array. */
    private static final HintDefinition[] ALL_HINTS;

    /** Hint disabing this join type. */
    private final HintDefinition knownDisableHint;

    /** Hint forcing usage of this join type. */
    private final HintDefinition knownForceHint;

    static {
        HINTS.put(NL_JOIN, NO_NL_JOIN);
        HINTS.put(CNL_JOIN, NO_CNL_JOIN);
        HINTS.put(MERGE_JOIN, NO_MERGE_JOIN);

        ALL_HINTS = Stream.concat(HINTS.keySet().stream(), HINTS.values().stream()).toArray(HintDefinition[]::new);
    }

    /** */
    protected AbstractIgniteJoinConverterRule(String descriptionPrefix, HintDefinition forceHint) {
        super(LogicalJoin.class, descriptionPrefix);

        assert HINTS.containsKey(forceHint);

        knownDisableHint = HINTS.get(forceHint);
        knownForceHint = forceHint;
    }

    /** {@inheritDoc} */
    @Override public final boolean matches(RelOptRuleCall call) {
        return super.matches(call) && matchesJoin(call) && !disabledByHints(call.rel(0));
    }

    /** */
    private boolean disabledByHints(LogicalJoin join) {
        if (HintUtils.allRelHints(join).isEmpty())
            return false;

        boolean ruleDisabled = false;

        Map<String, Collection<HintDefinition>> hintedTables = new HashMap<>();

        Set<String> joinTbls = joinTblNames(join);

        assert joinTbls.size() < 3;

        for (RelHint hint : HintUtils.hints(join, ALL_HINTS)) {
            Set<String> matchedTbls = hint.listOptions.isEmpty() ? joinTbls : new HashSet<>(hint.listOptions);

            if (!hint.listOptions.isEmpty())
                matchedTbls.retainAll(joinTbls);

            if (matchedTbls.isEmpty())
                continue;

            HintDefinition curHintDef = HintDefinition.valueOf(hint.hintName);
            boolean curHintIsDisable = !HINTS.containsKey(curHintDef);
            boolean skipHint = false;

            for (String tbl : joinTbls) {
                Collection<HintDefinition> prevTblHints = hintedTables.get(tbl);

                if (prevTblHints == null)
                    continue;

                Set<HintDefinition> disabled = null;

                for (HintDefinition prevTblHint : prevTblHints) {
                    boolean prevHintIsDisable = !HINTS.containsKey(prevTblHint);

                    if (prevHintIsDisable) {
                        if (disabled == null)
                            disabled = new HashSet<>();

                        disabled.add(prevTblHint);
                    }

                    // Prohibited: disabling all join types, combinations of forcing and disabling same join type,
                    // forcing of different join types.
                    if (curHintIsDisable && (disabled != null && disabled.size() == HINTS.size() - 1)
                        || isMutuallyExclusive(curHintDef, prevTblHint))
                        skipHint = true;
                }
            }

            if (skipHint) {
                HintUtils.skippedHint(join, hint, "This join type is already disabled or forced to use before " +
                    "by previous hints");

                continue;
            }

            for (String tbl : matchedTbls)
                hintedTables.computeIfAbsent(tbl, t -> new ArrayList<>()).add(curHintDef);

            // This join type is directyly disabled or other join type is forced.
            if (curHintIsDisable && curHintDef == knownDisableHint || !curHintIsDisable && knownForceHint != curHintDef)
                ruleDisabled = true;
        }

        return ruleDisabled;
    }

    /**
     * @return {@code True} if {@code curHint} and {@code prevHint} cannot be applied both. {@code False} otherwise.
     */
    private static boolean isMutuallyExclusive(HintDefinition curHint, HintDefinition prevHint) {
        if (curHint == prevHint)
            return false;

        HintDefinition curDisable = HINTS.get(curHint);
        HintDefinition prevDisable = HINTS.get(prevHint);

        return curDisable != null && prevDisable != null || curDisable == prevHint || curHint == prevDisable;
    }

    /** */
    protected static Set<String> joinTblNames(Join join) {
        Set<String> res = new LinkedHashSet<>();

        for (RelNode in : join.getInputs()) {
            if (in instanceof RelSubset)
                in = ((RelSubset)in).getOriginal();

            if (in.getTable() != null)
                res.add(last(in.getTable().getQualifiedName()));
        }

        return res;
    }

    /**
     * @return {@code True} if {@code call} is supported by current join rule. {@code False} otherwise.
     */
    protected boolean matchesJoin(RelOptRuleCall call) {
        return true;
    }
}
