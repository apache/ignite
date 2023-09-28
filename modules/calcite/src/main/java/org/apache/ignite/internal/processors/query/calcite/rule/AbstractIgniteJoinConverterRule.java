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
import java.util.stream.Collectors;
import org.apache.calcite.plan.RelOptRuleCall;
import org.apache.calcite.plan.volcano.RelSubset;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.convert.ConverterRule;
import org.apache.calcite.rel.core.Join;
import org.apache.calcite.rel.core.TableScan;
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
    /** Known join type hints. */
    private static final EnumMap<HintDefinition, HintDefinition> HINTS = new EnumMap<>(HintDefinition.class);

    /** Hint disabing this join type. */
    private final HintDefinition knownDisableHint;

    /** Hint forcing usage of this join type. */
    private final HintDefinition knownForceHint;

    static {
        HINTS.put(NL_JOIN, NO_NL_JOIN);
        HINTS.put(CNL_JOIN, NO_CNL_JOIN);
        HINTS.put(MERGE_JOIN, NO_MERGE_JOIN);
    }

    /** */
    protected AbstractIgniteJoinConverterRule(String descriptionPrefix, HintDefinition forceHint,
        HintDefinition disableHint) {
        super(LogicalJoin.class, descriptionPrefix);

        assert forceHint != disableHint;
        assert HINTS.containsKey(forceHint);
        assert HINTS.containsValue(disableHint);
        assert !HINTS.containsKey(disableHint);
        assert !HINTS.containsValue(forceHint);

        knownDisableHint = disableHint;
        knownForceHint = forceHint;
    }

    /** {@inheritDoc} */
    @Override public final boolean matches(RelOptRuleCall call) {
        return super.matches(call) && matchesCall(call) && !disabledByHints(call.rel(0));
    }

    /** */
    private boolean disabledByHints(LogicalJoin join) {
        if (HintUtils.allRelHints(join).isEmpty())
            return false;

        boolean ruleDisabled = false;

        Map<String, Collection<HintDefinition>> hintedTables = new HashMap<>();

        Set<String> joinTbls = joinTblNames(join);

        assert joinTbls.size() < 3;

        for (RelHint hint : HintUtils.hints(join, HINTS.keySet(), HINTS.values())) {
            Set<String> matchedTbls = hint.listOptions.isEmpty() ? joinTbls : new HashSet<>(hint.listOptions);

            if (!hint.listOptions.isEmpty())
                matchedTbls.retainAll(joinTbls);

            if (matchedTbls.isEmpty())
                continue;

            HintDefinition curHintDef = HintDefinition.valueOf(hint.hintName);
            boolean curHintIsDisable = !HINTS.containsKey(curHintDef);
            boolean unableToProcess = false;

            for (String tbl : joinTbls) {
                Collection<HintDefinition> prevTblHints = hintedTables.get(tbl);

                if (prevTblHints == null)
                    continue;

                int disableCnt = 0;

                for (HintDefinition prevTblHint : prevTblHints) {
                    boolean prevHintIsDisable = !HINTS.containsKey(prevTblHint);

                    if (prevHintIsDisable)
                        ++disableCnt;

                    boolean opposite = prevTblHint != curHintDef && (!prevHintIsDisable || !curHintIsDisable)
                        && (prevTblHint == HINTS.get(curHintDef) || curHintDef == HINTS.get(prevTblHint)
                        || (!curHintIsDisable && HINTS.get(prevTblHint) != null));

                    // Prohibited: disabling all join types, combinations of forcing and disabling same join type,
                    // forcing of different join types.
                    if (curHintIsDisable && disableCnt == HINTS.size() - 1 || opposite)
                        unableToProcess = true;
                }
            }

            if (unableToProcess) {
                HintUtils.skippedHint(join, hint,
                    "This join type is already disabled or forced to use before by previous hints");

                continue;
            }

            for (String tbl : matchedTbls) {
                hintedTables.compute(tbl, (t, tblHints) -> {
                    if (tblHints == null)
                        tblHints = new ArrayList<>();

                    tblHints.add(curHintDef);

                    return tblHints;
                });
            }

            // This join type is directyly disabled or other join type is forced.
            if (curHintIsDisable && curHintDef == knownDisableHint || !curHintIsDisable && knownForceHint != curHintDef)
                ruleDisabled = true;
        }

        return ruleDisabled;
    }

    /** */
    private static String relDescription(Collection<String> tbls) {
        return "Join tables " + tbls.stream().sorted().collect(Collectors.joining(","));
    }

    /** */
    protected static Set<String> joinTblNames(Join join) {
        Set<String> res = new LinkedHashSet<>();

        for (RelNode in : join.getInputs()) {
            if (in instanceof RelSubset) {
                for (RelNode sub : ((RelSubset)in).getRels()) {
                    if (sub instanceof TableScan)
                        res.add(last(((TableScan)sub).getTable().getQualifiedName()));
                }
            }
            else if (in instanceof TableScan) {
                res.add(last(((TableScan)in).getTable().getQualifiedName()));
            }
        }

        return res;
    }

    /**
     * The seme as {@link ConverterRule#matches(RelOptRuleCall)}
     */
    protected boolean matchesCall(RelOptRuleCall call) {
        return true;
    }
}
