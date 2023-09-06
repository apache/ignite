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

import java.util.Arrays;
import java.util.HashSet;
import java.util.LinkedHashSet;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.apache.calcite.plan.RelOptRuleCall;
import org.apache.calcite.plan.volcano.RelSubset;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.convert.ConverterRule;
import org.apache.calcite.rel.core.Join;
import org.apache.calcite.rel.core.TableScan;
import org.apache.calcite.rel.hint.RelHint;
import org.apache.calcite.rel.logical.LogicalJoin;
import org.apache.ignite.internal.processors.query.calcite.hint.Hint;
import org.apache.ignite.internal.processors.query.calcite.hint.HintDefinition;
import org.apache.ignite.internal.processors.query.calcite.util.Commons;

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
    private static final HintDefinition[] HINTS = new HintDefinition[] {
        MERGE_JOIN, NL_JOIN, CNL_JOIN,
        NO_MERGE_JOIN, NO_NL_JOIN, NO_CNL_JOIN};

    /** Known force join type hints. */
    private static final Set<HintDefinition> FORCE_HINTS =
        Stream.of(Arrays.copyOfRange(HINTS, 0, HINTS.length / 2)).collect(Collectors.toSet());

    /** Known disable join type hints. */
    private static final Set<HintDefinition> DISABLE_HINTS =
        Stream.of(Arrays.copyOfRange(HINTS, HINTS.length / 2, HINTS.length)).collect(Collectors.toSet());

    /** */
    public static final String SKIPPED_HINT_PREFIX = "This join type is already disabled or forced to use before " +
        "by previous hints";

    /** Hint disabing this join type. */
    private final HintDefinition disableHint;

    /** Hint forcing usage of this join type. */
    private final HintDefinition forceHint;

    /** */
    protected AbstractIgniteJoinConverterRule(String descriptionPrefix, HintDefinition forceHint,
        HintDefinition disableHint) {
        super(LogicalJoin.class, descriptionPrefix);

        assert forceHint != disableHint;
        assert FORCE_HINTS.contains(forceHint);
        assert DISABLE_HINTS.contains(disableHint);
        assert !FORCE_HINTS.contains(disableHint);
        assert !DISABLE_HINTS.contains(forceHint);

        this.disableHint = disableHint;
        this.forceHint = forceHint;
    }

    /** {@inheritDoc} */
    @Override public final boolean matches(RelOptRuleCall call) {
        return super.matches(call) && !disabledByHints(call.rel(0)) && matchesCall(call);
    }

    /** */
    private boolean disabledByHints(LogicalJoin join) {
        // Disabled - negative, enabled - positive. 0 - no action.
        int forcedOrDisabled = 0;

        Set<String> hintedTables = new HashSet<>();

        Set<String> joinTbls = joinTblNames(join);

        assert joinTbls.size() < 3;

        for (RelHint hint : Hint.hints(join, HINTS)) {
            boolean skip = false;

            for (String tbl : joinTbls) {
                if (hintedTables.contains(tbl)) {
                    skip = true;

                    break;
                }
            }

            hintedTables.addAll(joinTbls);

            if (!skip && !hint.listOptions.isEmpty() && forcedOrDisabled != 0)
                skip = true;

            if (skip) {
                Commons.planContext(join).skippedHint(join, hint, null, SKIPPED_HINT_PREFIX
                    + " for the tables " + joinTbls.stream().map(t -> '\'' + t + '\'')
                    .collect(Collectors.joining(",")) + '.');

                continue;
            }

            if (!hint.listOptions.isEmpty()) {
                skip = true;

                for (String hintTblName : hint.listOptions) {
                    if (!joinTbls.contains(hintTblName))
                        continue;

                    skip = false;

                    if (forcedOrDisabled != 0) {
                        Commons.planContext(join).skippedHint(join, hint, hintTblName, SKIPPED_HINT_PREFIX
                            + " for the table '" + hintTblName + "'.");
                    }
                }

                if (skip)
                    continue;
            }

            if (forcedOrDisabled != 0)
                continue;

            if (FORCE_HINTS.contains(HintDefinition.valueOf(hint.hintName)))
                forcedOrDisabled = forceHint.name().equals(hint.hintName) ? 1 : -1;
            else if (disableHint.name().equals(hint.hintName) && activateDisableHint(join, hint))
                forcedOrDisabled = -1;
        }

        return forcedOrDisabled < 0;
    }

    /** */
    protected boolean activateDisableHint(LogicalJoin join, RelHint hint) {
        return true;
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
