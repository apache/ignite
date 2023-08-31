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
import java.util.HashSet;
import java.util.List;
import java.util.Set;
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

/** */
abstract class AbstractIgniteJoinConverterRule extends AbstractIgniteConverterRule<LogicalJoin> {
    /** Known force join type hints. */
    private static final Set<HintDefinition> FORCE_HINTS;

    static {
        FORCE_HINTS = new HashSet<>();

        FORCE_HINTS.add(HintDefinition.MERGE_JOIN);
        FORCE_HINTS.add(HintDefinition.NL_JOIN);
        FORCE_HINTS.add(HintDefinition.CNL_JOIN);
    }

    /** Hint which disabled this join type. */
    private final HintDefinition disableHint;

    /** Hint which forces usage of this join type. */
    private final HintDefinition forceHint;

    /** Hints to search for. */
    private final HintDefinition[] hintsToProcess;

    /** */
    protected AbstractIgniteJoinConverterRule(String descriptionPrefix, HintDefinition forceHint,
        HintDefinition disableHint) {
        super(LogicalJoin.class, descriptionPrefix);

        assert forceHint != disableHint;
        assert FORCE_HINTS.contains(forceHint);
        assert !FORCE_HINTS.contains(disableHint);

        this.disableHint = disableHint;
        this.forceHint = forceHint;

        List<HintDefinition> hintsToProcess = new ArrayList<>(FORCE_HINTS);
        hintsToProcess.add(disableHint);
        this.hintsToProcess = hintsToProcess.toArray(new HintDefinition[0]);
    }

    /** {@inheritDoc} */
    @Override public final boolean matches(RelOptRuleCall call) {
        return super.matches(call) && !disabledByHints(call.rel(0)) && matchesCall(call);
    }

    /** */
    private boolean disabledByHints(LogicalJoin join) {
        // Disabled - negative, enabled - positive. 0 - no action.
        int forcedOrDisabled = 0;

        List<String> joinTbls = joinTblNames(join);

        for (RelHint hint : Hint.hints(join, hintsToProcess)) {
            if (hint.listOptions.isEmpty()) {
                if (forcedOrDisabled != 0) {
                    Commons.planContext(join).skippedHint(hint, null, "This join type is already " +
                        "disabled or forced to use by previous hints.");
                }
            }
            else {
                boolean tblFound = false;

                for (String hintTblName : hint.listOptions) {
                    if (!joinTbls.contains(hintTblName))
                        continue;

                    tblFound = true;

                    if (forcedOrDisabled != 0) {
                        Commons.planContext(join).skippedHint(hint, hintTblName, "This join type is already " +
                            "disabled or forced to use by previous optons or other hints for table '" + hintTblName
                            + "'.");
                    }
                }

                if (!tblFound)
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
    protected static List<String> joinTblNames(Join join) {
        List<String> res = new ArrayList<>();

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
