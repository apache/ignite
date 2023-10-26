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

package org.apache.ignite.internal.processors.query.calcite.hint;

import java.util.Arrays;
import org.apache.calcite.plan.RelOptRule;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.hint.HintPredicate;
import org.apache.calcite.rel.hint.HintStrategy;
import org.apache.calcite.rel.hint.HintStrategyTable;
import org.apache.calcite.rel.hint.RelHint;
import org.apache.calcite.util.Litmus;
import org.apache.ignite.internal.util.typedef.F;
import org.jetbrains.annotations.Nullable;

/**
 * Provides configuration of the supported SQL hints.
 */
public final class HintsConfig {
    /** */
    private HintsConfig() {
        // No-op.
    }

    /** Allows no key-value option. */
    static final HintOptionsChecker OPTS_CHECK_NO_KV = new HintOptionsChecker() {
        /** {@inheritDoc} */
        @Override public @Nullable String apply(RelHint hint) {
            return hint.kvOptions.isEmpty()
                ? null
                : String.format("Hint '%s' can't have any key-value option (not supported).", hint.hintName);
        }
    };

    /** Allows no option. */
    static final HintOptionsChecker OPTS_CHECK_EMPTY = new HintOptionsChecker() {
        @Override public @Nullable String apply(RelHint hint) {
            String noKv = OPTS_CHECK_NO_KV.apply(hint);

            if (noKv != null)
                return noKv;

            return hint.kvOptions.isEmpty() && hint.listOptions.isEmpty()
                ? null
                : String.format("Hint '%s' can't have any option.", hint.hintName);
        }
    };

    /** Allows only plain options. */
    static final HintOptionsChecker OPTS_CHECK_PLAIN = new HintOptionsChecker() {
        @Override public @Nullable String apply(RelHint hint) {
            String noKv = OPTS_CHECK_NO_KV.apply(hint);

            if (noKv != null)
                return noKv;

            return !hint.listOptions.isEmpty()
                ? null
                : String.format("Hint '%s' must have at least one option.", hint.hintName);
        }
    };

    /**
     * @return Configuration of all the supported hints.
     */
    public static HintStrategyTable buildHintTable() {
        HintStrategyTable.Builder b = HintStrategyTable.builder().errorHandler(Litmus.IGNORE);

        RelOptRule[] disabledRulesTpl = new RelOptRule[0];

        Arrays.stream(HintDefinition.values()).forEach(hintDef ->
            b.hintStrategy(hintDef.name(), HintStrategy.builder(hintPredicate(hintDef))
                .excludedRules(hintDef.disabledRules().toArray(disabledRulesTpl)).build()));

        return b.build();
    }

    /**
     * Adds hint options checker to {@link HintPredicate} if {@code hintDef} has rules to exclude.
     *
     * @return Hint predicate.
     */
    private static HintPredicate hintPredicate(HintDefinition hintDef) {
        if (F.isEmpty(hintDef.disabledRules()))
            return hintDef.predicate();

        return new HintPredicate() {
            @Override public boolean apply(RelHint hint, RelNode rel) {
                if (!hintDef.predicate().apply(hint, rel))
                    return false;

                String optsErrMsg = hintDef.optionsChecker().apply(hint);

                if (F.isEmpty(optsErrMsg))
                    return true;

                HintUtils.skippedHint(rel, hint, optsErrMsg);

                return false;
            }
        };
    }
}
