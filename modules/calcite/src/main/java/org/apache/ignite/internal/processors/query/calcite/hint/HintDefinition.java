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
import java.util.Collection;
import java.util.Collections;
import org.apache.calcite.plan.RelOptRule;
import org.apache.calcite.rel.hint.HintPredicate;
import org.apache.calcite.rel.hint.HintPredicates;
import org.apache.calcite.rel.rules.CoreRules;
import org.apache.calcite.rel.rules.JoinPushThroughJoinRule;

/**
 * Holds supported SQL hints and their settings.
 */
public enum HintDefinition {
    /** Sets the query engine like H2 or Calcite. Is preprocessed by regexp. */
    QUERY_ENGINE,

    /** Disables planner rules. */
    DISABLE_RULE,

    /**
     * If optimizer wraps aggregation operations with a join, forces expanding of only distinct aggregates to the
     * join. Removes duplicates before joining and speeds up it.
     */
    EXPAND_DISTINCT_AGG {
        /** {@inheritDoc} */
        @Override public HintPredicate predicate() {
            return HintPredicates.AGGREGATE;
        }

        /** {@inheritDoc} */
        @Override public HintOptionsChecker optionsChecker() {
            return HintsConfig.OPTS_CHECK_EMPTY;
        }
    },

    /** Forces join order as appears in query. Fastens building of joins plan. */
    ENFORCE_JOIN_ORDER {
        /** {@inheritDoc} */
        @Override public HintPredicate predicate() {
            return HintPredicates.JOIN;
        }

        /** {@inheritDoc} */
        @Override public HintOptionsChecker optionsChecker() {
            return HintsConfig.OPTS_CHECK_EMPTY;
        }

        /** {@inheritDoc} */
        @Override public Collection<RelOptRule> disabledRules() {
            // CoreRules#JOIN_COMMUTE also disables CoreRules.JOIN_COMMUTE_OUTER.
            // Disabling JoinToMultiJoinRule also disables dependent IgniteMultiJoinOptimizationRule because it won't
            // receive a MultiJoin. Disabling IgniteMultiJoinOptimizationRule in this way doesn't work because
            // MultiJoin is not Hintable and HintStrategyTable ignores it.
            return Arrays.asList(CoreRules.JOIN_COMMUTE, JoinPushThroughJoinRule.LEFT, JoinPushThroughJoinRule.RIGHT,
                CoreRules.JOIN_TO_MULTI_JOIN);
        }
    },

    /** Disables indexes. */
    NO_INDEX {
        /** {@inheritDoc} */
        @Override public HintPredicate predicate() {
            return HintPredicates.TABLE_SCAN;
        }

        /** {@inheritDoc} */
        @Override public HintOptionsChecker optionsChecker() {
            return HintsConfig.OPTS_CHECK_NO_KV;
        }
    },

    /** Forces index usage. */
    FORCE_INDEX {
        /** {@inheritDoc} */
        @Override public HintPredicate predicate() {
            return NO_INDEX.predicate();
        }

        /** {@inheritDoc} */
        @Override public HintOptionsChecker optionsChecker() {
            return NO_INDEX.optionsChecker();
        }
    },

    /** Forces merge join. */
    MERGE_JOIN {
        /** {@inheritDoc} */
        @Override public HintPredicate predicate() {
            return joinHintPredicate();
        }

        /** {@inheritDoc} */
        @Override public HintOptionsChecker optionsChecker() {
            return HintsConfig.OPTS_CHECK_NO_KV;
        }
    },

    /** Disables merge join. */
    NO_MERGE_JOIN {
        /** {@inheritDoc} */
        @Override public HintPredicate predicate() {
            return MERGE_JOIN.predicate();
        }

        /** {@inheritDoc} */
        @Override public HintOptionsChecker optionsChecker() {
            return MERGE_JOIN.optionsChecker();
        }
    },

    /** Forces nested loop join. */
    NL_JOIN {
        /** {@inheritDoc} */
        @Override public HintPredicate predicate() {
            return joinHintPredicate();
        }

        /** {@inheritDoc} */
        @Override public HintOptionsChecker optionsChecker() {
            return HintsConfig.OPTS_CHECK_NO_KV;
        }
    },

    /** Disables nested loop join. */
    NO_NL_JOIN {
        /** {@inheritDoc} */
        @Override public HintPredicate predicate() {
            return NL_JOIN.predicate();
        }

        /** {@inheritDoc} */
        @Override public HintOptionsChecker optionsChecker() {
            return NL_JOIN.optionsChecker();
        }
    },

    /** Forces correlated nested loop join. */
    CNL_JOIN {
        /** {@inheritDoc} */
        @Override public HintPredicate predicate() {
            return joinHintPredicate();
        }

        /** {@inheritDoc} */
        @Override public HintOptionsChecker optionsChecker() {
            return HintsConfig.OPTS_CHECK_NO_KV;
        }
    },

    /** Disables correlated nested loop join. */
    NO_CNL_JOIN {
        /** {@inheritDoc} */
        @Override public HintPredicate predicate() {
            return CNL_JOIN.predicate();
        }

        /** {@inheritDoc} */
        @Override public HintOptionsChecker optionsChecker() {
            return CNL_JOIN.optionsChecker();
        }
    };

    /**
     * @return Hint predicate for join hints.
     */
    private static HintPredicate joinHintPredicate() {
        // HintPredicates.VALUES might be mentioned too. But RelShuttleImpl#visit(LogicalValues) does nothing and ignores
        // setting any hints.
        return HintPredicates.or(HintPredicates.JOIN, HintPredicates.TABLE_SCAN);
    }

    /**
     * @return Hint predicate which limits redundant hint copying and reduces mem/cpu consumption.
     */
    HintPredicate predicate() {
        return HintPredicates.SET_VAR;
    }

    /**
     * @return {@link HintOptionsChecker}.
     */
    HintOptionsChecker optionsChecker() {
        return HintsConfig.OPTS_CHECK_PLAIN;
    }

    /**
     * @return Rules to excluded by current hint.
     */
    public Collection<RelOptRule> disabledRules() {
        return Collections.emptyList();
    }
}
