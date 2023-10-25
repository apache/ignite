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
import org.apache.ignite.internal.processors.query.calcite.rel.logical.IgniteLogicalTableScan;

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
            return Arrays.asList(CoreRules.JOIN_COMMUTE, JoinPushThroughJoinRule.LEFT, JoinPushThroughJoinRule.RIGHT);
        }
    },

    /** Disables indexes. */
    NO_INDEX {
        /** {@inheritDoc} */
        @Override public HintPredicate predicate() {
            return (hint, rel) -> rel instanceof IgniteLogicalTableScan;
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
    };

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
