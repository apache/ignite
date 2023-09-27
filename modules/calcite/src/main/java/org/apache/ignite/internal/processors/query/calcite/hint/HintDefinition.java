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

import org.apache.calcite.rel.hint.HintPredicate;
import org.apache.calcite.rel.hint.HintPredicates;
import org.apache.ignite.internal.processors.query.calcite.rel.logical.IgniteLogicalTableScan;

/**
 * Holds supported SQL hints and their settings.
 */
public enum HintDefinition {
    /** Sets the query engine like H2 or Calcite. Is preprocessed by regexp. */
    QUERY_ENGINE,

    /** Disables planner rules. */
    DISABLE_RULE,

    /** Forces expanding of distinct aggregates to join. */
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

    /** Forces join order as appears in the query. Fastens building of joins plan. */
    ORDERED_JOINS {
        /** {@inheritDoc} */
        @Override public HintPredicate predicate() {
            return HintPredicates.JOIN;
        }

        /** {@inheritDoc} */
        @Override public HintOptionsChecker optionsChecker() {
            return HintsConfig.OPTS_CHECK_EMPTY;
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
}
