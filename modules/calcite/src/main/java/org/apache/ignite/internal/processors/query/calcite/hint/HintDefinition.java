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

import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.hint.HintOptionChecker;
import org.apache.calcite.rel.hint.HintPredicate;
import org.apache.calcite.rel.hint.HintPredicates;
import org.apache.calcite.rel.hint.RelHint;
import org.apache.ignite.internal.processors.query.calcite.rel.logical.IgniteLogicalTableScan;

/**
 * Holds supported hints and their settings.
 */
public enum HintDefinition {
    /** Selects query engine. */
    QUERY_ENGINE,

    /** Disables converter rules. */
    DISABLE_RULE,

    /** */
    EXPAND_DISTINCT_AGG {
        /** {@inheritDoc} */
        @Override public HintPredicate predicate() {
            return HintPredicates.AGGREGATE;
        }

        /** {@inheritDoc} */
        @Override public HintOptionChecker optionsChecker() {
            return HintsConfig.OPTS_CHECK_EMPTY;
        }
    },

    /** Disables indexes. */
    NO_INDEX {
        /** {@inheritDoc} */
        @Override public HintPredicate predicate() {
            return new HintPredicate() {
                @Override public boolean apply(RelHint hint, RelNode rel) {
                    return rel instanceof IgniteLogicalTableScan;
                }
            };
        }

        /** {@inheritDoc} */
        @Override public HintOptionChecker optionsChecker() {
            return HintsConfig.OPTS_CHECK_ANY;
        }
    };

    /**
     * @return Hint predicate which limits redundant hint copying to reduce resources usage.
     */
    HintPredicate predicate() {
        return HintPredicates.SET_VAR;
    }

    /**
     * @return Hint options validator.
     */
    HintOptionChecker optionsChecker() {
        return HintsConfig.OPTS_CHECK_PLAIN;
    }
}
