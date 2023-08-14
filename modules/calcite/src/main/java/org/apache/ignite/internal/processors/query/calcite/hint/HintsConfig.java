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
import org.apache.calcite.rel.hint.HintOptionChecker;
import org.apache.calcite.rel.hint.HintStrategy;
import org.apache.calcite.rel.hint.HintStrategyTable;
import org.apache.calcite.rel.hint.RelHint;
import org.apache.calcite.util.Litmus;

/**
 * Provides configuration of all the supported SQL hints.
 */
public final class HintsConfig {
    /** */
    private HintsConfig() {
        // No-op.
    }

    /** Allows no option. */
    static final HintOptionChecker OPTS_CHECK_EMPTY = new HintOptionChecker() {
        @Override public boolean checkOptions(RelHint hint, Litmus errorHandler) {
            return errorHandler.check(
                hint.kvOptions.isEmpty() && hint.listOptions.isEmpty(),
                "Hint '{}' can't have any option.",
                hint.hintName
            );
        }
    };

    /** Allows only plain options. */
    static final HintOptionChecker OPTS_CHECK_PLAIN = new HintOptionChecker() {
        @Override public boolean checkOptions(RelHint hint, Litmus errorHandler) {
            return errorHandler.check(
                hint.kvOptions.isEmpty() && !hint.listOptions.isEmpty(),
                "Hint '{}' must have at least one plain option and no any key-value option.",
                hint.hintName
            );
        }
    };

    /** Allows any hint options. */
    static final HintOptionChecker OPTS_CHECK_SINGLE = new HintOptionChecker() {
        @Override public boolean checkOptions(RelHint hint, Litmus errorHandler) {
            return errorHandler.check(
                hint.listOptions.size() == 1 || hint.kvOptions.size() == 1,
                "Hint '{}' must have exactly one plain or key-value option.",
                hint.hintName
            );
        }
    };

    /** Allows any hint options. */
    static final HintOptionChecker OPTS_CHECK_ANY = new HintOptionChecker() {
        @Override public boolean checkOptions(RelHint hint, Litmus errorHandler) {
            return errorHandler.succeed();
        }
    };

    /**
     * @return Configuration of all the supported hints.
     */
    public static HintStrategyTable buildHintTable() {
        HintStrategyTable.Builder b = HintStrategyTable.builder().errorHandler(Litmus.THROW);

        Arrays.stream(HintDefinition.values()).forEach(hintDef ->
            b.hintStrategy(hintDef.name(), HintStrategy.builder(hintDef.predicate())
                .optionChecker(hintDef.optionsChecker()).build()));

        return b.build();
    }
}
