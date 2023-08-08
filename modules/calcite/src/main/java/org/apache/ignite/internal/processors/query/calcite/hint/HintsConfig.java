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
import org.apache.ignite.internal.util.typedef.F;

/** */
public class HintsConfig {
    /** */
    static final HintOptionChecker OPTS_CHECK_EMPTY = new HintOptionChecker() {
        @Override public boolean checkOptions(RelHint hint, Litmus errorHandler) {
            return errorHandler.check(
                F.isEmpty(hint.kvOptions) && F.isEmpty(hint.listOptions),
                "Hint '{}' can't have any option.",
                hint.hintName
            );
        }
    };

    /** */
    static final HintOptionChecker OPTS_CHECK_PLAIN = new HintOptionChecker() {
        @Override public boolean checkOptions(RelHint hint, Litmus errorHandler) {
            return errorHandler.check(
                F.isEmpty(hint.kvOptions) && !F.isEmpty(hint.listOptions),
                "Hint '{}' must have at least one plain option.",
                hint.hintName
            );
        }
    };

    /** */
    static final HintOptionChecker OPTS_CHECK_ONE_PLAIN = new HintOptionChecker() {
        @Override public boolean checkOptions(RelHint hint, Litmus errorHandler) {
            return errorHandler.check(
                F.isEmpty(hint.kvOptions) && !F.isEmpty(hint.listOptions) && hint.listOptions.size() == 1,
                "Hint '{}' must have exactly one plain option.",
                hint.hintName
            );
        }
    };

    /** */
    static final HintOptionChecker OPTS_CHECK_ANY = new HintOptionChecker() {
        @Override public boolean checkOptions(RelHint hint, Litmus errorHandler) {
            return errorHandler.succeed();
        }
    };

    /** */
    public static HintStrategyTable buildHintTable() {
        HintStrategyTable.Builder b = HintStrategyTable.builder().errorHandler(Litmus.THROW);

        Arrays.stream(HintDefinition.values()).forEach(hintDef ->
            b.hintStrategy(hintDef.name(), HintStrategy.builder(hintDef.predicate())
                .optionChecker(hintDef.optionsChecker()).build()));

        return b.build();
    }
}
