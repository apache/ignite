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
import java.util.List;
import java.util.stream.Collectors;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.hint.HintStrategyTable;
import org.apache.calcite.rel.hint.Hintable;
import org.apache.calcite.rel.hint.RelHint;
import org.apache.ignite.internal.processors.query.calcite.prepare.PlannerHelper;
import org.apache.ignite.internal.processors.query.calcite.prepare.PlanningContext;
import org.apache.ignite.internal.util.typedef.F;
import org.jetbrains.annotations.Nullable;

/**
 * Base class for working with Calcite's SQL hints.
 */
public final class Hint {
    /** */
    private Hint() {
        // No-op.
    }

    /**
     * @return Hints filtered with {@code hintDef} and suitable for {@code rel}.
     * @see HintStrategyTable#apply(List, RelNode)
     * @see PlanningContext#hints()
     */
    public static Collection<RelHint> hints(RelNode rel, HintDefinition... hintDef) {
        if (!(rel instanceof Hintable))
            return Collections.emptyList();

        RelOptCluster cl = rel.getCluster();

        return F.flatCollections(Arrays.stream(hintDef)
            .map(hd -> cl.getHintStrategies().apply(filterHints(PlannerHelper.context(cl).hints(), hd), rel))
            .collect(Collectors.toList()));
    }

    /**
     * @return {@code True} if the query has a suitable hint for {@code rel} defined by {@code hintDef}.
     * {@code False} otherwise.
     */
    public static boolean hasHint(RelNode rel, HintDefinition hintDef) {
        return hints(rel, hintDef) != null;
    }

    /**
     * @return Combined options set of all {@code hints} filtered with {@code hintDef} with the natural order.
     * {@code Null} if no hint is found by {@code hintDef}.
     * @see PlanningContext#hints()
     */
    public static @Nullable HintOptions options(Collection<RelHint> hints, HintDefinition hintDef) {
        return HintOptions.collect(filterHints(hints, hintDef));
    }

    /**
     * @return All hints of {@code rel}.
     */
    public static List<RelHint> relHints(RelNode rel) {
        return rel instanceof Hintable ? ((Hintable)rel).getHints() : Collections.emptyList();
    }

    /**
     * @return Hints within {@code hints} filtered with {@code hintDef}.
     */
    private static List<RelHint> filterHints(Collection<RelHint> hints, HintDefinition hintDef) {
        return hints.stream().filter(h -> h.hintName.equalsIgnoreCase(hintDef.name())).collect(Collectors.toList());
    }
}
