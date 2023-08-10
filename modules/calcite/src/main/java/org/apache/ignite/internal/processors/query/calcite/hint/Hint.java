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

import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.hint.Hintable;
import org.apache.calcite.rel.hint.RelHint;
import org.apache.calcite.rel.logical.LogicalAggregate;
import org.apache.calcite.tools.Planner;
import org.apache.ignite.internal.processors.query.calcite.prepare.IgnitePlanner;
import org.apache.ignite.internal.processors.query.calcite.prepare.PlanningContext;

/**
 * Base class for working with Calcite's SQL hints.
 */
public final class Hint {
    /** */
    private Hint() {
        // No-op.
    }

    /**
     * @return Hints if any is found by {@code hintDef} in {@code hints}. Empty collection if hints are not found.
     */
    public static List<RelHint> allHints(Collection<RelHint> hints, HintDefinition hintDef) {
        return hints.stream().filter(h -> h.hintName.equals(hintDef.name())).collect(Collectors.toList());
    }

    /**
     * @return Distinct hints witout inherit paths of {@code rel} if any is found by {@code hintDef}.
     * Empty collection if hints are not found or if{@code rel} is not {@code Hintable}.
     */
    public static List<RelHint> distinctHints(RelNode rel, HintDefinition hintDef) {
        if (!(rel instanceof Hintable))
            return Collections.emptyList();

        Planner planner = rel.getCluster().getPlanner().getContext().unwrap(PlanningContext.class).planner();

        List<RelHint> rootHints = planner instanceof IgnitePlanner
            ? rel.getCluster().getHintStrategies().apply(((IgnitePlanner)planner).rootHints(), rel)
            : Collections.emptyList();

        List<RelHint> relHints = allHints(((Hintable)rel).getHints(), hintDef);

        return Stream.concat(rootHints.stream(), relHints.stream()).distinct().collect(Collectors.toList());
    }

    /**
     * @return Options collections of distinct hints if any hint is found by {@code hintDef} in {@code rel}.
     * Empty options if no hint is found.
     * @see HintOptions#notFound()
     */
    public static HintOptions options(RelNode rel, HintDefinition hintDef) {
        return HintOptions.collect(distinctHints(rel, hintDef));
    }

    /**
     * @return {@code True} if {@code rel} has any hint defined by {@code hintDef}. {@code False} otherwise.
     */
    public static boolean hasHint(LogicalAggregate rel, HintDefinition hintDef) {
        for (RelHint h : rel.getHints()) {
            if (h.hintName.equals(hintDef.name()))
                return true;
        }

        return false;
    }
}
