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
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.hint.Hintable;
import org.apache.calcite.rel.hint.RelHint;
import org.apache.calcite.rel.logical.LogicalAggregate;
import org.apache.ignite.internal.processors.query.calcite.prepare.PlanningContext;
import org.apache.ignite.internal.util.typedef.F;

/**
 * Base class for working with Calcite's SQL hints.
 */
public final class Hint {
    /** */
    private Hint() {
        // No-op.
    }

    /**
     * @return All hints of {@code rel} if it is {@code Hintable}.
     */
    public static List<RelHint> allHints(RelNode rel) {
        return rel instanceof Hintable ? ((Hintable)rel).getHints() : Collections.emptyList();
    }

    /**
     * @return Hints if any is found by {@code hintDef} in {@code hints}. Empty collection if hints are not found.
     */
    public static List<RelHint> filterHints(Collection<RelHint> hints, HintDefinition hintDef) {
        return hints.stream().filter(h -> h.hintName.equals(hintDef.name())).collect(Collectors.toList());
    }

    /**
     * @return Combined distinct query (root) hints and hints of {@code rel} witout hint inherit pathes.
     * Empty collection if hints are not found or if {@code rel} is not {@code Hintable}.
     * @see PlanningContext#queryHints()
     * @see #removeInheritPath(RelHint)
     */
    public static List<RelHint> queryAndNodeHints(RelNode rel, HintDefinition hintDef) {
        if (!(rel instanceof Hintable))
            return Collections.emptyList();

        RelOptCluster cl = rel.getCluster();

        List<RelHint> finteredQueryHints = cl.getHintStrategies()
            .apply(filterHints(cl.getPlanner().getContext().unwrap(PlanningContext.class).queryHints(), hintDef), rel);

        return Stream.concat(finteredQueryHints.stream(), filterHints(((Hintable)rel).getHints(), hintDef).stream())
            .map(Hint::removeInheritPath).distinct().collect(Collectors.toList());
    }

    /**
     * @return Combined options of {@code rel}'s hints and of the query hints if any is found bu {@code hintDef}.
     * Empty options if no hint is found.
     * @see HintOptions#notFound()
     * @see PlanningContext#queryHints()
     */
    public static HintOptions options(RelNode rel, HintDefinition hintDef) {
        return HintOptions.collect(queryAndNodeHints(rel, hintDef));
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

    /**
     * @return Hint witout the inherit path.
     * @see org.apache.calcite.rel.hint.RelHint#inheritPath
     */
    private static RelHint removeInheritPath(RelHint hint) {
        RelHint.Builder b = RelHint.builder(hint.hintName).hintOptions(hint.listOptions);

        if (!F.isEmpty(hint.kvOptions))
            b = b.hintOptions(hint.kvOptions);

        return b.build();
    }
}
