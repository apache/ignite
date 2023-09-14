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
import java.util.Iterator;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.AggregateCall;
import org.apache.calcite.rel.hint.HintStrategyTable;
import org.apache.calcite.rel.hint.Hintable;
import org.apache.calcite.rel.hint.RelHint;
import org.apache.calcite.rel.logical.LogicalAggregate;
import org.apache.ignite.internal.processors.query.calcite.util.Commons;
import org.apache.ignite.internal.util.typedef.F;

import static org.apache.ignite.internal.processors.query.calcite.hint.HintDefinition.EXPAND_DISTINCT_AGG;

/**
 * Base class for working with Calcite's SQL hints.
 */
public final class HintUtils {
    /** */
    private HintUtils() {
        // No-op.
    }

    /**
     * @return Combined list options of all {@code hints} filtered with {@code hintDef}.
     * @see #filterHints(RelNode, Collection, List)
     */
    public static Collection<String> options(RelNode rel, Collection<RelHint> hints, HintDefinition hintDef) {
        return F.flatCollections(filterHints(rel, hints, Collections.singletonList(hintDef)).stream()
            .map(h -> h.listOptions).collect(Collectors.toList()));
    }

    /**
     * @return Hints filtered with {@code hintDefs} and suitable for {@code rel}.
     * @see HintStrategyTable#apply(List, RelNode)
     * @see #filterHints(RelNode, Collection, List)
     */
    public static List<RelHint> hints(RelNode rel, HintDefinition... hintDefs) {
        return rel.getCluster().getHintStrategies().apply(filterHints(rel, allRelHints(rel), Arrays.asList(hintDefs)),
            rel);
    }

    /**
     * @return {@code True} if the query has a suitable hint for {@code rel} defined by {@code hintDefs}.
     * {@code False} otherwise.
     * @see HintStrategyTable#apply(List, RelNode)
     */
    public static boolean hasHint(RelNode rel, HintDefinition... hintDefs) {
        return !hints(rel, hintDefs).isEmpty();
    }

    /**
     * @return Hints of {@code rel} if it is a {@code Hintable}. If is not or has no hints, empty collection.
     * @see Hintable#getHints()
     */
    public static List<RelHint> allRelHints(RelNode rel) {
        return rel instanceof Hintable ? ((Hintable)rel).getHints() : Collections.emptyList();
    }

    /**
     * @return Distinct hints within {@code hints} filtered with {@code hintDefs}, {@link HintOptionsChecker} and removed inherit pathes.
     * @see HintOptionsChecker
     * @see RelHint#inheritPath
     */
    private static List<RelHint> filterHints(RelNode rel, Collection<RelHint> hints, List<HintDefinition> hintDefs) {
        Set<String> hintNames = hintDefs.stream().map(Enum::name).collect(Collectors.toSet());

        List<RelHint> res = hints.stream().filter(h -> hintNames.contains(h.hintName))
            .map(h -> RelHint.builder(h.hintName).hintOptions(h.listOptions).build()).distinct()
            .collect(Collectors.toList());

        // Validate hint options.
        Iterator<RelHint> it = res.iterator();

        while (it.hasNext()) {
            RelHint hint = it.next();

            String optsErr = HintDefinition.valueOf(hint.hintName).optionsChecker().apply(hint);

            if (!F.isEmpty(optsErr)) {
                Commons.planContext(rel).skippedHint(rel, hint, optsErr);

                it.remove();
            }
        }

        return res;
    }

    /**
     * @return {@code True} if {@code rel} is hinted with {@link HintDefinition#EXPAND_DISTINCT_AGG}.
     * {@code False} otherwise.
     */
    public static boolean isExpandedDistinct(LogicalAggregate rel) {
        return hasHint(rel, EXPAND_DISTINCT_AGG) && rel.getAggCallList().stream().anyMatch(AggregateCall::isDistinct);
    }
}
