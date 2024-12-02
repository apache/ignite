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
import org.apache.calcite.plan.RelOptUtil;
import org.apache.calcite.rel.AbstractRelNode;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.RelWriter;
import org.apache.calcite.rel.core.AggregateCall;
import org.apache.calcite.rel.hint.HintStrategyTable;
import org.apache.calcite.rel.hint.Hintable;
import org.apache.calcite.rel.hint.RelHint;
import org.apache.calcite.rel.logical.LogicalAggregate;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.sql.SqlExplainLevel;
import org.apache.ignite.IgniteLogger;
import org.apache.ignite.internal.processors.query.calcite.prepare.BaseQueryContext;
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
     * @return Hints of {@code rel} filtered with {@code hintDefs}.
     * @see HintStrategyTable#apply(List, RelNode)
     * @see #filterHints(RelNode, Collection, List)
     */
    public static List<RelHint> hints(RelNode rel, HintDefinition... hintDefs) {
        return hints(rel, allRelHints(rel), hintDefs);
    }

    /**
     * @return Hints filtered with {@code hintDefs} and suitable for {@code rel}.
     * @see HintStrategyTable#apply(List, RelNode)
     * @see #filterHints(RelNode, Collection, List)
     */
    public static List<RelHint> hints(RelNode rel, Collection<RelHint> hints, HintDefinition... hintDefs) {
        return rel.getCluster().getHintStrategies().apply(filterHints(rel, hints, Arrays.asList(hintDefs)), rel);
    }

    /**
     * @return Hints of {@code rel} if it is a {@code Hintable}. If is not or has no hints, empty collection.
     * @see Hintable#getHints()
     */
    public static List<RelHint> allRelHints(RelNode rel) {
        return rel instanceof Hintable ? ((Hintable)rel).getHints() : Collections.emptyList();
    }

    /**
     * @return Only noninherited hints of {@code rel} if it is a {@code Hintable}. If is not or has no hints, empty
     * collection.
     * @see Hintable#getHints()
     */
    public static List<RelHint> nonInheritedRelHints(RelNode rel) {
        return rel instanceof Hintable
            ? ((Hintable)rel).getHints().stream().filter(hint -> hint.inheritPath.isEmpty()).collect(Collectors.toList())
            : Collections.emptyList();
    }

    /**
     * @return Distinct hints within {@code hints} filtered with {@code hintDefs}, {@link HintOptionsChecker} and
     * removed inherit pathes.
     * @see HintOptionsChecker
     * @see RelHint#inheritPath
     */
    private static List<RelHint> filterHints(RelNode rel, Collection<RelHint> hints, List<HintDefinition> hintDefs) {
        Set<String> requiredHintDefs = hintDefs.stream().map(Enum::name).collect(Collectors.toSet());

        List<RelHint> res = hints.stream().filter(h -> requiredHintDefs.contains(h.hintName))
            .map(h -> {
                RelHint.Builder rb = RelHint.builder(h.hintName);

                if (!h.listOptions.isEmpty())
                    rb.hintOptions(h.listOptions);
                else if (!h.kvOptions.isEmpty())
                    rb.hintOptions(h.kvOptions);

                return rb.build();
            }).distinct().collect(Collectors.toList());

        // Validate hint options.
        Iterator<RelHint> it = res.iterator();

        while (it.hasNext()) {
            RelHint hint = it.next();

            String optsErr = HintDefinition.valueOf(hint.hintName).optionsChecker().apply(hint);

            if (!F.isEmpty(optsErr)) {
                skippedHint(rel, hint, optsErr);

                it.remove();
            }
        }

        return res;
    }

    /**
     * @return {@code True} if {@code rel} is hinted with {@link HintDefinition#EXPAND_DISTINCT_AGG}.
     * {@code False} otherwise.
     */
    public static boolean isExpandDistinctAggregate(LogicalAggregate rel) {
        return !hints(rel, EXPAND_DISTINCT_AGG).isEmpty()
            && rel.getAggCallList().stream().anyMatch(AggregateCall::isDistinct);
    }

    /**
     * Logs skipped hint.
     */
    public static void skippedHint(RelNode relNode, RelHint hint, String reason) {
        IgniteLogger log = Commons.context(relNode).unwrap(BaseQueryContext.class).logger();

        if (log.isDebugEnabled()) {
            String hintOptions = hint.listOptions.isEmpty() ? "" : "with options "
                + hint.listOptions.stream().map(o -> '\'' + o + '\'').collect(Collectors.joining(","))
                + ' ';

            if (!relNode.getInputs().isEmpty())
                relNode = noInputsRelWrap(relNode);

            log.debug(String.format("Skipped hint '%s' %sfor relation operator '%s'. %s", hint.hintName,
                hintOptions, RelOptUtil.toString(relNode, SqlExplainLevel.EXPPLAN_ATTRIBUTES).trim(), reason));
        }
    }

    /**
     * @return A RelNode witout any inputs. For logging purposes.
     */
    public static RelNode noInputsRelWrap(RelNode rel) {
        return new NoInputsRelNodeWrap(rel);
    }

    /** */
    private static final class NoInputsRelNodeWrap extends AbstractRelNode {
        /** Original rel. */
        private final RelNode rel;

        /** Ctor. */
        private NoInputsRelNodeWrap(RelNode relNode) {
            super(relNode.getCluster(), relNode.getTraitSet());

            this.rel = relNode;
        }

        /** {@inheritDoc} */
        @Override public List<RelNode> getInputs() {
            return Collections.emptyList();
        }

        /** {@inheritDoc} */
        @Override public RelNode getInput(int i) {
            throw new UnsupportedOperationException("Failed to pass any node input. This a no-inputs node.");
        }

        /** {@inheritDoc} */
        @Override protected RelDataType deriveRowType() {
            return rel.getRowType();
        }

        /** {@inheritDoc} */
        @Override public void explain(RelWriter pw) {
            rel.explain(pw);
        }
    }
}
