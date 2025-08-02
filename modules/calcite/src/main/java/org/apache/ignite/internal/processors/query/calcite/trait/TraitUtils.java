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

package org.apache.ignite.internal.processors.query.calcite.trait;

import java.math.BigDecimal;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import org.apache.calcite.linq4j.Ord;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelOptPlanner;
import org.apache.calcite.plan.RelOptRule;
import org.apache.calcite.plan.RelOptTable;
import org.apache.calcite.plan.RelTrait;
import org.apache.calcite.plan.RelTraitDef;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.RelCollation;
import org.apache.calcite.rel.RelCollationTraitDef;
import org.apache.calcite.rel.RelCollations;
import org.apache.calcite.rel.RelDistribution;
import org.apache.calcite.rel.RelFieldCollation;
import org.apache.calcite.rel.RelInput;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.AggregateCall;
import org.apache.calcite.rel.core.Spool;
import org.apache.calcite.rel.core.Window;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rex.RexInputRef;
import org.apache.calcite.rex.RexLiteral;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.util.ControlFlowException;
import org.apache.calcite.util.ImmutableBitSet;
import org.apache.calcite.util.Pair;
import org.apache.calcite.util.mapping.Mappings;
import org.apache.ignite.internal.processors.query.calcite.externalize.RelInputEx;
import org.apache.ignite.internal.processors.query.calcite.prepare.bounds.SearchBounds;
import org.apache.ignite.internal.processors.query.calcite.rel.IgniteConvention;
import org.apache.ignite.internal.processors.query.calcite.rel.IgniteExchange;
import org.apache.ignite.internal.processors.query.calcite.rel.IgniteRel;
import org.apache.ignite.internal.processors.query.calcite.rel.IgniteSort;
import org.apache.ignite.internal.processors.query.calcite.rel.IgniteTableSpool;
import org.apache.ignite.internal.processors.query.calcite.rel.IgniteTrimExchange;
import org.apache.ignite.internal.util.typedef.F;
import org.jetbrains.annotations.Nullable;

import static java.util.Collections.emptyList;
import static java.util.Collections.singletonList;
import static org.apache.calcite.plan.RelOptUtil.permutationPushDownProject;
import static org.apache.calcite.rel.RelDistribution.Type.BROADCAST_DISTRIBUTED;
import static org.apache.calcite.rel.RelDistribution.Type.HASH_DISTRIBUTED;
import static org.apache.ignite.internal.processors.query.calcite.trait.IgniteDistributions.any;
import static org.apache.ignite.internal.processors.query.calcite.trait.IgniteDistributions.single;

/**
 *
 */
public class TraitUtils {
    /** */
    @Nullable public static RelNode enforce(RelNode rel, RelTraitSet toTraits) {
        RelOptPlanner planner = rel.getCluster().getPlanner();
        RelTraitSet fromTraits = rel.getTraitSet();
        int size = Math.min(fromTraits.size(), toTraits.size());

        if (!fromTraits.satisfies(toTraits)) {
            RelNode old = null;

            for (int i = 0; rel != null && i < size; i++) {
                RelTrait fromTrait = rel.getTraitSet().getTrait(i);
                RelTrait toTrait = toTraits.getTrait(i);

                if (fromTrait.satisfies(toTrait))
                    continue;

                if (old != null && old != rel)
                    rel = planner.register(rel, old);

                old = rel;

                rel = convertTrait(planner, fromTrait, toTrait, rel);

                assert rel == null || rel.getTraitSet().getTrait(i).satisfies(toTrait);
            }

            assert rel == null || rel.getTraitSet().satisfies(toTraits);
        }

        return rel;
    }

    /** */
    @SuppressWarnings({"rawtypes"})
    @Nullable private static RelNode convertTrait(RelOptPlanner planner, RelTrait fromTrait, RelTrait toTrait, RelNode rel) {
        assert fromTrait.getTraitDef() == toTrait.getTraitDef();

        RelTraitDef converter = fromTrait.getTraitDef();

        if (converter == RelCollationTraitDef.INSTANCE)
            return convertCollation(planner, (RelCollation)toTrait, rel);
        else if (converter == DistributionTraitDef.INSTANCE)
            return convertDistribution(planner, (IgniteDistribution)toTrait, rel);
        else if (converter == RewindabilityTraitDef.INSTANCE)
            return convertRewindability(planner, (RewindabilityTrait)toTrait, rel);
        else
            return convertOther(planner, converter, toTrait, rel);
    }

    /** */
    @Nullable public static RelNode convertCollation(RelOptPlanner planner,
        RelCollation toTrait, RelNode rel) {
        RelCollation fromTrait = collation(rel);

        if (fromTrait.satisfies(toTrait))
            return rel;

        RelTraitSet traits = rel.getTraitSet().replace(toTrait);

        return new IgniteSort(rel.getCluster(), traits, rel, toTrait, true);
    }

    /** */
    @Nullable public static RelNode convertDistribution(RelOptPlanner planner,
        IgniteDistribution toTrait, RelNode rel) {
        IgniteDistribution fromTrait = distribution(rel);

        if (fromTrait.satisfies(toTrait))
            return rel;

        // Cannot enforce node to correlated distribution, this distribution is only set by trait propagation.
        if (toTrait.function().correlated())
            return null;

        // right now we cannot create a multi-column affinity
        // key object, thus this conversion is impossible
        if (toTrait.function().affinity() && toTrait.getKeys().size() > 1)
            return null;

        RelTraitSet traits = rel.getTraitSet().replace(toTrait);
        if (fromTrait.getType() == BROADCAST_DISTRIBUTED && toTrait.getType() == HASH_DISTRIBUTED)
            return new IgniteTrimExchange(rel.getCluster(), traits, rel, toTrait);
        else {
            return new IgniteExchange(
                rel.getCluster(),
                traits
                    .replace(RewindabilityTrait.ONE_WAY)
                    .replace(CorrelationTrait.UNCORRELATED),
                RelOptRule.convert(
                    rel,
                    rel.getTraitSet()
                        .replace(CorrelationTrait.UNCORRELATED)
                ),
                toTrait);
        }
    }

    /** */
    @Nullable public static RelNode convertRewindability(RelOptPlanner planner,
        RewindabilityTrait toTrait, RelNode rel) {
        RewindabilityTrait fromTrait = rewindability(rel);

        if (fromTrait.satisfies(toTrait))
            return rel;

        RelTraitSet traits = rel.getTraitSet()
            .replace(toTrait)
            .replace(CorrelationTrait.UNCORRELATED);

        return new IgniteTableSpool(
            rel.getCluster(),
            traits,
            Spool.Type.LAZY,
            RelOptRule.convert(rel, rel.getTraitSet().replace(CorrelationTrait.UNCORRELATED))
        );
    }

    /** */
    @SuppressWarnings({"unchecked", "rawtypes"})
    @Nullable private static RelNode convertOther(RelOptPlanner planner, RelTraitDef converter,
        RelTrait toTrait, RelNode rel) {
        RelTrait fromTrait = rel.getTraitSet().getTrait(converter);

        if (fromTrait.satisfies(toTrait))
            return rel;

        if (!converter.canConvert(planner, fromTrait, toTrait))
            return null;

        return converter.convert(planner, rel, toTrait, true);
    }

    /** Change distribution and Convention. */
    public static RelTraitSet fixTraits(RelTraitSet traits) {
        if (distribution(traits) == any())
            traits = traits.replace(single());

        return traits.replace(IgniteConvention.INSTANCE);
    }

    /** */
    public static IgniteDistribution distribution(RelNode rel) {
        return rel instanceof IgniteRel
            ? ((IgniteRel)rel).distribution()
            : distribution(rel.getTraitSet());
    }

    /** */
    public static IgniteDistribution distribution(RelTraitSet traits) {
        return traits.getTrait(DistributionTraitDef.INSTANCE);
    }

    /** */
    public static RelCollation collation(RelNode rel) {
        return rel instanceof IgniteRel
            ? ((IgniteRel)rel).collation()
            : collation(rel.getTraitSet());
    }

    /** */
    public static RelCollation collation(RelTraitSet traits) {
        return traits.getTrait(RelCollationTraitDef.INSTANCE);
    }

    /** */
    public static RewindabilityTrait rewindability(RelNode rel) {
        return rel instanceof IgniteRel
            ? ((IgniteRel)rel).rewindability()
            : rewindability(rel.getTraitSet());
    }

    /** */
    public static RewindabilityTrait rewindability(RelTraitSet traits) {
        return traits.getTrait(RewindabilityTraitDef.INSTANCE);
    }

    /** */
    public static CorrelationTrait correlation(RelNode rel) {
        return rel instanceof IgniteRel
            ? ((IgniteRel)rel).correlation()
            : correlation(rel.getTraitSet());
    }

    /** */
    public static CorrelationTrait correlation(RelTraitSet traits) {
        return traits.getTrait(CorrelationTraitDef.INSTANCE);
    }

    /** */
    public static RelInput changeTraits(RelInput input, RelTrait... traits) {
        RelTraitSet traitSet = input.getTraitSet();

        for (RelTrait trait : traits)
            traitSet = traitSet.replace(trait);

        RelTraitSet traitSet0 = traitSet;

        return new RelInputEx() {
            @Override public RelOptCluster getCluster() {
                return input.getCluster();
            }

            @Override public RelTraitSet getTraitSet() {
                return traitSet0;
            }

            @Override public RelOptTable getTable(String table) {
                return input.getTable(table);
            }

            @Override public RelNode getInput() {
                return input.getInput();
            }

            @Override public List<RelNode> getInputs() {
                return input.getInputs();
            }

            @Override public RexNode getExpression(String tag) {
                return input.getExpression(tag);
            }

            @Override public ImmutableBitSet getBitSet(String tag) {
                return input.getBitSet(tag);
            }

            @Override public List<ImmutableBitSet> getBitSetList(String tag) {
                return input.getBitSetList(tag);
            }

            @Override public List<AggregateCall> getAggregateCalls(String tag) {
                return input.getAggregateCalls(tag);
            }

            @Override public Object get(String tag) {
                return input.get(tag);
            }

            @Override public String getString(String tag) {
                return input.getString(tag);
            }

            @Override public float getFloat(String tag) {
                return input.getFloat(tag);
            }

            @Override public BigDecimal getBigDecimal(String tag) {
                return input.getBigDecimal(tag);
            }

            @Override public <E extends Enum<E>> E getEnum(String tag, Class<E> enumClass) {
                return input.getEnum(tag, enumClass);
            }

            @Override public List<RexNode> getExpressionList(String tag) {
                return input.getExpressionList(tag);
            }

            @Override public List<String> getStringList(String tag) {
                return input.getStringList(tag);
            }

            @Override public List<Integer> getIntegerList(String tag) {
                return input.getIntegerList(tag);
            }

            @Override public List<List<Integer>> getIntegerListList(String tag) {
                return input.getIntegerListList(tag);
            }

            @Override public RelDataType getRowType(String tag) {
                return input.getRowType(tag);
            }

            @Override public RelDataType getRowType(String expressionsTag, String fieldsTag) {
                return input.getRowType(expressionsTag, fieldsTag);
            }

            @Override public RelCollation getCollation() {
                return input.getCollation();
            }

            @Override public RelDistribution getDistribution() {
                return input.getDistribution();
            }

            @Override public ImmutableList<ImmutableList<RexLiteral>> getTuples(
                String tag) {
                return input.getTuples(tag);
            }

            @Override public boolean getBoolean(String tag, boolean default_) {
                return input.getBoolean(tag, default_);
            }

            @Override public RelCollation getCollation(String tag) {
                return ((RelInputEx)input).getCollation(tag);
            }

            @Override public List<SearchBounds> getSearchBounds(String tag) {
                return ((RelInputEx)input).getSearchBounds(tag);
            }

            @Override public Window.Group getWindowGroup(String tag) {
                return ((RelInputEx)input).getWindowGroup(tag);
            }
        };
    }

    /** */
    public static RelCollation projectCollation(RelCollation collation, List<RexNode> projects, RelDataType inputRowType) {
        if (collation.getFieldCollations().isEmpty())
            return RelCollations.EMPTY;

        Mappings.TargetMapping mapping = permutationPushDownProject(projects, inputRowType, 0, 0);

        return collation.apply(mapping);
    }

    /** */
    public static IgniteDistribution projectDistribution(IgniteDistribution distribution, List<RexNode> projects,
        RelDataType inputRowType) {
        if (distribution.getType() != HASH_DISTRIBUTED)
            return distribution;

        Mappings.TargetMapping mapping = createProjectionMapping(inputRowType.getFieldCount(), projects);

        return distribution.apply(mapping);
    }

    /** */
    public static Pair<RelTraitSet, List<RelTraitSet>> passThrough(TraitsAwareIgniteRel rel, RelTraitSet requiredTraits) {
        if (requiredTraits.getConvention() != IgniteConvention.INSTANCE || rel.getInputs().isEmpty())
            return null;

        List<RelTraitSet> inTraits = Collections.nCopies(rel.getInputs().size(),
            rel.getCluster().traitSetOf(IgniteConvention.INSTANCE));

        List<Pair<RelTraitSet, List<RelTraitSet>>> traits = new PropagationContext(ImmutableSet.of(Pair.of(requiredTraits, inTraits)))
            .propagate((in, outs) -> singletonListFromNullable(rel.passThroughCollation(in, outs)))
            .propagate((in, outs) -> singletonListFromNullable(rel.passThroughDistribution(in, outs)))
            .propagate((in, outs) -> singletonListFromNullable(rel.passThroughRewindability(in, outs)))
            .propagate((in, outs) -> singletonListFromNullable(rel.passThroughCorrelation(in, outs)))
            .combinations();

        assert traits.size() <= 1;

        return F.first(traits);
    }

    /** */
    public static List<RelNode> derive(TraitsAwareIgniteRel rel, List<List<RelTraitSet>> inTraits) {
        assert !F.isEmpty(inTraits);

        RelTraitSet outTraits = rel.getCluster().traitSetOf(IgniteConvention.INSTANCE);
        Set<Pair<RelTraitSet, List<RelTraitSet>>> combinations = combinations(outTraits, inTraits);

        if (combinations.isEmpty())
            return ImmutableList.of();

        return new PropagationContext(combinations)
            .propagate(rel::deriveCollation)
            .propagate(rel::deriveDistribution)
            .propagate(rel::deriveRewindability)
            .propagate(rel::deriveCorrelation)
            .nodes(rel::createNode);
    }

    /**
     * @param elem Elem.
     */
    private static <T> List<T> singletonListFromNullable(@Nullable T elem) {
        return elem == null ? emptyList() : singletonList(elem);
    }

    /** */
    private static Set<Pair<RelTraitSet, List<RelTraitSet>>> combinations(RelTraitSet outTraits, List<List<RelTraitSet>> inTraits) {
        Set<Pair<RelTraitSet, List<RelTraitSet>>> out = new HashSet<>();
        fillRecursive(outTraits, inTraits, out, new RelTraitSet[inTraits.size()], 0);
        return out;
    }

    /** */
    private static boolean fillRecursive(
        RelTraitSet outTraits,
        List<List<RelTraitSet>> inTraits,
        Set<Pair<RelTraitSet, List<RelTraitSet>>> result,
        RelTraitSet[] combination,
        int idx
    ) throws ControlFlowException {
        boolean processed = false, last = idx == inTraits.size() - 1;
        for (RelTraitSet t : inTraits.get(idx)) {
            assert t.getConvention() == IgniteConvention.INSTANCE;

            processed = true;
            combination[idx] = t;

            if (last)
                result.add(Pair.of(outTraits, ImmutableList.copyOf(combination)));
            else if (!fillRecursive(outTraits, inTraits, result, combination, idx + 1))
                return false;
        }
        return processed;
    }

    /**
     * Creates collations from provided keys.
     *
     * @param keys The keys to create collation from.
     * @return New collation.
     */
    public static RelCollation createCollation(Collection<Integer> keys) {
        return RelCollations.of(
            keys.stream().map(TraitUtils::createFieldCollation).collect(Collectors.toList())
        );
    }

    /**
     * Creates field collation with default direction and nulls ordering.
     */
    public static RelFieldCollation createFieldCollation(int fieldIdx) {
        return createFieldCollation(fieldIdx, true);
    }

    /**
     * Creates field collation with default nulls ordering.
     */
    public static RelFieldCollation createFieldCollation(int fieldIdx, boolean asc) {
        return asc
            ? new RelFieldCollation(fieldIdx, RelFieldCollation.Direction.ASCENDING, RelFieldCollation.NullDirection.FIRST)
            : new RelFieldCollation(fieldIdx, RelFieldCollation.Direction.DESCENDING, RelFieldCollation.NullDirection.LAST);
    }

    /**
     * Creates mapping from provided projects that maps a source column idx
     * to idx in a row after applying projections.
     *
     * @param inputFieldCount Size of a source row.
     * @param projects Projections.
     */
    private static Mappings.TargetMapping createProjectionMapping(int inputFieldCount, List<? extends RexNode> projects) {
        Map<Integer, Integer> src2target = new HashMap<>();

        for (Ord<RexNode> exp : Ord.<RexNode>zip(projects)) {
            if (exp.e instanceof RexInputRef)
                src2target.putIfAbsent(((RexInputRef)exp.e).getIndex(), exp.i);
        }

        return Mappings.target(src -> src2target.getOrDefault(src, -1), inputFieldCount, projects.size());
    }

    /** */
    private static class PropagationContext {
        /** */
        private final Set<Pair<RelTraitSet, List<RelTraitSet>>> combinations;

        /** */
        private PropagationContext(Set<Pair<RelTraitSet, List<RelTraitSet>>> combinations) {
            this.combinations = combinations;
        }

        /**
         * Propagates traits in bottom-up or up-to-bottom manner using given traits propagator.
         */
        public PropagationContext propagate(TraitsPropagator processor) {
            if (combinations.isEmpty())
                return this;

            ImmutableSet.Builder<Pair<RelTraitSet, List<RelTraitSet>>> b = ImmutableSet.builder();
            for (Pair<RelTraitSet, List<RelTraitSet>> variant : combinations)
                b.addAll(processor.propagate(variant.left, variant.right));
            return new PropagationContext(b.build());
        }

        /**
         * Creates nodes using given factory.
         */
        public List<RelNode> nodes(RelFactory nodesCreator) {
            if (combinations.isEmpty())
                return ImmutableList.of();

            ImmutableList.Builder<RelNode> b = ImmutableList.builder();
            for (Pair<RelTraitSet, List<RelTraitSet>> variant : combinations)
                b.add(nodesCreator.create(variant.left, variant.right));
            return b.build();
        }

        /** */
        public List<Pair<RelTraitSet, List<RelTraitSet>>> combinations() {
            return ImmutableList.copyOf(combinations);
        }
    }

    /** */
    private interface TraitsPropagator {
        /**
         * Propagates traits in bottom-up or up-to-bottom manner.
         *
         * @param outTraits Relational node traits.
         * @param inTraits Relational node input traits.
         * @return List of possible input-output traits combinations.
         */
        List<Pair<RelTraitSet, List<RelTraitSet>>> propagate(RelTraitSet outTraits, List<RelTraitSet> inTraits);
    }
}
