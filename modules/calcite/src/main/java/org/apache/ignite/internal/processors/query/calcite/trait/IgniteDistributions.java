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

import com.google.common.collect.ImmutableList;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import org.apache.calcite.plan.RelTraitDef;
import org.apache.calcite.rel.RelDistribution;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.Aggregate;
import org.apache.calcite.rel.core.AggregateCall;
import org.apache.calcite.rel.core.JoinInfo;
import org.apache.calcite.rel.core.JoinRelType;
import org.apache.calcite.rel.core.Project;
import org.apache.calcite.rel.metadata.RelMetadataQuery;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rex.RexLiteral;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.util.ImmutableBitSet;
import org.apache.calcite.util.ImmutableIntList;
import org.apache.calcite.util.mapping.Mappings;
import org.apache.ignite.IgniteSystemProperties;
import org.apache.ignite.internal.processors.query.calcite.metadata.IgniteMdDerivedDistribution;
import org.apache.ignite.internal.processors.query.calcite.metadata.IgniteMdDistribution;
import org.apache.ignite.internal.processors.query.calcite.rel.IgniteAggregate;
import org.apache.ignite.internal.processors.query.calcite.trait.DistributionFunction.AffinityDistribution;
import org.apache.ignite.internal.processors.query.calcite.trait.DistributionFunction.AnyDistribution;
import org.apache.ignite.internal.processors.query.calcite.trait.DistributionFunction.BroadcastDistribution;
import org.apache.ignite.internal.processors.query.calcite.trait.DistributionFunction.HashDistribution;
import org.apache.ignite.internal.processors.query.calcite.trait.DistributionFunction.RandomDistribution;
import org.apache.ignite.internal.processors.query.calcite.trait.DistributionFunction.SingletonDistribution;
import org.apache.ignite.internal.processors.query.calcite.util.Commons;
import org.apache.ignite.internal.util.typedef.F;
import org.apache.ignite.internal.util.typedef.internal.CU;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.jetbrains.annotations.NotNull;

import static org.apache.calcite.rel.core.JoinRelType.INNER;
import static org.apache.calcite.rel.core.JoinRelType.LEFT;
import static org.apache.calcite.rel.core.JoinRelType.RIGHT;

/**
 *
 */
public class IgniteDistributions {
    /** */
    private static final int BEST_CNT = IgniteSystemProperties.getInteger("IGNITE_CALCITE_JOIN_SUGGESTS_COUNT", 0);

    /** */
    private static final Integer[] INTS = new Integer[]{0, 1, 2};

    /** */
    private static final IgniteDistribution BROADCAST = canonize(new DistributionTrait(BroadcastDistribution.INSTANCE));

    /** */
    private static final IgniteDistribution SINGLETON = canonize(new DistributionTrait(SingletonDistribution.INSTANCE));

    /** */
    private static final IgniteDistribution RANDOM = canonize(new DistributionTrait(RandomDistribution.INSTANCE));

    /** */
    private static final IgniteDistribution ANY = canonize(new DistributionTrait(AnyDistribution.INSTANCE));

    /**
     * @return Any distribution.
     */
    public static IgniteDistribution any() {
        return ANY;
    }

    /**
     * @return Random distribution.
     */
    public static IgniteDistribution random() {
        return RANDOM;
    }

    /**
     * @return Single distribution.
     */
    public static IgniteDistribution single() {
        return SINGLETON;
    }

    /**
     * @return Broadcast distribution.
     */
    public static IgniteDistribution broadcast() {
        return BROADCAST;
    }

    /**
     * @param key Affinity key.
     * @param cacheName Affinity cache name.
     * @param identity Affinity identity key.
     * @return Affinity distribution.
     */
    public static IgniteDistribution affinity(int key, String cacheName, Object identity) {
        return affinity(key, CU.cacheId(cacheName), identity);
    }

    /**
     * @param key Affinity key.
     * @param cacheId Affinity cache ID.
     * @param identity Affinity identity key.
     * @return Affinity distribution.
     */
    public static IgniteDistribution affinity(int key, int cacheId, Object identity) {
        return hash(ImmutableIntList.of(key), new AffinityDistribution(cacheId, identity));
    }

    /**
     * @param keys Distribution keys.
     * @return Hash distribution.
     */
    public static IgniteDistribution hash(List<Integer> keys) {
        return canonize(new DistributionTrait(ImmutableIntList.copyOf(keys), HashDistribution.INSTANCE));
    }

    /**
     * @param keys Distribution keys.
     * @param function Specific hash function.
     * @return Hash distribution.
     */
    public static IgniteDistribution hash(List<Integer> keys, DistributionFunction function) {
        return canonize(new DistributionTrait(ImmutableIntList.copyOf(keys), function));
    }

    /**
     * Suggests possible aggregate distributions.
     *
     * @param inDistr Input node distribution.
     * @param groupSet Aggregate group set.
     * @param groupSets Aggregate group sets.
     * @return Array of possible distributions, sorted by their efficiency (cheaper first).
     */
    public static List<Suggestion> suggestAggregate(IgniteDistribution inDistr, ImmutableBitSet groupSet,
        List<ImmutableBitSet> groupSets) {
        return sorted(suggestAggregate0(new HashMap<>(), inDistr, groupSet, groupSets));
    }

    /**
     * Suggests possible aggregate distributions.
     *
     * @param mq Metadata query.
     * @param input Input node.
     * @param groupSet Aggregate group set.
     * @param groupSets Aggregate group sets.
     * @return Array of possible distributions, sorted by their efficiency (cheaper first).
     */
    public static List<Suggestion> suggestAggregate(RelMetadataQuery mq, RelNode input, ImmutableBitSet groupSet,
        List<ImmutableBitSet> groupSets) {
        List<IgniteDistribution> inDistrs = IgniteMdDerivedDistribution._deriveDistributions(input, mq);

        Map<Suggestion, Integer> suggestions = new HashMap<>();

        for (IgniteDistribution inDistr : inDistrs)
            suggestions = suggestAggregate0(suggestions, inDistr, groupSet, groupSets);

        return sorted(suggestions);
    }

    /**
     * Suggests possible aggregate distributions based on next principles:
     * <li>Simple aggregate may be done in a distributed form in case its input is hash distributed by aggregate group keys</li>
     * <li>Any aggregate may be done without map-reduce processing in case its input is singleton or broadcast distributed</li>
     * <li>Any aggregate may be done in map-reduce style in case its input is hash or random distributed</li>
     */
    private static Map<Suggestion,Integer> suggestAggregate0(Map<Suggestion, Integer> dst, IgniteDistribution inDistr,
        ImmutableBitSet groupSet, List<ImmutableBitSet> groupSets) {

        IgniteDistribution out, in;

        if (simpleAggregate(groupSet, groupSets)) {
            // re-hash by group keys
            in = hash(groupSet.asList());
            out = hash(ImmutableIntList.range(0, groupSet.cardinality()));
            dst = add(dst, out, inDistr, in);
        }

        if (inDistr.satisfies(random()))
            // map-reduce case
            dst = add(dst, new Suggestion(single(), inDistr), BEST_CNT > 0 ? INTS[1] : INTS[0]);

        out = broadcast(); in = broadcast();
        dst = add(dst, out, inDistr, in);

        out = single(); in = single();
        dst = add(dst, out, inDistr, in);

        return dst;
    }

    /**
     * Suggests possible join distributions.
     *
     * @param mq Metadata query.
     * @param left Left node.
     * @param right Right node.
     * @param joinInfo Join info.
     * @param joinType Join type.
     * @return Array of possible distributions, sorted by their efficiency (cheaper first).
     */
    public static List<BiSuggestion> suggestJoin(RelMetadataQuery mq, RelNode left, RelNode right, JoinInfo joinInfo,
        JoinRelType joinType) {
        List<IgniteDistribution> leftIn = IgniteMdDerivedDistribution._deriveDistributions(left, mq);
        List<IgniteDistribution> rightIn = IgniteMdDerivedDistribution._deriveDistributions(right, mq);

        Map<BiSuggestion, Integer> suggestions = new HashMap<>();

        for (IgniteDistribution leftIn0 : leftIn)
            for (IgniteDistribution rightIn0 : rightIn)
                suggestions = suggestJoin0(suggestions, leftIn0, rightIn0, joinInfo, joinType);

        return sorted(suggestions);
    }

    /**
     * Suggests possible join distributions.
     *
     * @param leftIn Left distribution.
     * @param rightIn Right distribution.
     * @param joinInfo Join info.
     * @param joinType Join type.
     * @return Array of possible distributions, sorted by their efficiency (cheaper first).
     */
    public static List<BiSuggestion> suggestJoin(IgniteDistribution leftIn, IgniteDistribution rightIn,
        JoinInfo joinInfo, JoinRelType joinType) {
        return sorted(suggestJoin0(new HashMap<>(), leftIn, rightIn, joinInfo, joinType));
    }

    /**
     * Suggests possible join distributions using inputs distributions and join info
     * based on next distributions table:
     * <p/><table>
     * <tr><th>===============INNER JOIN==============</th></tr>
     * <tr><td>hash + hash = hash</td></tr>
     * <tr><td>broadcast + hash = hash</td></tr>
     * <tr><td>hash + broadcast = hash</td></tr>
     * <tr><td>broadcast + broadcast = broadcast</td></tr>
     * <tr><td>single + single = single</td></tr>
     * <tr><th>===============LEFT JOIN===============</th></tr>
     * <tr><td>hash + hash = hash</td></tr>
     * <tr><td>hash + broadcast = hash</td></tr>
     * <tr><td>broadcast + broadcast = broadcast</td></tr>
     * <tr><td>single + single = single</td></tr>
     * <tr><th>===============RIGHT JOIN==============</th></tr>
     * <tr><td>hash + hash = hash</td></tr>
     * <tr><td>broadcast + hash = hash</td></tr>
     * <tr><td>broadcast + broadcast = broadcast</td></tr>
     * <tr><td>single + single = single</td></tr>
     * <tr><th>===========FULL JOIN/CROSS JOIN========</th></tr>
     * <tr><td>broadcast + broadcast = broadcast</td></tr>
     * <tr><td>single + single = single</td></tr>
     * </table>
     * <p/>others require redistribution.
     */
    private static Map<BiSuggestion, Integer> suggestJoin0(Map<BiSuggestion, Integer> dst, IgniteDistribution leftIn,
        IgniteDistribution rightIn, JoinInfo joinInfo, JoinRelType joinType) {
        IgniteDistribution out, left, right;

        if (joinType == LEFT || joinType == RIGHT || (joinType == INNER && !F.isEmpty(joinInfo.pairs()))) {
            HashSet<DistributionFunction> factories = U.newHashSet(3);

            if (leftIn.getType() == RelDistribution.Type.HASH_DISTRIBUTED
                && Objects.equals(joinInfo.leftKeys, leftIn.getKeys()))
                factories.add(leftIn.function());

            if (rightIn.getType() == RelDistribution.Type.HASH_DISTRIBUTED
                && Objects.equals(joinInfo.rightKeys, rightIn.getKeys()))
                factories.add(rightIn.function());

            factories.add(HashDistribution.INSTANCE);

            for (DistributionFunction factory : factories) {
                out = hash(joinInfo.leftKeys, factory);

                left = hash(joinInfo.leftKeys, factory); right = hash(joinInfo.rightKeys, factory);
                dst = add(dst, out, leftIn, rightIn, left, right);

                if (joinType == INNER || joinType == LEFT) {
                    left = hash(joinInfo.leftKeys, factory); right = broadcast();
                    dst = add(dst, out, leftIn, rightIn, left, right);
                }

                if (joinType == INNER || joinType == RIGHT) {
                    left = broadcast(); right = hash(joinInfo.rightKeys, factory);
                    dst = add(dst, out, leftIn, rightIn, left, right);
                }
            }
        }

        out = left = right = broadcast();
        dst = add(dst, out, leftIn, rightIn, left, right);

        out = left = right = single();
        dst = add(dst, out, leftIn, rightIn, left, right);

        return dst;
    }

    /** */
    private static Map<Suggestion, Integer> add(Map<Suggestion, Integer> dst, IgniteDistribution out,
        IgniteDistribution in, IgniteDistribution newIn) {
        if (BEST_CNT > 0) {
            int exch = 0;

            if (!in.satisfies(newIn))
                exch++;

            return add(dst, new Suggestion(out, newIn), INTS[exch]);
        }
        else
            return add(dst, new Suggestion(out, newIn), INTS[0]);
    }

    /** */
    private static Map<BiSuggestion, Integer> add(Map<BiSuggestion, Integer> dst, IgniteDistribution out,
        IgniteDistribution left, IgniteDistribution right, IgniteDistribution newLeft, IgniteDistribution newRight) {
        if (BEST_CNT > 0) {
            int exch = 0;

            if (!left.satisfies(newLeft))
                exch++;

            if (!right.satisfies(newRight))
                exch++;

            return add(dst, new BiSuggestion(out, newLeft, newRight), INTS[exch]);
        }
        else
            return add(dst, new BiSuggestion(out, newLeft, newRight), INTS[0]);
    }

    /** */
    private static <T> Map<T, Integer> add(Map<T, Integer> dst, T suggest, Integer exchCnt) {
        if (dst == null)
            dst = new HashMap<>();

        dst.merge(suggest, exchCnt, IgniteDistributions::min);

        return dst;
    }

    /** */
    private static <T> List<T> sorted(Map<T, Integer> src) {
        if (BEST_CNT > 0) {
            List<Map.Entry<T, Integer>> entries = new ArrayList<>(src.entrySet());

            entries.sort(Map.Entry.comparingByValue());

            if (entries.size() >= BEST_CNT)
                entries = entries.subList(0, BEST_CNT);

            return Commons.transform(entries, Map.Entry::getKey);
        }

        return new ArrayList<>(src.keySet());
    }

    /** */
    private static @NotNull Integer min(@NotNull Integer i1, @NotNull Integer i2) {
        return i1 < i2 ? i1 : i2;
    }

    /**
     * Projects distribution keys using target mapping.
     * Returns empty collection in case any of distribution keys is lost.
     *
     * @param mapping Target mapping.
     * @param keys Distribution keys.
     * @return New distribution keys.
     */
    public static ImmutableIntList projectDistributionKeys(Mappings.TargetMapping mapping, ImmutableIntList keys) {
        if (mapping.getTargetCount() < keys.size())
            return ImmutableIntList.of();

        int[] resKeys = new int[keys.size()];

        for (int i = 0; i < keys.size(); i++) {
            boolean found = false;
            int key = keys.getInt(i);

            for (int j = 0; j < mapping.getTargetCount(); j++) {
                if (mapping.getSourceOpt(j) != key)
                    continue;

                found = true;
                resKeys[i] = j;

                break;
            }

            if (!found)
                return ImmutableIntList.of();
        }

        return ImmutableIntList.of(resKeys);
    }

    /**
     * @return Values relational node distribution.
     */
    public static IgniteDistribution values(RelDataType rowType, ImmutableList<ImmutableList<RexLiteral>> tuples) {
        return broadcast();
    }

    /**
     * @return Project relational node distribution calculated on the basis of its input and projections.
     */
    public static IgniteDistribution project(RelMetadataQuery mq, RelNode input, List<? extends RexNode> projects) {
        IgniteDistribution inDistr = IgniteMdDistribution._distribution(input, mq);
        Mappings.TargetMapping mapping = Project.getPartialMapping(input.getRowType().getFieldCount(), projects);

        return inDistr.apply(mapping);
    }

    /**
     * @return Single aggregate relational node distribution calculated on the basis of its input and groupingSets.
     * <b>Note</b> that the method returns {@code null} in case the given input cannot be processed by a single aggregate.
     */
    public static IgniteDistribution aggregate(RelMetadataQuery mq, RelNode input, ImmutableBitSet groupSet,
        List<ImmutableBitSet> groupSets, List<AggregateCall> aggCalls) {
        RelDataType inType = input.getRowType();
        IgniteDistribution inDistr = IgniteMdDistribution._distribution(input, mq);

        return aggregate(inType, inDistr, groupSet, groupSets, aggCalls);
    }

    /**
     * @return Single aggregate relational node distribution calculated on the basis of its input and groupingSets.
     * <b>Note</b> that the method returns {@code null} in case the given input cannot be processed by a single aggregate.
     */
    public static IgniteDistribution aggregate(RelDataType inType, IgniteDistribution inDistr, ImmutableBitSet groupSet, List<ImmutableBitSet> groupSets, List<AggregateCall> aggCalls) {
        switch (inDistr.getType()) {
            case SINGLETON:
            case BROADCAST_DISTRIBUTED:
                return inDistr;
            case HASH_DISTRIBUTED:
                if (!simpleAggregate(groupSet, groupSets))
                    return null; // ROLLUP and CUBE require map-reduce processing

                int inFields = inType.getFieldCount();
                int outFields = groupSet.cardinality() + aggCalls.size();

                Mappings.TargetMapping mapping = IgniteAggregate.partialMapping(inFields, outFields, groupSet);

                return inDistr.apply(mapping);
            default:
                // Other group types require map-reduce aggregate processing;
                return null;
        }
    }

    /**
     * @return Map aggregate relational node distribution calculated on the basis of its input and groupingSets.
     * <b>Note</b> that the method returns {@code null} in case the given input cannot be processed in map-reduce
     * style by an aggregate.
     */
    public static IgniteDistribution mapAggregate(RelMetadataQuery mq, RelNode input, ImmutableBitSet groupSet,
        List<ImmutableBitSet> groupSets, List<AggregateCall> aggCalls) {
        IgniteDistribution inputDistr = IgniteMdDistribution._distribution(input, mq);

        switch (inputDistr.getType()) {
            case SINGLETON:
            case BROADCAST_DISTRIBUTED:
                return inputDistr;

            case RANDOM_DISTRIBUTED:
            case HASH_DISTRIBUTED:
                return random(); // its OK to just erase distribution here

            default:
                return null;
        }
    }

    /**
     * @return Reduce aggregate relational node distribution calculated on the basis of its input and groupingSets.
     * <b>Note</b> that the method returns {@code null} in case the given input cannot be reduced.
     */
    public static IgniteDistribution reduceAggregate(RelMetadataQuery mq, RelNode input, ImmutableBitSet groupSet,
        List<ImmutableBitSet> groupSets, List<AggregateCall> aggCalls) {
        IgniteDistribution inputDistr = IgniteMdDistribution._distribution(input, mq);

        // requires singleton distribution
        if (inputDistr.getType() == RelDistribution.Type.SINGLETON)
            return inputDistr;

        return null;
    }

    /**
     * See {@link RelTraitDef#canonize(org.apache.calcite.plan.RelTrait)}.
     */
    private static IgniteDistribution canonize(IgniteDistribution distr) {
        return DistributionTraitDef.INSTANCE.canonize(distr);
    }

    /** */
    private static boolean simpleAggregate(ImmutableBitSet groupSet, List<ImmutableBitSet> groupSets) {
        return Aggregate.Group.induce(groupSet, groupSets) == Aggregate.Group.SIMPLE;
    }

    /**
     * Distribution suggestion for SingleRel.
     */
    public static class Suggestion {
        /** */
        private final IgniteDistribution out;

        /** */
        private final IgniteDistribution in;

        /**
         * @param out Result distribution.
         * @param in Required in distribution.
         */
        public Suggestion(IgniteDistribution out, IgniteDistribution in) {
            this.out = out;
            this.in = in;
        }

        /**
         * @return Result distribution.
         */
        public IgniteDistribution out() {
            return out;
        }

        /**
         * @return Required in distribution.
         */
        public IgniteDistribution in() {
            return in;
        }

        /** {@inheritDoc} */
        @Override public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;

            Suggestion that = (Suggestion) o;

            return out == that.out && in == that.in;
        }

        /** {@inheritDoc} */
        @Override public int hashCode() {
            int result = out.hashCode();
            result = 31 * result + in.hashCode();
            return result;
        }
    }

    /**
     * Distribution suggestion for BiRel.
     */
    public static class BiSuggestion {
        /** */
        private final IgniteDistribution out;

        /** */
        private final IgniteDistribution left;

        /** */
        private final IgniteDistribution right;

        /**
         * @param out Result distribution.
         * @param left Required left distribution.
         * @param right Required right distribution.
         */
        public BiSuggestion(IgniteDistribution out, IgniteDistribution left, IgniteDistribution right) {
            this.out = out;
            this.left = left;
            this.right = right;
        }

        /**
         * @return Result distribution.
         */
        public IgniteDistribution out() {
            return out;
        }

        /**
         * @return Required left distribution.
         */
        public IgniteDistribution left() {
            return left;
        }

        /**
         * @return Required right distribution.
         */
        public IgniteDistribution right() {
            return right;
        }

        /** {@inheritDoc} */
        @Override public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;

            BiSuggestion that = (BiSuggestion) o;

            return out == that.out
                && left == that.left
                && right == that.right;
        }

        /** {@inheritDoc} */
        @Override public int hashCode() {
            int result = out.hashCode();
            result = 31 * result + left.hashCode();
            result = 31 * result + right.hashCode();
            return result;
        }
    }
}
