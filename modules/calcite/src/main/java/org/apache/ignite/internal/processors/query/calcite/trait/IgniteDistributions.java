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

import java.util.ArrayList;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import org.apache.calcite.plan.RelTraitDef;
import org.apache.calcite.rel.RelDistribution;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.JoinInfo;
import org.apache.calcite.rel.core.JoinRelType;
import org.apache.calcite.rel.metadata.RelMetadataQuery;
import org.apache.calcite.util.ImmutableIntList;
import org.apache.calcite.util.mapping.Mappings;
import org.apache.ignite.IgniteSystemProperties;
import org.apache.ignite.internal.processors.query.calcite.metadata.IgniteMdDerivedDistribution;
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
     * Suggests possible join distributions.
     *
     * @param left Left node.
     * @param right Right node.
     * @param joinInfo Join info.
     * @param joinType Join type.
     * @return Array of possible distributions, sorted by their efficiency (cheaper first).
     */
    public static List<BiSuggestion> suggestJoin(RelNode left, RelNode right, JoinInfo joinInfo, JoinRelType joinType) {
        RelMetadataQuery mq = left.getCluster().getMetadataQuery();

        List<IgniteDistribution> leftIn = IgniteMdDerivedDistribution._deriveDistributions(left, mq);
        List<IgniteDistribution> rightIn = IgniteMdDerivedDistribution._deriveDistributions(right, mq);

        Map<BiSuggestion, Integer> suggestions = new LinkedHashMap<>();

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
        return sorted(suggestJoin0(new LinkedHashMap<>(), leftIn, rightIn, joinInfo, joinType));
    }

    /**
     * Suggests possible join distributions using inputs distributions and join info
     * based on next distributions table:
     *
     * ===============INNER JOIN==============
     * hash + hash = hash
     * broadcast + hash = hash
     * hash + broadcast = hash
     * broadcast + broadcast = broadcast
     * single + single = single
     *
     * ===============LEFT JOIN===============
     * hash + hash = hash
     * hash + broadcast = hash
     * broadcast + broadcast = broadcast
     * single + single = single
     *
     * ===============RIGHT JOIN==============
     * hash + hash = hash
     * broadcast + hash = hash
     * broadcast + broadcast = broadcast
     * single + single = single
     *
     * ===========FULL JOIN/CROSS JOIN========
     * broadcast + broadcast = broadcast
     * single + single = single
     *
     * others require redistribution.
     *
     * @param leftIn Left distribution.
     * @param rightIn Right distribution.
     * @param joinInfo Join info.
     * @param joinType Join type.
     */
    private static Map<BiSuggestion, Integer> suggestJoin0(Map<BiSuggestion, Integer> dst, IgniteDistribution leftIn, IgniteDistribution rightIn,
        JoinInfo joinInfo, JoinRelType joinType) {
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
    private static Map<BiSuggestion, Integer> add(Map<BiSuggestion, Integer> dst, IgniteDistribution out, IgniteDistribution left, IgniteDistribution right,
        IgniteDistribution newLeft, IgniteDistribution newRight) {
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
    private static Map<BiSuggestion, Integer> add(Map<BiSuggestion, Integer> dst, BiSuggestion suggest, Integer exchCnt) {
        if (dst == null)
            dst = new LinkedHashMap<>();

        dst.merge(suggest, exchCnt, IgniteDistributions::min);

        return dst;
    }

    /** */
    private static List<BiSuggestion> sorted(Map<BiSuggestion, Integer> src) {
        if (BEST_CNT > 0) {
            List<Map.Entry<BiSuggestion, Integer>> entries = new ArrayList<>(src.entrySet());

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
     * See {@link RelTraitDef#canonize(org.apache.calcite.plan.RelTrait)}.
     */
    private static IgniteDistribution canonize(IgniteDistribution distr) {
        return DistributionTraitDef.INSTANCE.canonize(distr);
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
