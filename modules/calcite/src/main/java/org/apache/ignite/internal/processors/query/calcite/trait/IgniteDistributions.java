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
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Objects;
import org.apache.calcite.plan.RelTraitDef;
import org.apache.calcite.rel.core.JoinInfo;
import org.apache.calcite.rel.core.JoinRelType;
import org.apache.calcite.util.ImmutableIntList;
import org.apache.calcite.util.mapping.Mappings;
import org.apache.ignite.internal.processors.query.calcite.trait.DistributionFunction.AffinityDistribution;
import org.apache.ignite.internal.processors.query.calcite.trait.DistributionFunction.AnyDistribution;
import org.apache.ignite.internal.processors.query.calcite.trait.DistributionFunction.BroadcastDistribution;
import org.apache.ignite.internal.processors.query.calcite.trait.DistributionFunction.HashDistribution;
import org.apache.ignite.internal.processors.query.calcite.trait.DistributionFunction.RandomDistribution;
import org.apache.ignite.internal.processors.query.calcite.trait.DistributionFunction.SingletonDistribution;
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
    private static final int BEST_CNT = 3;

    /** */
    private static final IgniteDistribution BROADCAST = new DistributionTrait(BroadcastDistribution.INSTANCE);

    /** */
    private static final IgniteDistribution SINGLETON = new DistributionTrait(SingletonDistribution.INSTANCE);

    /** */
    private static final IgniteDistribution RANDOM = new DistributionTrait(RandomDistribution.INSTANCE);

    /** */
    private static final IgniteDistribution ANY = new DistributionTrait(AnyDistribution.INSTANCE);

    /**
     * @return Any distribution.
     */
    public static IgniteDistribution any() {
        return canonize(ANY);
    }

    /**
     * @return Random distribution.
     */
    public static IgniteDistribution random() {
        return canonize(RANDOM);
    }

    /**
     * @return Single distribution.
     */
    public static IgniteDistribution single() {
        return canonize(SINGLETON);
    }

    /**
     * @return Broadcast distribution.
     */
    public static IgniteDistribution broadcast() {
        return canonize(BROADCAST);
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
     * @param key Affinity key.
     * @param cacheId Affinity cache ID.
     * @param identity Affinity identity key.
     * @return Affinity distribution.
     */
    public static IgniteDistribution affinity(int key, int cacheId, Object identity) {
        return canonize(new DistributionTrait(ImmutableIntList.of(key), new AffinityDistribution(cacheId, identity)));
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
     * See {@link RelTraitDef#canonize(org.apache.calcite.plan.RelTrait)}.
     */
    public static IgniteDistribution canonize(IgniteDistribution distr) {
        return DistributionTraitDef.INSTANCE.canonize(distr);
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
        return topN(suggestJoin0(leftIn, rightIn, joinInfo, joinType), BEST_CNT);
    }

    /**
     * Suggests possible join distributions.
     *
     * @param leftIn Left distributions.
     * @param rightIn Right distributions.
     * @param joinInfo Join info.
     * @param joinType Join type.
     * @return Array of possible distributions, sorted by their efficiency (cheaper first).
     */
    public static List<BiSuggestion> suggestJoin(List<IgniteDistribution> leftIn, List<IgniteDistribution> rightIn,
        JoinInfo joinInfo, JoinRelType joinType) {
        HashSet<BiSuggestion> suggestions = new HashSet<>();

        int bestCnt = 0;

        for (IgniteDistribution leftIn0 : leftIn) {
            for (IgniteDistribution rightIn0 : rightIn) {
                for (BiSuggestion suggest : suggestJoin0(leftIn0, rightIn0, joinInfo, joinType)) {
                    if (suggestions.add(suggest) && suggest.needExchange == 0 && (++bestCnt) == BEST_CNT)
                        return topN(new ArrayList<>(suggestions), BEST_CNT);
                }
            }
        }

        return topN(new ArrayList<>(suggestions), BEST_CNT);
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
     * @return Array of possible distributions, sorted by their efficiency (cheaper first).
     */
    private static ArrayList<BiSuggestion> suggestJoin0(IgniteDistribution leftIn, IgniteDistribution rightIn,
        JoinInfo joinInfo, JoinRelType joinType) {

        ArrayList<BiSuggestion> res = new ArrayList<>();

        IgniteDistribution out, left, right;

        if (joinType == LEFT || joinType == RIGHT || (joinType == INNER && !F.isEmpty(joinInfo.pairs()))) {
            HashSet<DistributionFunction> factories = U.newHashSet(3);

            if (Objects.equals(joinInfo.leftKeys, leftIn.getKeys()))
                factories.add(leftIn.function());

            if (Objects.equals(joinInfo.rightKeys, rightIn.getKeys()))
                factories.add(rightIn.function());

            factories.add(HashDistribution.INSTANCE);

            for (DistributionFunction factory : factories) {
                out = hash(joinInfo.leftKeys, factory);

                left = hash(joinInfo.leftKeys, factory); right = hash(joinInfo.rightKeys, factory);
                add(res, out, leftIn, rightIn, left, right);

                if (joinType == INNER || joinType == LEFT) {
                    left = hash(joinInfo.leftKeys, factory); right = broadcast();
                    add(res, out, leftIn, rightIn, left, right);
                }

                if (joinType == INNER || joinType == RIGHT) {
                    left = broadcast(); right = hash(joinInfo.rightKeys, factory);
                    add(res, out, leftIn, rightIn, left, right);
                }
            }
        }

        out = left = right = broadcast();
        add(res, out, leftIn, rightIn, left, right);

        out = left = right = single();
        add(res, out, leftIn, rightIn, left, right);

        return res;
    }

    /** */
    private static int add(ArrayList<BiSuggestion> dst, IgniteDistribution out, IgniteDistribution left, IgniteDistribution right,
        IgniteDistribution newLeft, IgniteDistribution newRight) {
        int exch = 0;

        if (needsExchange(left, newLeft))
            exch++;

        if (needsExchange(right, newRight))
            exch++;

        dst.add(new BiSuggestion(out, newLeft, newRight, exch));

        return exch;
    }

    /** */
    private static boolean needsExchange(IgniteDistribution sourceDist, IgniteDistribution targetDist) {
        return !sourceDist.satisfies(targetDist);
    }

    /** */
    @SuppressWarnings("SameParameterValue")
    private static List<BiSuggestion> topN(ArrayList<BiSuggestion> src, int n) {
        Collections.sort(src);

        return src.size() <= n ? src : src.subList(0, n);
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
     * Distribution suggestion for BiRel.
     */
    public static class BiSuggestion implements Comparable<BiSuggestion> {
        /** */
        private final IgniteDistribution out;

        /** */
        private final IgniteDistribution left;

        /** */
        private final IgniteDistribution right;

        /** */
        private final int needExchange;

        /**
         * @param out Result distribution.
         * @param left Required left distribution.
         * @param right Required right distribution.
         * @param needExchange Exchanges count (for ordering).
         */
        public BiSuggestion(IgniteDistribution out, IgniteDistribution left, IgniteDistribution right, int needExchange) {
            this.out = out;
            this.left = left;
            this.right = right;
            this.needExchange = needExchange;
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
        @Override public int compareTo(@NotNull IgniteDistributions.BiSuggestion o) {
            return Integer.compare(needExchange, o.needExchange);
        }

        /** {@inheritDoc} */
        @Override public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;

            BiSuggestion that = (BiSuggestion) o;

            return needExchange == that.needExchange
                && out == that.out
                && left == that.left
                && right == that.right;
        }

        /** {@inheritDoc} */
        @Override public int hashCode() {
            int result = out.hashCode();
            result = 31 * result + left.hashCode();
            result = 31 * result + right.hashCode();
            result = 31 * result + needExchange;
            return result;
        }
    }
}
