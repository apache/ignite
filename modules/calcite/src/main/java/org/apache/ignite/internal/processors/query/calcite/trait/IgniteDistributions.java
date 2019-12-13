/*
 * Copyright 2019 GridGain Systems, Inc. and Contributors.
 *
 * Licensed under the GridGain Community Edition License (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     https://www.gridgain.com/products/software/community-edition/gridgain-community-edition-license
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
import org.apache.calcite.rel.core.JoinInfo;
import org.apache.calcite.rel.core.JoinRelType;
import org.apache.calcite.util.ImmutableIntList;
import org.apache.calcite.util.mapping.Mappings;
import org.apache.ignite.internal.util.typedef.F;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.jetbrains.annotations.NotNull;

import static org.apache.calcite.rel.RelDistribution.Type.ANY;
import static org.apache.calcite.rel.RelDistribution.Type.BROADCAST_DISTRIBUTED;
import static org.apache.calcite.rel.RelDistribution.Type.HASH_DISTRIBUTED;
import static org.apache.calcite.rel.RelDistribution.Type.RANDOM_DISTRIBUTED;
import static org.apache.calcite.rel.RelDistribution.Type.SINGLETON;
import static org.apache.calcite.rel.core.JoinRelType.INNER;
import static org.apache.calcite.rel.core.JoinRelType.LEFT;
import static org.apache.calcite.rel.core.JoinRelType.RIGHT;

/**
 *
 */
public class IgniteDistributions {
    private static final int BEST_CNT = 3;

    private static final IgniteDistribution BROADCAST_DISTR = new DistributionTrait(BROADCAST_DISTRIBUTED, ImmutableIntList.of(), AllTargetsFactory.INSTANCE);
    private static final IgniteDistribution SINGLETON_DISTR = new DistributionTrait(SINGLETON, ImmutableIntList.of(), SingleTargetFactory.INSTANCE);
    private static final IgniteDistribution RANDOM_DISTR = new DistributionTrait(RANDOM_DISTRIBUTED, ImmutableIntList.of(), RandomTargetFactory.INSTANCE);
    private static final IgniteDistribution ANY_DISTR = new DistributionTrait(ANY, ImmutableIntList.of(), NoOpFactory.INSTANCE);

    public static IgniteDistribution any() {
        return ANY_DISTR;
    }

    public static IgniteDistribution random() {
        return RANDOM_DISTR;
    }

    public static IgniteDistribution single() {
        return SINGLETON_DISTR;
    }

    public static IgniteDistribution broadcast() {
        return BROADCAST_DISTR;
    }

    public static IgniteDistribution hash(List<Integer> keys) {
        return DistributionTraitDef.INSTANCE.canonize(
            new DistributionTrait(HASH_DISTRIBUTED, ImmutableIntList.copyOf(keys), HashFunctionFactory.INSTANCE));
    }

    public static IgniteDistribution hash(List<Integer> keys, DestinationFunctionFactory factory) {
        return DistributionTraitDef.INSTANCE.canonize(
            new DistributionTrait(HASH_DISTRIBUTED, ImmutableIntList.copyOf(keys), factory));
    }

    public static List<BiSuggestion> suggestJoin(IgniteDistribution leftIn, IgniteDistribution rightIn,
        JoinInfo joinInfo, JoinRelType joinType) {
        return topN(suggestJoin0(leftIn, rightIn, joinInfo, joinType), BEST_CNT);
    }

    public static List<BiSuggestion> suggestJoin(List<IgniteDistribution> leftIn, List<IgniteDistribution> rightIn,
        JoinInfo joinInfo, JoinRelType joinType) {
        HashSet<BiSuggestion> suggestions = new HashSet<>();

        int bestCnt = 0;

        for (IgniteDistribution leftIn0 : leftIn) {
            for (IgniteDistribution rightIn0 : rightIn) {
                for (BiSuggestion suggest : suggestJoin0(leftIn0, rightIn0, joinInfo, joinType)) {
                    if (suggestions.add(suggest) && suggest.needExchange == 0 && (++bestCnt) == BEST_CNT)
                        topN(new ArrayList<>(suggestions), BEST_CNT);
                }
            }
        }

        return topN(new ArrayList<>(suggestions), BEST_CNT);
    }

    private static ArrayList<BiSuggestion> suggestJoin0(IgniteDistribution leftIn, IgniteDistribution rightIn,
        JoinInfo joinInfo, JoinRelType joinType) {
        /*
         * Distributions table:
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
         *
         * others require redistribution
         */

        ArrayList<BiSuggestion> res = new ArrayList<>();

        IgniteDistribution out, left, right;

        if (joinType == LEFT || joinType == RIGHT || (joinType == INNER && !F.isEmpty(joinInfo.keys()))) {
            HashSet<DestinationFunctionFactory> factories = U.newHashSet(3);

            if (leftIn.getKeys().equals(joinInfo.leftKeys))
                factories.add(leftIn.destinationFunctionFactory());

            if (rightIn.getKeys().equals(joinInfo.rightKeys))
                factories.add(rightIn.destinationFunctionFactory());

            factories.add(HashFunctionFactory.INSTANCE);

            for (DestinationFunctionFactory factory : factories) {
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

    private static int add(ArrayList<BiSuggestion> dst, IgniteDistribution out, IgniteDistribution left, IgniteDistribution right,
        IgniteDistribution newLeft, IgniteDistribution newRight) {
        int exch = 0;

        if (!left.satisfies(newLeft))
            exch++;

        if (!right.satisfies(newRight))
            exch++;

        dst.add(new BiSuggestion(out, newLeft, newRight, exch));

        return exch;
    }

    private static List<BiSuggestion> topN(ArrayList<BiSuggestion> src, int n) {
        Collections.sort(src);

        return src.size() <= n ? src : src.subList(0, n);
    }

    public static List<Integer> projectDistributionKeys(Mappings.TargetMapping mapping, ImmutableIntList keys) {
        if (mapping.getTargetCount() < keys.size())
            return Collections.emptyList();

        List<Integer> resKeys = new ArrayList<>(mapping.getTargetCount());

        parent:
        for (int i = 0; i < keys.size(); i++) {
            int key = keys.getInt(i);

            for (int j = 0; j < mapping.getTargetCount(); j++) {
                if (mapping.getSourceOpt(j) == key) {
                    resKeys.add(j);

                    continue parent;
                }
            }

            return Collections.emptyList();
        }

        return resKeys;
    }

    public static class BiSuggestion implements Comparable<BiSuggestion> {
        private final IgniteDistribution out;
        private final IgniteDistribution left;
        private final IgniteDistribution right;
        private final int needExchange;

        public BiSuggestion(IgniteDistribution out, IgniteDistribution left, IgniteDistribution right, int needExchange) {
            this.out = out;
            this.left = left;
            this.right = right;
            this.needExchange = needExchange;
        }

        public IgniteDistribution out() {
            return out;
        }

        public IgniteDistribution left() {
            return left;
        }

        public IgniteDistribution right() {
            return right;
        }

        public int needExchange() {
            return needExchange;
        }

        @Override public int compareTo(@NotNull IgniteDistributions.BiSuggestion o) {
            return Integer.compare(needExchange, o.needExchange);
        }

        @Override public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;

            BiSuggestion that = (BiSuggestion) o;

            if (needExchange != that.needExchange) return false;
            if (out != that.out) return false;
            if (left != that.left) return false;
            return right == that.right;
        }

        @Override public int hashCode() {
            int result = out.hashCode();
            result = 31 * result + left.hashCode();
            result = 31 * result + right.hashCode();
            result = 31 * result + needExchange;
            return result;
        }
    }
}
