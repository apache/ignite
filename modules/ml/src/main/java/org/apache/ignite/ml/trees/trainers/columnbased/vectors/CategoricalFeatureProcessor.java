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

package org.apache.ignite.ml.trees.trainers.columnbased.vectors;

import com.zaxxer.sparsebits.SparseBitSet;
import java.util.Arrays;
import java.util.BitSet;
import java.util.Comparator;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.DoubleStream;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;
import org.apache.ignite.lang.IgniteBiTuple;
import org.apache.ignite.ml.math.functions.IgniteFunction;
import org.apache.ignite.ml.trees.CategoricalRegionInfo;
import org.apache.ignite.ml.trees.CategoricalSplitInfo;
import org.apache.ignite.ml.trees.RegionInfo;
import org.apache.ignite.ml.trees.trainers.columnbased.RegionProjection;

import static org.apache.ignite.ml.trees.trainers.columnbased.vectors.Utils.splitByBitSet;

/**
 * Categorical feature vector processor implementation used by {@see ColumnDecisionTreeTrainer}.
 */
public class CategoricalFeatureProcessor
    implements FeatureProcessor<CategoricalRegionInfo, CategoricalSplitInfo<CategoricalRegionInfo>> {
    /** Count of categories for this feature. */
    private final int catsCnt;

    /** Function for calculating impurity of a given region of points. */
    private final IgniteFunction<DoubleStream, Double> calc;

    /**
     * @param calc Function for calculating impurity of a given region of points.
     * @param catsCnt Number of categories.
     */
    public CategoricalFeatureProcessor(IgniteFunction<DoubleStream, Double> calc, int catsCnt) {
        this.calc = calc;
        this.catsCnt = catsCnt;
    }

    /** */
    private SplitInfo<CategoricalRegionInfo> split(BitSet leftCats, int intervalIdx, Map<Integer, Integer> mapping,
        Integer[] sampleIndexes, double[] values, double[] labels, double impurity) {
        Map<Boolean, List<Integer>> leftRight = Arrays.stream(sampleIndexes).
            collect(Collectors.partitioningBy((smpl) -> leftCats.get(mapping.get((int)values[smpl]))));

        List<Integer> left = leftRight.get(true);
        int leftSize = left.size();
        double leftImpurity = calc.apply(left.stream().mapToDouble(s -> labels[s]));

        List<Integer> right = leftRight.get(false);
        int rightSize = right.size();
        double rightImpurity = calc.apply(right.stream().mapToDouble(s -> labels[s]));

        int totalSize = leftSize + rightSize;

        // Result of this call will be sent back to trainer node, we do not need vectors inside of sent data.
        CategoricalSplitInfo<CategoricalRegionInfo> res = new CategoricalSplitInfo<>(intervalIdx,
            new CategoricalRegionInfo(leftImpurity, null), // cats can be computed on the last step.
            new CategoricalRegionInfo(rightImpurity, null),
            leftCats);

        res.setInfoGain(impurity - (double)leftSize / totalSize * leftImpurity - (double)rightSize / totalSize * rightImpurity);
        return res;
    }

    /**
     * Get a stream of subsets given categories count.
     *
     * @param catsCnt categories count.
     * @return Stream of subsets given categories count.
     */
    private Stream<BitSet> powerSet(int catsCnt) {
        Iterable<BitSet> iterable = () -> new PSI(catsCnt);
        return StreamSupport.stream(iterable.spliterator(), false);
    }

    /** {@inheritDoc} */
    @Override public SplitInfo findBestSplit(RegionProjection<CategoricalRegionInfo> regionProjection, double[] values,
        double[] labels, int regIdx) {
        Map<Integer, Integer> mapping = mapping(regionProjection.data().cats());

        return powerSet(regionProjection.data().cats().length()).
            map(s -> split(s, regIdx, mapping, regionProjection.sampleIndexes(), values, labels, regionProjection.data().impurity())).
            max(Comparator.comparingDouble(SplitInfo::infoGain)).
            orElse(null);
    }

    /** {@inheritDoc} */
    @Override public RegionProjection<CategoricalRegionInfo> createInitialRegion(Integer[] sampleIndexes,
        double[] values, double[] labels) {
        BitSet set = new BitSet();
        set.set(0, catsCnt);

        Double impurity = calc.apply(Arrays.stream(labels));

        return new RegionProjection<>(sampleIndexes, new CategoricalRegionInfo(impurity, set), 0);
    }

    /** {@inheritDoc} */
    @Override public SparseBitSet calculateOwnershipBitSet(RegionProjection<CategoricalRegionInfo> regionProjection,
        double[] values,
        CategoricalSplitInfo<CategoricalRegionInfo> s) {
        SparseBitSet res = new SparseBitSet();
        Arrays.stream(regionProjection.sampleIndexes()).forEach(smpl -> res.set(smpl, s.bitSet().get((int)values[smpl])));
        return res;
    }

    /** {@inheritDoc} */
    @Override public IgniteBiTuple<RegionProjection, RegionProjection> performSplit(SparseBitSet bs,
        RegionProjection<CategoricalRegionInfo> reg, CategoricalRegionInfo leftData, CategoricalRegionInfo rightData) {
        return performSplitGeneric(bs, null, reg, leftData, rightData);
    }

    /** {@inheritDoc} */
    @Override public IgniteBiTuple<RegionProjection, RegionProjection> performSplitGeneric(
        SparseBitSet bs, double[] values, RegionProjection<CategoricalRegionInfo> reg, RegionInfo leftData,
        RegionInfo rightData) {
        int depth = reg.depth();

        int lSize = bs.cardinality();
        int rSize = reg.sampleIndexes().length - lSize;
        IgniteBiTuple<Integer[], Integer[]> lrSamples = splitByBitSet(lSize, rSize, reg.sampleIndexes(), bs);
        BitSet leftCats = calculateCats(lrSamples.get1(), values);
        CategoricalRegionInfo lInfo = new CategoricalRegionInfo(leftData.impurity(), leftCats);

        // TODO: IGNITE-5892 Check how it will work with sparse data.
        BitSet rightCats = calculateCats(lrSamples.get2(), values);
        CategoricalRegionInfo rInfo = new CategoricalRegionInfo(rightData.impurity(), rightCats);

        RegionProjection<CategoricalRegionInfo> rPrj = new RegionProjection<>(lrSamples.get2(), rInfo, depth + 1);
        RegionProjection<CategoricalRegionInfo> lPrj = new RegionProjection<>(lrSamples.get1(), lInfo, depth + 1);
        return new IgniteBiTuple<>(lPrj, rPrj);
    }

    /**
     * Powerset iterator. Iterates not over the whole powerset, but on half of it.
     */
    private static class PSI implements Iterator<BitSet> {

        /** Current subset number. */
        private int i = 1; // We are not interested in {emptyset, set} split and therefore start from 1.

        /** Size of set, subsets of which we iterate over. */
        final int size;

        /**
         * @param bitCnt Size of set, subsets of which we iterate over.
         */
        public PSI(int bitCnt) {
            this.size = 1 << (bitCnt - 1);
        }

        /** {@inheritDoc} */
        @Override public boolean hasNext() {
            return i < size;
        }

        /** {@inheritDoc} */
        @Override public BitSet next() {
            BitSet res = BitSet.valueOf(new long[] {i});
            i++;
            return res;
        }
    }

    /** */
    private Map<Integer, Integer> mapping(BitSet bs) {
        int bn = 0;
        Map<Integer, Integer> res = new HashMap<>();

        int i = 0;
        while ((bn = bs.nextSetBit(bn)) != -1) {
            res.put(bn, i);
            i++;
            bn++;
        }

        return res;
    }

    /** Get set of categories of given samples */
    private BitSet calculateCats(Integer[] sampleIndexes, double[] values) {
        BitSet res = new BitSet();

        for (int smpl : sampleIndexes)
            res.set((int)values[smpl]);

        return res;
    }
}
