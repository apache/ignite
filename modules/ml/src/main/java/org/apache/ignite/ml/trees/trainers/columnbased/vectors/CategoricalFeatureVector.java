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
import it.unimi.dsi.fastutil.ints.IntArrayList;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.BitSet;
import java.util.Comparator;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedList;
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

/**
 * Categorical feature vector implementation used by {@see ColumnDecisionTreeTrainer}.
 */
public class CategoricalFeatureVector
    implements FeatureVector<CategoricalRegionInfo, CategoricalSplitInfo<CategoricalRegionInfo>> {
    /** Intervals data. */
    private List<CategoricalRegionInfo> intervals;

    /** Samples data. */
    private List<SampleInfo> samples;

    /** Function for calculating impurity of a given region of points. */
    private IgniteFunction<DoubleStream, Double> calc;

    /**
     * @param calc Function for calculating impurity of a given region of points.
     * @param data Projection of samples on given feature in format of stream of (sample index, projection value).
     * @param samplesCnt Number of samples.
     * @param labels Labels of samples.
     * @param catsCnt Number of categories.
     */
    public CategoricalFeatureVector(IgniteFunction<DoubleStream, Double> calc,
        Stream<IgniteBiTuple<Integer, Double>> data,
        int samplesCnt, double[] labels, int catsCnt) {
        samples = new ArrayList<>(samplesCnt);
        this.calc = calc;

        intervals = new LinkedList<>();

        data.forEach(d -> samples.add(new SampleInfo(labels[d.get1()], d.get2(), d.get1())));

        int[] vecs = new int[samples.size()];
        for (int j = 0; j < vecs.length; j++)
            vecs[j] = j;

        BitSet set = new BitSet();
        set.set(0, catsCnt);

        Double impurity = calc.apply(samples.stream().mapToDouble(SampleInfo::getVal));

        intervals.add(0, new CategoricalRegionInfo(impurity, vecs, set));
    }

    /** {@inheritDoc} */
    @Override public SplitInfo<CategoricalRegionInfo> findBestSplit() {
        SplitInfo<CategoricalRegionInfo> res = null;
        double curInfoGain = 0.0;

        int i = 0;

        for (CategoricalRegionInfo interval : intervals) {
            final int j = i;

            Map<Integer, Integer> mapping = mapping(interval.cats());

            SplitInfo<CategoricalRegionInfo> bestForInterval = powerSet(interval.cats().length()).
                map(s -> split(s, j, mapping, interval)).
                max(Comparator.comparingDouble(SplitInfo::infoGain)).
                orElse(null);

            if (bestForInterval != null && bestForInterval.infoGain() > curInfoGain) {
                res = bestForInterval;
                curInfoGain = bestForInterval.infoGain();
            }

            i++;
        }

        return res;
    }

    /** {@inheritDoc} */
    @Override public SparseBitSet calculateOwnershipBitSet(CategoricalSplitInfo<CategoricalRegionInfo> s) {
        SparseBitSet res = new SparseBitSet();
        Arrays.stream(intervals.get(s.regionIndex()).vectors()).forEach(i -> res.set(i, s.bitSet().get((int)samples.get(i).getVal())));
        return res;
    }

    /** {@inheritDoc} */
    @Override public FeatureVector<CategoricalRegionInfo, CategoricalSplitInfo<CategoricalRegionInfo>> performSplit(
        SparseBitSet bs, int regionIdx, CategoricalRegionInfo leftData, CategoricalRegionInfo rightData) {
        return performSplitGeneric(bs, regionIdx, leftData, rightData);
    }

    /** {@inheritDoc} */
    @Override public CategoricalFeatureVector performSplitGeneric(SparseBitSet bs, int regionIdx, RegionInfo leftData,
        RegionInfo rightData) {
        CategoricalRegionInfo toSplit = intervals.get(regionIdx);

        // Modify existing interval (left interval of new split).
        int[] leftVecs = filterVecs(toSplit.vectors(), bs);
        BitSet leftCats = calculateCats(leftVecs);
        intervals.set(regionIdx, new CategoricalRegionInfo(leftData.impurity(), leftVecs, leftCats));

        // Add new interval next to the left interval
        // TODO: check how it will work with sparse data.
        bs.flip(0, samples.size());
        int[] rightVecs = filterVecs(toSplit.vectors(), bs);
        BitSet rightCats = calculateCats(rightVecs);
        intervals.add(regionIdx + 1, new CategoricalRegionInfo(rightData.impurity(), rightVecs, rightCats));

        return this;
    }

    /** {@inheritDoc} */
    @Override public double[] calculateRegions(IgniteFunction<DoubleStream, Double> regCalc) {
        double[] res = new double[intervals.size()];

        int i = 0;
        for (CategoricalRegionInfo interval : intervals) {
            res[i] = regCalc.apply(Arrays.stream(interval.vectors()).mapToDouble(j -> samples.get(j).getLabel()));
            i++;
        }

        return res;
    }

    /** */
    private int[] filterVecs(int[] vecs, SparseBitSet bs) {
        IntArrayList res = new IntArrayList(bs.length());
        Arrays.stream(vecs).filter(bs::get).forEach(res::add);
        return res.toIntArray();
    }

    /** */
    private SplitInfo<CategoricalRegionInfo> split(BitSet leftCats, int intervalIdx, Map<Integer, Integer> mapping,
        CategoricalRegionInfo interval) {
        Map<Boolean, List<Integer>> leftRight = Arrays.stream(interval.vectors()).boxed().
            collect(Collectors.partitioningBy((bi) -> leftCats.get(mapping.get((int)samples.get(bi).getVal()))));

        int leftSize = leftRight.get(true).size();
        double leftImpurity = calc.apply(leftRight.get(true).stream().mapToDouble(i -> samples.get(i).getLabel()));

        int rightSize = leftRight.get(false).size();
        double rightImpurity = calc.apply(leftRight.get(false).stream().mapToDouble(i -> samples.get(i).getLabel()));

        int totalSize = leftSize + rightSize;

        // Result of this call will be sent back to trainer node, we do not need vectors inside of sent data.
        CategoricalSplitInfo<CategoricalRegionInfo> res = new CategoricalSplitInfo<>(intervalIdx,
            new CategoricalRegionInfo(leftImpurity, null, null), // cats can be computed on the last step.
            new CategoricalRegionInfo(rightImpurity, null, null),
            leftCats);

        res.setInfoGain(interval.impurity() - (double)leftSize / totalSize * leftImpurity - (double)rightSize / totalSize * rightImpurity);
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

    /**
     * Powerset iterator. Iterates not over the whole powerset, but on half of it.
     */
    private static class PSI implements Iterator<BitSet> {

        /** Current subset number. */
        private int i = 1; // We are not interested in {emptyset, set} split and therefore start from 1.

        /** Size of set, subsets of which we iterate over. */
        int size;

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
    private BitSet calculateCats(int[] vecs) {
        BitSet res = new BitSet();

        for (int vec : vecs)
            res.set((int)samples.get(vec).getVal());

        return res;
    }
}
