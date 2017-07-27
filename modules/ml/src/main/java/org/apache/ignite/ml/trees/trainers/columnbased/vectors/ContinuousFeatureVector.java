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
import java.util.ArrayList;
import java.util.Comparator;
import java.util.LinkedList;
import java.util.List;
import java.util.stream.DoubleStream;
import java.util.stream.Stream;
import org.apache.ignite.lang.IgniteBiTuple;
import org.apache.ignite.ml.math.functions.IgniteFunction;
import org.apache.ignite.ml.trees.ContinuousRegionInfo;
import org.apache.ignite.ml.trees.ContinuousSplitCalculator;
import org.apache.ignite.ml.trees.RegionInfo;

/**
 * Container of projection of samples on continuous feature.
 *
 * @param <D> Information about regions. Designed to contain information which will make computations of impurity
 * optimal.
 */
public class ContinuousFeatureVector<D extends ContinuousRegionInfo> implements
    FeatureVector<D, ContinuousSplitInfo<D>> {
    /** ContinuousSplitCalculator used for calculating of best split of each region. */
    private final ContinuousSplitCalculator<D> calc;

    /** Samples. */
    private List<SampleInfo> samples;

    /** Information about regions. */
    private final List<D> regions = new LinkedList<>();

    /**
     * @param splitCalc Calculator used for calculating splits.
     * @param data Stream containing projection of samples on this feature in format (sample index, value of
     * projection).
     * @param samplesCnt Number of samples.
     * @param labels Labels of samples.
     */
    public ContinuousFeatureVector(ContinuousSplitCalculator<D> splitCalc, Stream<IgniteBiTuple<Integer, Double>> data,
        int samplesCnt, double[] labels) {
        samples = new ArrayList<>(samplesCnt);

        data.forEach(d ->
            samples.add(new SampleInfo(labels[d.get1()], d.get2(), d.get1()))
        );

        this.calc = splitCalc;

        samples.sort(Comparator.comparingDouble(SampleInfo::getVal));
        regions.add(calc.calculateRegionInfo(samples.stream().mapToDouble(SampleInfo::getLabel), 0));
    }

    /** {@inheritDoc} */
    @Override public SplitInfo<D> findBestSplit() {
        double maxInfoGain = 0.0;
        SplitInfo<D> res = null;

        // Try to split every possible interval and find the best split.
        int i = 0;
        for (D info : regions) {
            int l = info.left();
            int r = info.right();
            int size = (r - l) + 1;

            double curImpurity = info.impurity();
            SplitInfo<D> split = calc.splitRegion(samples.subList(l, r + 1).stream(), i, info);

            if (split == null) {
                i++;
                continue;
            }

            double lWeight = ((double)split.leftData().right() - split.leftData().left() + 1) / size;
            double rWeight = ((double)split.rightData().right() - split.rightData().left() + 1) / size;

            double infoGain = curImpurity - lWeight * split.leftData().impurity() - rWeight * split.rightData().impurity();
            if (maxInfoGain < infoGain) {
                maxInfoGain = infoGain;

                res = split;
                res.setInfoGain(maxInfoGain);
            }
            i++;
        }

        return res;
    }

    /** {@inheritDoc} */
    @Override public SparseBitSet calculateOwnershipBitSet(ContinuousSplitInfo<D> s) {
        int l = s.leftData().left();

        SparseBitSet res = new SparseBitSet();

        for (int i = l; i < s.rightData().left(); i++)
            res.set(samples.get(i).getSampleInd());

        return res;
    }

    /** {@inheritDoc} */
    @Override public double[] calculateRegions(IgniteFunction<DoubleStream, Double> regCalc) {
        double[] res = new double[regions.size()];

        int i = 0;

        for (D interval : regions) {
            int l = interval.left();
            int r = interval.right();

            res[i] = regCalc.apply(samples.subList(l, r + 1).stream().mapToDouble(SampleInfo::getLabel));
            i++;
        }

        return res;
    }

    /** {@inheritDoc} */
    @Override public ContinuousFeatureVector<D> performSplit(SparseBitSet bs, int regionIdx, D leftData, D rightData) {
        D info = regions.get(regionIdx);
        int l = info.left();
        int r = info.right();

        sortByBitSet(l, r, bs);

        regions.set(regionIdx, leftData);
        regions.add(regionIdx + 1, rightData);

        return this;
    }

    /** {@inheritDoc} */
    @Override public ContinuousFeatureVector<D> performSplitGeneric(SparseBitSet bs, int regionIdx, RegionInfo leftData,
        RegionInfo rightData) {
        D info = regions.get(regionIdx);
        int l = info.left();
        int r = info.right();

        sortByBitSet(l, r, bs);

        int newLSize = bs.cardinality();

        // TODO: maybe we should optimize here: because we keep extra information for calculations in <D>, we are obliged
        // to do following calculations even though we already have impurity for each region calculated.
        D ld = calc.calculateRegionInfo(samples.subList(l, l + newLSize).stream().mapToDouble(SampleInfo::getLabel), l);
        D rd = calc.calculateRegionInfo(samples.subList(l + newLSize, r + 1).stream().mapToDouble(SampleInfo::getLabel), l + newLSize);

        regions.set(regionIdx, ld);
        regions.add(regionIdx + 1, rd);

        return this;
    }

    /** */
    private void sortByBitSet(int l, int r, SparseBitSet bs) {
        List<SampleInfo> lList = new LinkedList<>();
        List<SampleInfo> rList = new LinkedList<>();

        samples.subList(l, r + 1).stream().forEach(fi -> {
            if (bs.get(fi.getSampleInd()))
                lList.add(fi);
            else
                rList.add(fi);
        });

        int i = 0;
        for (SampleInfo sampleInfo : lList) {
            samples.set(l + i, sampleInfo);
            i++;
        }

        for (SampleInfo sampleInfo : rList) {
            samples.set(l + i, sampleInfo);
            i++;
        }
    }
}
