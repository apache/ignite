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
import java.util.Comparator;
import org.apache.ignite.lang.IgniteBiTuple;
import org.apache.ignite.ml.trees.ContinuousRegionInfo;
import org.apache.ignite.ml.trees.ContinuousSplitCalculator;
import org.apache.ignite.ml.trees.RegionInfo;
import org.apache.ignite.ml.trees.trainers.columnbased.RegionProjection;

import static org.apache.ignite.ml.trees.trainers.columnbased.vectors.Utils.splitByBitSet;

/**
 * Container of projection of samples on continuous feature.
 *
 * @param <D> Information about regions. Designed to contain information which will make computations of impurity
 * optimal.
 */
public class ContinuousFeatureProcessor<D extends ContinuousRegionInfo> implements
    FeatureProcessor<D, ContinuousSplitInfo<D>> {
    /** ContinuousSplitCalculator used for calculating of best split of each region. */
    private final ContinuousSplitCalculator<D> calc;

    /**
     * @param splitCalc Calculator used for calculating splits.
     */
    public ContinuousFeatureProcessor(ContinuousSplitCalculator<D> splitCalc) {
        this.calc = splitCalc;
    }

    /** {@inheritDoc} */
    @Override public SplitInfo<D> findBestSplit(RegionProjection<D> ri, double[] values, double[] labels, int regIdx) {
        SplitInfo<D> res = calc.splitRegion(ri.sampleIndexes(), values, labels, regIdx, ri.data());

        if (res == null)
            return null;

        double lWeight = (double)res.leftData.getSize() / ri.sampleIndexes().length;
        double rWeight = (double)res.rightData.getSize() / ri.sampleIndexes().length;

        double infoGain = ri.data().impurity() - lWeight * res.leftData().impurity() - rWeight * res.rightData().impurity();
        res.setInfoGain(infoGain);

        return res;
    }

    /** {@inheritDoc} */
    @Override public RegionProjection<D> createInitialRegion(Integer[] samples, double[] values, double[] labels) {
        Arrays.sort(samples, Comparator.comparingDouble(s -> values[s]));
        return new RegionProjection<>(samples, calc.calculateRegionInfo(Arrays.stream(labels), samples.length), 0);
    }

    /** {@inheritDoc} */
    @Override public SparseBitSet calculateOwnershipBitSet(RegionProjection<D> reg, double[] values,
        ContinuousSplitInfo<D> s) {
        SparseBitSet res = new SparseBitSet();

        for (int i = 0; i < s.leftData().getSize(); i++)
            res.set(reg.sampleIndexes()[i]);

        return res;
    }

    /** {@inheritDoc} */
    @Override public IgniteBiTuple<RegionProjection, RegionProjection> performSplit(SparseBitSet bs,
        RegionProjection<D> reg, D leftData, D rightData) {
        int lSize = leftData.getSize();
        int rSize = rightData.getSize();
        int depth = reg.depth();

        IgniteBiTuple<Integer[], Integer[]> lrSamples = splitByBitSet(lSize, rSize, reg.sampleIndexes(), bs);

        RegionProjection<D> left = new RegionProjection<>(lrSamples.get1(), leftData, depth + 1);
        RegionProjection<D> right = new RegionProjection<>(lrSamples.get2(), rightData, depth + 1);

        return new IgniteBiTuple<>(left, right);
    }

    /** {@inheritDoc} */
    @Override public IgniteBiTuple<RegionProjection, RegionProjection> performSplitGeneric(SparseBitSet bs,
        double[] labels, RegionProjection<D> reg, RegionInfo leftData, RegionInfo rightData) {
        int lSize = bs.cardinality();
        int rSize = reg.sampleIndexes().length - lSize;
        int depth = reg.depth();

        IgniteBiTuple<Integer[], Integer[]> lrSamples = splitByBitSet(lSize, rSize, reg.sampleIndexes(), bs);

        D ld = calc.calculateRegionInfo(Arrays.stream(lrSamples.get1()).mapToDouble(s -> labels[s]), lSize);
        D rd = calc.calculateRegionInfo(Arrays.stream(lrSamples.get2()).mapToDouble(s -> labels[s]), rSize);

        return new IgniteBiTuple<>(new RegionProjection<>(lrSamples.get1(), ld, depth + 1), new RegionProjection<>(lrSamples.get2(), rd, depth + 1));
    }
}
