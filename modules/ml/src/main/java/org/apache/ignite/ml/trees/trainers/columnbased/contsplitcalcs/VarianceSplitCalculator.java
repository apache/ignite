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

package org.apache.ignite.ml.trees.trainers.columnbased.contsplitcalcs;

import java.util.Iterator;
import java.util.PrimitiveIterator;
import java.util.stream.DoubleStream;
import java.util.stream.Stream;
import org.apache.ignite.ml.trees.ContinuousRegionInfo;
import org.apache.ignite.ml.trees.ContinuousSplitCalculator;
import org.apache.ignite.ml.trees.trainers.columnbased.vectors.ContinuousSplitInfo;
import org.apache.ignite.ml.trees.trainers.columnbased.vectors.SampleInfo;
import org.apache.ignite.ml.trees.trainers.columnbased.vectors.SplitInfo;

/**
 * Calculator of variance in a given region.
 */
public class VarianceSplitCalculator implements ContinuousSplitCalculator<VarianceSplitCalculator.VarianceData> {
    /**
     * Data used in variance calculations.
     */
    public static class VarianceData extends ContinuousRegionInfo {
        /** Mean value in a given region. */
        double mean;

        /**
         * @param left Left bound of this region.
         * @param right Right bound of this region.
         * @param var Variance in this region.
         * @param mean Mean value in this region.
         */
        VarianceData(int left, int right, double var, double mean) {
            super(var, left, right);
            this.mean = mean;
        }
    }

    /** {@inheritDoc} */
    @Override public VarianceData calculateRegionInfo(DoubleStream s, int l) {
        PrimitiveIterator.OfDouble itr = s.iterator();
        int i = 0;

        double mean = 0.0;
        double m2 = 0.0;

        while (itr.hasNext()) {
            i++;
            double x = itr.next();
            double delta = x - mean;
            mean += delta / i;
            double delta2 = x - mean;
            m2 += delta * delta2;
        }

        return new VarianceData(l, l + i - 1, m2 / i, mean);
    }

    /** {@inheritDoc} */
    @Override public SplitInfo<VarianceData> splitRegion(Stream<SampleInfo> s, int regionIdx, VarianceData d) {
        int size = d.right() - d.left() + 1;

        double lm2 = 0.0;
        double rm2 = d.impurity() * size;
        int lSize = size;

        double lMean = 0.0;
        double rMean = d.mean;

        Iterator<SampleInfo> itr = s.iterator();

        double minImpurity = d.impurity();
        double curThreshold;
        double curImpurity;
        double threshold = Double.NEGATIVE_INFINITY;

        int i = 0;
        SampleInfo next = itr.next();
        double[] lrImps = new double[] {lm2, rm2, lMean, rMean};

        do {
            // Process all values equal to prev.
            while (itr.hasNext()) {
                i++;
                lrImps = moveLeft(next.getLabel(), lrImps[2], i, lrImps[0], lrImps[3], size - i, lrImps[1]);
                curImpurity = (1.0 / size) * (lrImps[0] + lrImps[1]);
                curThreshold = next.getVal();

                if (next.getVal() != (next = itr.next()).getVal()) {
                    if (curImpurity < minImpurity) {
                        lSize = i;

                        lm2 = lrImps[0];
                        rm2 = lrImps[1];

                        lMean = lrImps[2];
                        rMean = lrImps[3];

                        minImpurity = curImpurity;
                        threshold = curThreshold;
                    }

                    break;
                }
            }
        }
        while (itr.hasNext());

        i++;
        lrImps = moveLeft(next.getLabel(), lrImps[2], i, lrImps[0], lrImps[3], size - i, lrImps[1]);
        curImpurity = (1.0 / size) * (lrImps[0] + lrImps[1]);
        curThreshold = next.getVal();

        if (curImpurity < minImpurity) {
            lSize = i;

            lm2 = lrImps[0];
            rm2 = lrImps[1];

            lMean = lrImps[2];
            rMean = lrImps[3];

            threshold = curThreshold;
        }

        if (lSize == size)
            return null;

        VarianceData lData = new VarianceData(d.left(), d.left() + lSize - 1, lm2 / (lSize != 0 ? lSize : 1), lMean);
        VarianceData rData = new VarianceData(d.left() + lSize, d.right(), rm2 / ((size - lSize) != 0 ? (size - lSize) : 1), rMean);

        return new ContinuousSplitInfo<>(regionIdx, threshold, lData, rData);
    }

    /**
     * Add point to the left interval and remove it from the right interval and calculate necessary statistics on
     * intervals with new bounds.
     */
    private double[] moveLeft(double x, double lMean, int lSize, double lm2, double rMean, int rSize, double rm2) {
        double[] res = new double[4];

        // We add point to the left interval.
        double lDelta = x - lMean;
        double lMeanNew = lMean + lDelta / lSize;
        double lm2New = lm2 + lDelta * (x - lMeanNew);

        // We remove point from the right interval. lSize + 1 is the size of right interval before removal.
        double rMeanNew = rSize != 0 ? (rMean * (rSize + 1) - x) / rSize : 0;
        double rm2New = rSize != 0 ? rm2 - (x - rMean) * (x - rMeanNew) : 0;

        res[0] = lm2New;
        res[1] = rm2New;

        res[2] = lMeanNew;
        res[3] = rMeanNew;

        return res;
    }
}
