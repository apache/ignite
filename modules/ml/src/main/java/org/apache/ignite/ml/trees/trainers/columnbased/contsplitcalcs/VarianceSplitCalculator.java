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

import org.apache.ignite.ml.trees.ContinuousRegionInfo;
import org.apache.ignite.ml.trees.ContinuousSplitCalculator;
import org.apache.ignite.ml.trees.trainers.columnbased.vectors.ContinuousSplitInfo;
import org.apache.ignite.ml.trees.trainers.columnbased.vectors.SplitInfo;

import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.util.PrimitiveIterator;
import java.util.stream.DoubleStream;

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
         * @param var Variance in this region.
         * @param size Size of data for which variance is calculated.
         * @param mean Mean value in this region.
         */
        public VarianceData(double var, int size, double mean) {
            super(var, size);
            this.mean = mean;
        }

        /**
         * No-op constructor. For serialization/deserialization.
         */
        public VarianceData() {
            // No-op.
        }

        /** {@inheritDoc} */
        @Override public void writeExternal(ObjectOutput out) throws IOException {
            super.writeExternal(out);
            out.writeDouble(mean);
        }

        /** {@inheritDoc} */
        @Override public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {
            super.readExternal(in);
            mean = in.readDouble();
        }

        /**
         * Returns mean.
         */
        public double mean() {
            return mean;
        }
    }

    /** {@inheritDoc} */
    @Override public VarianceData calculateRegionInfo(DoubleStream s, int size) {
        PrimitiveIterator.OfDouble itr = s.iterator();
        int i = 0;

        double mean = 0.0;
        double m2 = 0.0;

        // Here we calculate variance and mean by incremental computation.
        while (itr.hasNext()) {
            i++;
            double x = itr.next();
            double delta = x - mean;
            mean += delta / i;
            double delta2 = x - mean;
            m2 += delta * delta2;
        }

        return new VarianceData(m2 / i, size, mean);
    }

    /** {@inheritDoc} */
    @Override public SplitInfo<VarianceData> splitRegion(Integer[] s, double[] values, double[] labels, int regionIdx,
        VarianceData d) {
        int size = d.getSize();

        double lm2 = 0.0;
        double rm2 = d.impurity() * size;
        int lSize = size;

        double lMean = 0.0;
        double rMean = d.mean;

        double minImpurity = d.impurity() * size;
        double curThreshold;
        double curImpurity;
        double threshold = Double.NEGATIVE_INFINITY;

        int i = 0;
        int nextIdx = s[0];
        i++;
        double[] lrImps = new double[] {lm2, rm2, lMean, rMean};

        do {
            // Process all values equal to prev.
            while (i < s.length) {
                moveLeft(labels[nextIdx], lrImps[2], i, lrImps[0], lrImps[3], size - i, lrImps[1], lrImps);
                curImpurity = (lrImps[0] + lrImps[1]);
                curThreshold = values[nextIdx];

                if (values[nextIdx] != values[(nextIdx = s[i++])]) {
                    if (curImpurity < minImpurity) {
                        lSize = i - 1;

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
        while (i < s.length - 1);

        if (lSize == size)
            return null;

        VarianceData lData = new VarianceData(lm2 / (lSize != 0 ? lSize : 1), lSize, lMean);
        int rSize = size - lSize;
        VarianceData rData = new VarianceData(rm2 / (rSize != 0 ? rSize : 1), rSize, rMean);

        return new ContinuousSplitInfo<>(regionIdx, threshold, lData, rData);
    }

    /**
     * Add point to the left interval and remove it from the right interval and calculate necessary statistics on
     * intervals with new bounds.
     */
    private void moveLeft(double x, double lMean, int lSize, double lm2, double rMean, int rSize, double rm2,
        double[] data) {
        // We add point to the left interval.
        double lDelta = x - lMean;
        double lMeanNew = lMean + lDelta / lSize;
        double lm2New = lm2 + lDelta * (x - lMeanNew);

        // We remove point from the right interval. lSize + 1 is the size of right interval before removal.
        double rMeanNew = (rMean * (rSize + 1) - x) / rSize;
        double rm2New = rm2 - (x - rMean) * (x - rMeanNew);

        data[0] = lm2New;
        data[1] = rm2New;

        data[2] = lMeanNew;
        data[3] = rMeanNew;
    }
}
