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

import it.unimi.dsi.fastutil.doubles.Double2IntArrayMap;
import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.util.HashMap;
import java.util.Map;
import java.util.PrimitiveIterator;
import java.util.stream.DoubleStream;
import org.apache.ignite.ml.trees.ContinuousRegionInfo;
import org.apache.ignite.ml.trees.ContinuousSplitCalculator;
import org.apache.ignite.ml.trees.trainers.columnbased.vectors.ContinuousSplitInfo;
import org.apache.ignite.ml.trees.trainers.columnbased.vectors.SplitInfo;

/**
 * Calculator for Gini impurity.
 */
public class GiniSplitCalculator implements ContinuousSplitCalculator<GiniSplitCalculator.GiniData> {
    /** Mapping assigning index to each member value */
    private final Map<Double, Integer> mapping = new Double2IntArrayMap();

    /**
     * Create Gini split calculator from labels.
     *
     * @param labels Labels.
     */
    public GiniSplitCalculator(double[] labels) {
        int i = 0;

        for (double label : labels) {
            if (!mapping.containsKey(label)) {
                mapping.put(label, i);
                i++;
            }
        }
    }

    /** {@inheritDoc} */
    @Override public GiniData calculateRegionInfo(DoubleStream s, int l) {
        PrimitiveIterator.OfDouble itr = s.iterator();

        Map<Double, Integer> m = new HashMap<>();

        int size = 0;

        while (itr.hasNext()) {
            size++;
            m.compute(itr.next(), (a, i) -> i != null ? i + 1 : 1);
        }

        double c2 = m.values().stream().mapToDouble(v -> v * v).sum();

        int[] cnts = new int[mapping.size()];

        m.forEach((key, value) -> cnts[mapping.get(key)] = value);

        return new GiniData(size != 0 ? 1 - c2 / (size * size) : 0.0, size, cnts, c2);
    }

    /** {@inheritDoc} */
    @Override public SplitInfo<GiniData> splitRegion(Integer[] s, double[] values, double[] labels, int regionIdx,
        GiniData d) {
        int size = d.getSize();

        double lg = 0.0;
        double rg = d.impurity();

        double lc2 = 0.0;
        double rc2 = d.c2;
        int lSize = 0;

        double minImpurity = d.impurity() * size;
        double curThreshold;
        double curImpurity;
        double threshold = Double.NEGATIVE_INFINITY;

        int i = 0;
        int nextIdx = s[0];
        i++;
        double[] lrImps = new double[] {0.0, d.impurity(), lc2, rc2};

        int[] lMapCur = new int[d.counts().length];
        int[] rMapCur = new int[d.counts().length];

        System.arraycopy(d.counts(), 0, rMapCur, 0, d.counts().length);

        int[] lMap = new int[d.counts().length];
        int[] rMap = new int[d.counts().length];

        System.arraycopy(d.counts(), 0, rMap, 0, d.counts().length);

        do {
            // Process all values equal to prev.
            while (i < s.length) {
                moveLeft(labels[nextIdx], i, size - i, lMapCur, rMapCur, lrImps);
                curImpurity = (i * lrImps[0] + (size - i) * lrImps[1]);
                curThreshold = values[nextIdx];

                if (values[nextIdx] != values[(nextIdx = s[i++])]) {
                    if (curImpurity < minImpurity) {
                        lSize = i - 1;

                        lg = lrImps[0];
                        rg = lrImps[1];

                        lc2 = lrImps[2];
                        rc2 = lrImps[3];

                        System.arraycopy(lMapCur, 0, lMap, 0, lMapCur.length);
                        System.arraycopy(rMapCur, 0, rMap, 0, rMapCur.length);

                        minImpurity = curImpurity;
                        threshold = curThreshold;
                    }

                    break;
                }
            }
        }
        while (i < s.length - 1);

        if (lSize == size || lSize == 0)
            return null;

        GiniData lData = new GiniData(lg, lSize, lMap, lc2);
        int rSize = size - lSize;
        GiniData rData = new GiniData(rg, rSize, rMap, rc2);

        return new ContinuousSplitInfo<>(regionIdx, threshold, lData, rData);
    }

    /**
     * Add point to the left interval and remove it from the right interval and calculate necessary statistics on
     * intervals with new bounds.
     */
    private void moveLeft(double x, int lSize, int rSize, int[] lMap, int[] rMap, double[] data) {
        double lc2 = data[2];
        double rc2 = data[3];

        Integer idx = mapping.get(x);

        int cxl = lMap[idx];
        int cxr = rMap[idx];

        lc2 += 2 * cxl + 1;
        rc2 -= 2 * cxr - 1;

        lMap[idx] += 1;
        rMap[idx] -= 1;

        data[0] = 1 - lc2 / (lSize * lSize);
        data[1] = 1 - rc2 / (rSize * rSize);

        data[2] = lc2;
        data[3] = rc2;
    }

    /**
     * Data used for gini impurity calculations.
     */
    public static class GiniData extends ContinuousRegionInfo {
        /** Sum of squares of counts of each label. */
        private double c2;

        /** Counts of each label. On i-th position there is count of label which is mapped to index i. */
        private int[] m;

        /**
         * Create Gini data.
         *
         * @param impurity Impurity (i.e. Gini impurity).
         * @param size Count of samples.
         * @param m Counts of each label.
         * @param c2 Sum of squares of counts of each label.
         */
        public GiniData(double impurity, int size, int[] m, double c2) {
            super(impurity, size);
            this.m = m;
            this.c2 = c2;
        }

        /**
         * No-op constructor for serialization/deserialization..
         */
        public GiniData() {
            // No-op.
        }

        /** Get counts of each label. */
        public int[] counts() {
            return m;
        }

        /** {@inheritDoc} */
        @Override public void writeExternal(ObjectOutput out) throws IOException {
            super.writeExternal(out);
            out.writeDouble(c2);
            out.writeInt(m.length);
            for (int i : m)
                out.writeInt(i);

        }

        /** {@inheritDoc} */
        @Override public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {
            super.readExternal(in);

            c2 = in.readDouble();
            int size = in.readInt();
            m = new int[size];

            for (int i = 0; i < size; i++)
                m[i] = in.readInt();
        }
    }
}
