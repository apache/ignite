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

package org.apache.ignite.ml.clustering.gmm;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;
import org.apache.ignite.internal.util.typedef.internal.A;
import org.apache.ignite.ml.dataset.Dataset;
import org.apache.ignite.ml.dataset.primitive.context.EmptyContext;
import org.apache.ignite.ml.math.primitives.vector.Vector;
import org.apache.ignite.ml.math.primitives.vector.VectorUtils;

/**
 * Statistics aggregator for mean values and cluster probabilities computing.
 */
class MeanWithClusterProbAggregator implements Serializable {
    /** Serial version uid. */
    private static final long serialVersionUID = 2700985110021774629L;

    /** Weighted sum of vectors. */
    private Vector weightedXsSum;

    /** P(C|xi) sum. */
    private double pcxiSum;

    /** Aggregated partition data size. */
    private int rowCount;

    /**
     * Create an instance of MeanWithClusterProbAggregator.
     */
    MeanWithClusterProbAggregator() {
        // NO-OP.
    }

    /**
     * Create an instance of MeanWithClusterProbAggregator.
     *
     * @param weightedXsSum Weighted sum of vectors.
     * @param pcxiSum P(c|xi) sum.
     * @param rowCount Count of rows.
     */
    MeanWithClusterProbAggregator(Vector weightedXsSum, double pcxiSum, int rowCount) {
        this.weightedXsSum = weightedXsSum;
        this.pcxiSum = pcxiSum;
        this.rowCount = rowCount;
    }

    /**
     * @return Compute mean value by aggregated data.
     */
    public Vector mean() {
        return weightedXsSum.divide(pcxiSum);
    }

    /**
     * @return Compute cluster probability by aggreated data.
     */
    public double clusterProb() {
        return pcxiSum / rowCount;
    }

    /**
     * Aggregates statistics for means and cluster probabilities computing given dataset.
     *
     * @param dataset Dataset.
     * @param countOfComponents Count of componets.
     */
    public static AggregatedStats aggreateStats(Dataset<EmptyContext, GmmPartitionData> dataset, int countOfComponents) {
        return new AggregatedStats(dataset.compute(
            data -> map(data, countOfComponents),
            MeanWithClusterProbAggregator::reduce
        ));
    }

    /**
     * Add vector to statistics.
     *
     * @param x Vector.
     * @param pcxi P(c|xi).
     */
    void add(Vector x, double pcxi) {
        A.ensure(pcxi >= 0 && pcxi <= 1., "pcxi >= 0 && pcxi <= 1.");

        Vector weightedVector = x.times(pcxi);
        if (weightedXsSum == null)
            weightedXsSum = weightedVector;
        else
            weightedXsSum = weightedXsSum.plus(weightedVector);

        pcxiSum += pcxi;
        rowCount += 1;
    }

    /**
     * @param other Other.
     * @return Sum of aggregators.
     */
    MeanWithClusterProbAggregator plus(MeanWithClusterProbAggregator other) {
        return new MeanWithClusterProbAggregator(
            weightedXsSum.plus(other.weightedXsSum),
            pcxiSum + other.pcxiSum,
            rowCount + other.rowCount
        );
    }

    /**
     * Map stage for statistics aggregation.
     *
     * @param data Partition data.
     * @param countOfComponents Count of components.
     * @return Aggregated statistics.
     */
    static List<MeanWithClusterProbAggregator> map(GmmPartitionData data, int countOfComponents) {
        List<MeanWithClusterProbAggregator> aggregators = new ArrayList<>();
        for (int i = 0; i < countOfComponents; i++)
            aggregators.add(new MeanWithClusterProbAggregator());

        for (int i = 0; i < data.size(); i++) {
            for (int c = 0; c < countOfComponents; c++)
                aggregators.get(c).add(data.getX(i), data.pcxi(c, i));
        }

        return aggregators;
    }

    /**
     * Reduce stage for statistics aggregation.
     *
     * @param l Reft part.
     * @param r Right part.
     * @return Sum of statistics for each cluster.
     */
    static List<MeanWithClusterProbAggregator> reduce(List<MeanWithClusterProbAggregator> l,
        List<MeanWithClusterProbAggregator> r) {
        A.ensure(l != null || r != null, "Both partitions cannot equal to null");

        if (l == null || l.isEmpty())
            return r;
        if (r == null || r.isEmpty())
            return l;

        A.ensure(l.size() == r.size(), "l.size() == r.size()");
        List<MeanWithClusterProbAggregator> res = new ArrayList<>();
        for (int i = 0; i < l.size(); i++)
            res.add(l.get(i).plus(r.get(i)));

        return res;
    }

    /**
     * Computed cluster probabilities and means.
     */
    public static class AggregatedStats {
        /** Cluster probs. */
        private final Vector clusterProbs;

        /** Means. */
        private final List<Vector> means;

        /**
         * Creates an instance of AggregatedStats.
         *
         * @param stats Statistics.
         */
        private AggregatedStats(List<MeanWithClusterProbAggregator> stats) {
            clusterProbs = VectorUtils.of(stats.stream()
                .mapToDouble(MeanWithClusterProbAggregator::clusterProb)
                .toArray()
            );

            means = stats.stream()
                .map(MeanWithClusterProbAggregator::mean)
                .collect(Collectors.toList());
        }

        /**
         * @return Clusters probabilities.
         */
        public Vector clusterProbabilities() {
            return clusterProbs;
        }

        /**
         * @return Means.
         */
        public List<Vector> means() {
            return means;
        }
    }
}
