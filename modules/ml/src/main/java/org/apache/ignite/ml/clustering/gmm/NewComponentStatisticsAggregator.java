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
import org.apache.ignite.internal.util.typedef.internal.A;
import org.apache.ignite.ml.dataset.Dataset;
import org.apache.ignite.ml.dataset.primitive.context.EmptyContext;
import org.apache.ignite.ml.math.primitives.vector.Vector;

/**
 * Class for aggregate statistics for finding new mean for GMM.
 */
public class NewComponentStatisticsAggregator implements Serializable {
    /** Serial version uid. */
    private static final long serialVersionUID = 6748270328889375005L;

    /** Total row count in dataset. */
    private long totalRowCount;

    /** Row count for new cluster. */
    private long rowCountForNewCluster;

    /** Sum of anomalies vectors. */
    private Vector sumOfAnomalies;

    /**
     * Creates an instance of NewComponentStatisticsAggregator.
     *
     * @param totalRowCount Total row count in dataset.
     * @param rowCountForNewCluster Row count for new cluster.
     * @param sumOfAnomalies Sum of anomalies.
     */
    public NewComponentStatisticsAggregator(long totalRowCount, long rowCountForNewCluster, Vector sumOfAnomalies) {
        this.totalRowCount = totalRowCount;
        this.rowCountForNewCluster = rowCountForNewCluster;
        this.sumOfAnomalies = sumOfAnomalies;
    }

    /**
     * Creates an instance of NewComponentStatisticsAggregator.
     */
    public NewComponentStatisticsAggregator() {
    }

    /**
     * @return Mean of anomalies.
     */
    public Vector mean() {
        return sumOfAnomalies.divide(rowCountForNewCluster);
    }

    /**
     * @return Row count for new cluster.
     */
    public long rowCountForNewCluster() {
        return rowCountForNewCluster;
    }

    /**
     * @return Total count of rows in partition/dataset.
     */
    public long totalRowCount() {
        return totalRowCount;
    }

    /**
     * Compute statistics for new mean for GMM.
     *
     * @param dataset Dataset.
     * @param maxXsProb Max likelihood between all xs.
     * @param maxProbDivergence Max probability divergence between maximum value and others.
     * @param currentModel Current model.
     * @return Aggregated statistics for new mean.
     */
    static NewComponentStatisticsAggregator computeNewMean(Dataset<EmptyContext, GmmPartitionData> dataset,
        double maxXsProb, double maxProbDivergence, GmmModel currentModel) {

        return dataset.compute(
            data -> computeNewMeanMap(data, maxXsProb, maxProbDivergence, currentModel),
            NewComponentStatisticsAggregator::computeNewMeanReduce
        );
    }

    /**
     * Map stage for new mean computing.
     *
     * @param data Data.
     * @param maxXsProb Max xs prob.
     * @param maxProbDivergence Max prob divergence.
     * @param currentModel Current model.
     * @return Aggregator for partition.
     */
    static NewComponentStatisticsAggregator computeNewMeanMap(GmmPartitionData data, double maxXsProb,
        double maxProbDivergence, GmmModel currentModel) {

        NewComponentStatisticsAggregator adder = new NewComponentStatisticsAggregator();
        for (int i = 0; i < data.size(); i++) {
            Vector x = data.getX(i);
            adder.add(x, currentModel.prob(x) < (maxXsProb / maxProbDivergence));
        }
        return adder;
    }

    /**
     * Adds vector to statistics.
     *
     * @param x Vector from dataset.
     * @param isAnomaly True if vector is anomaly.
     */
    void add(Vector x, boolean isAnomaly) {
        if (isAnomaly) {
            if (sumOfAnomalies == null)
                sumOfAnomalies = x.copy();
            else
                sumOfAnomalies = sumOfAnomalies.plus(x);

            rowCountForNewCluster += 1;
        }

        totalRowCount += 1;
    }

    /**
     * Reduce stage for new mean computing.
     *
     * @param left Left argument of reduce.
     * @param right Right argument of reduce.
     * @return Sum of aggregators.
     */
    static NewComponentStatisticsAggregator computeNewMeanReduce(NewComponentStatisticsAggregator left,
        NewComponentStatisticsAggregator right) {
        A.ensure(left != null || right != null, "left != null || right != null");

        if (left == null)
            return right;
        else if (right == null)
            return left;
        else
            return left.plus(right);
    }

    /**
     * @param other Other aggregator.
     * @return Sum of aggregators.
     */
    NewComponentStatisticsAggregator plus(NewComponentStatisticsAggregator other) {
        return new NewComponentStatisticsAggregator(
            totalRowCount + other.totalRowCount,
            rowCountForNewCluster + other.rowCountForNewCluster,
            sumOfAnomalies.plus(other.sumOfAnomalies)
        );
    }
}
