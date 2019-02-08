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
import org.apache.ignite.internal.util.typedef.internal.A;
import org.apache.ignite.ml.dataset.Dataset;
import org.apache.ignite.ml.dataset.primitive.context.EmptyContext;
import org.apache.ignite.ml.math.primitives.matrix.Matrix;
import org.apache.ignite.ml.math.primitives.vector.Vector;

/**
 * This class encapsulates statistics aggregation logic for feature vector covariance matrix computation of one GMM
 * component (cluster).
 */
public class CovarianceMatricesAggregator implements Serializable {
    /** Serial version uid. */
    private static final long serialVersionUID = 4163253784526780812L;

    /** Mean vector. */
    private final Vector mean;

    /** Weighted by P(c|xi) sum of (xi - mean) * (xi - mean)^T values. */
    private Matrix weightedSum;

    /** Count of rows. */
    private int N;

    /**
     * Creates an instance of CovarianceMatricesAggregator.
     *
     * @param mean Mean vector.
     */
    CovarianceMatricesAggregator(Vector mean) {
        this.mean = mean;
    }

    /**
     * Creates an instance of CovarianceMatricesAggregator.
     *
     * @param mean Mean vector.
     * @param weightedSum Weighted sums for covariace computation.
     * @param n Count of rows.
     */
    CovarianceMatricesAggregator(Vector mean, Matrix weightedSum, int n) {
        this.mean = mean;
        this.weightedSum = weightedSum;
        this.N = n;
    }

    /**
     * Computes covatiation matrices for feature vector for each GMM component.
     *
     * @param dataset Dataset.
     * @param clusterProbs Probabilities of each GMM component.
     * @param means Means for each GMM component.
     */
    public static List<Matrix> computeCovariances(Dataset<EmptyContext, GmmPartitionData> dataset,
        Vector clusterProbs, Vector[] means) {

        List<CovarianceMatricesAggregator> aggregators = dataset.compute(
            data -> map(data, means),
            CovarianceMatricesAggregator::reduce
        );

        List<Matrix> res = new ArrayList<>();
        for (int i = 0; i < aggregators.size(); i++)
            res.add(aggregators.get(i).covariance(clusterProbs.get(i)));

        return res;
    }

    /**
     * @param x Feature vector (xi).
     * @param pcxi P(c|xi) for GMM component "c" and vector xi.
     */
    void add(Vector x, double pcxi) {
        Matrix deltaCol = x.minus(mean).toMatrix(false);
        Matrix weightedCovComponent = deltaCol.times(deltaCol.transpose()).times(pcxi);
        if (weightedSum == null)
            weightedSum = weightedCovComponent;
        else
            weightedSum = weightedSum.plus(weightedCovComponent);
        N += 1;
    }

    /**
     * @param other Other.
     * @return sum of aggregators.
     */
    CovarianceMatricesAggregator plus(CovarianceMatricesAggregator other) {
        A.ensure(this.mean.equals(other.mean), "this.mean == other.mean");

        return new CovarianceMatricesAggregator(
            mean,
            this.weightedSum.plus(other.weightedSum),
            this.N + other.N
        );
    }

    /**
     * Map stage for covariance computation over dataset.
     *
     * @param data Data partition.
     * @param means Means vector.
     * @return Covariance aggregators.
     */
    static List<CovarianceMatricesAggregator> map(GmmPartitionData data, Vector[] means) {
        int countOfComponents = means.length;

        List<CovarianceMatricesAggregator> aggregators = new ArrayList<>();
        for (int i = 0; i < countOfComponents; i++)
            aggregators.add(new CovarianceMatricesAggregator(means[i]));

        for (int i = 0; i < data.size(); i++) {
            for (int c = 0; c < countOfComponents; c++)
                aggregators.get(c).add(data.getX(i), data.pcxi(c, i));
        }

        return aggregators;
    }

    /**
     * @param clusterProb GMM component probability.
     * @return computed covariance matrix.
     */
    private Matrix covariance(double clusterProb) {
        return weightedSum.divide(N * clusterProb);
    }

    /**
     * Reduce stage for covariance computation over dataset.
     *
     * @param l L.
     * @param r R.
     */
    static List<CovarianceMatricesAggregator> reduce(List<CovarianceMatricesAggregator> l,
        List<CovarianceMatricesAggregator> r) {

        A.ensure(l != null || r != null, "Both partitions cannot equal to null");

        if (l == null)
            return r;
        if (r == null)
            return l;

        A.ensure(l.size() == r.size(), "l.size() == r.size()");
        List<CovarianceMatricesAggregator> res = new ArrayList<>();
        for (int i = 0; i < l.size(); i++)
            res.add(l.get(i).plus(r.get(i)));

        return res;
    }

    /**
     * @return mean vector.
     */
    Vector mean() {
        return mean.copy();
    }

    /**
     * @return weighted sum.
     */
    Matrix weightedSum() {
        return weightedSum.copy();
    }

    /**
     * @return N.
     */
    int N() {
        return N;
    }
}
