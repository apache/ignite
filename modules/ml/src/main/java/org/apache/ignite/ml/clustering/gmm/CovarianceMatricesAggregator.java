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
import org.apache.ignite.ml.math.functions.IgniteFunction;
import org.apache.ignite.ml.math.primitives.matrix.Matrix;
import org.apache.ignite.ml.math.primitives.vector.Vector;

public class CovarianceMatricesAggregator implements Serializable {
    private final Vector mean;
    private Matrix weightedSum;
    private int N;

    public CovarianceMatricesAggregator(Vector mean) {
        this.mean = mean;
    }

    private CovarianceMatricesAggregator(Vector mean, Matrix weightedSum, int n) {
        this.mean = mean;
        this.weightedSum = weightedSum;
        this.N = n;
    }

    public void add(Vector x, double pcxi) {
        Matrix deltaCol = x.minus(mean).toMatrix(false);
        Matrix weightedCovComponent = deltaCol.times(deltaCol.transpose()).times(pcxi);
        if (weightedSum == null)
            weightedSum = weightedCovComponent;
        else
            weightedSum = weightedSum.plus(weightedCovComponent);
        N += 1;
    }

    public CovarianceMatricesAggregator plus(CovarianceMatricesAggregator other) {
        A.ensure(this.mean == other.mean, "this.mean == other.mean");

        return new CovarianceMatricesAggregator(
            mean,
            this.weightedSum.plus(other.weightedSum),
            this.N + other.N
        );
    }

    public Matrix covariance(double clusterProb) {
        return weightedSum.divide(N * clusterProb);
    }

    public static IgniteFunction<GmmPartitionData, List<CovarianceMatricesAggregator>> map(int countOfComponents,
        List<Vector> means) {
        return data -> {
            List<CovarianceMatricesAggregator> aggregators = new ArrayList<>();
            for (int i = 0; i < countOfComponents; i++)
                aggregators.add(new CovarianceMatricesAggregator(means.get(i)));

            for (int i = 0; i < data.getXs().size(); i++) {
                for (int c = 0; c < countOfComponents; c++)
                    aggregators.get(c).add(data.getX(i), data.pcxi(c, i));
            }

            return aggregators;
        };
    }

    public static List<CovarianceMatricesAggregator> reduce(List<CovarianceMatricesAggregator> l,
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
}
