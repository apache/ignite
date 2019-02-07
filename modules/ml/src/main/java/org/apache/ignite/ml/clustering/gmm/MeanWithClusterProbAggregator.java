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
import org.apache.ignite.ml.math.functions.IgniteFunction;
import org.apache.ignite.ml.math.primitives.vector.Vector;

class MeanWithClusterProbAggregator implements Serializable {
    private Vector weightedXsSum;
    private double pcxiSum;
    private int N;

    private MeanWithClusterProbAggregator() {
    }

    private MeanWithClusterProbAggregator(Vector weightedXsSum, double pcxiSum, int N) {
        this.weightedXsSum = weightedXsSum;
        this.pcxiSum = pcxiSum;
        this.N = N;
    }

    public Vector mean() {
        return weightedXsSum.divide(pcxiSum);
    }

    public double clusterProb() {
        return pcxiSum / N;
    }

    public static List<MeanWithClusterProbAggregator> computeMeans(Dataset<EmptyContext, GmmPartitionData> dataset,
        int countOfComponents) {

        return dataset.compute(
            map(countOfComponents),
            MeanWithClusterProbAggregator::reduce
        );
    }

    void add(Vector x, double pcxi) {
        Vector weightedVector = x.times(pcxi);
        if (weightedXsSum == null)
            weightedXsSum = weightedVector;
        else
            weightedXsSum = weightedXsSum.plus(weightedVector);

        pcxiSum += pcxi;
        N += 1;
    }

    MeanWithClusterProbAggregator plus(MeanWithClusterProbAggregator other) {
        return new MeanWithClusterProbAggregator(
            weightedXsSum.plus(other.weightedXsSum),
            pcxiSum + other.pcxiSum,
            N + other.N
        );
    }

    static IgniteFunction<GmmPartitionData, List<MeanWithClusterProbAggregator>> map(int countOfComponents) {
        return data -> {
            List<MeanWithClusterProbAggregator> aggregators = new ArrayList<>();
            for (int i = 0; i < countOfComponents; i++)
                aggregators.add(new MeanWithClusterProbAggregator());

            for (int i = 0; i < data.size(); i++) {
                for (int c = 0; c < countOfComponents; c++)
                    aggregators.get(c).add(data.getX(i), data.pcxi(c, i));
            }

            return aggregators;
        };
    }

    static List<MeanWithClusterProbAggregator> reduce(List<MeanWithClusterProbAggregator> l,
        List<MeanWithClusterProbAggregator> r) {
        A.ensure(l != null || r != null, "Both partitions cannot equal to null");

        if (l == null)
            return r;
        if (r == null)
            return l;

        A.ensure(l.size() == r.size(), "l.size() == r.size()");
        List<MeanWithClusterProbAggregator> res = new ArrayList<>();
        for (int i = 0; i < l.size(); i++)
            res.add(l.get(i).plus(r.get(i)));

        return res;
    }
}
