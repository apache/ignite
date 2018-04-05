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

package org.apache.ignite.ml.tree;

import java.util.function.Predicate;
import org.apache.ignite.ml.dataset.Dataset;
import org.apache.ignite.ml.dataset.primitive.context.EmptyContext;
import org.apache.ignite.ml.dataset.primitive.data.SimpleLabeledDatasetData;
import org.apache.ignite.ml.tree.impurity.ImpurityMeasureCalculator;
import org.apache.ignite.ml.tree.impurity.mse.MSEImpurityMeasure;
import org.apache.ignite.ml.tree.impurity.mse.MSEImpurityMeasureCalculator;

/**
 * Decision tree regressor based on distributed decision tree trainer that allows to fit trees using row-partitioned
 * dataset.
 */
public class DecisionTreeRegressor extends DecisionTree<MSEImpurityMeasure> {
    /**
     * Constructs a new decision tree regressor.
     *
     * @param maxDeep Max tree deep.
     * @param minImpurityDecrease Min impurity decrease.
     */
    public DecisionTreeRegressor(int maxDeep, double minImpurityDecrease) {
        super(maxDeep, minImpurityDecrease);
    }

    /** {@inheritDoc} */
    @Override DecisionTreeLeafNode createLeafNode(Dataset<EmptyContext, SimpleLabeledDatasetData> dataset,
        Predicate<double[]> pred) {
        double[] aa = dataset.compute(part -> {
            double mean = 0;
            int cnt = 0;
            double[][] features = convert(part.getFeatures(), part.getRows());
            for (int i = 0; i < part.getRows(); i++) {
                if (pred.test(features[i])) {
                    mean += part.getLabels()[i];
                    cnt++;
                }
            }

            if (cnt != 0) {
                mean = mean / cnt;
                return new double[]{mean, cnt};
            }

            return null;
        }, this::reduce);

        return aa != null ? new DecisionTreeLeafNode(aa[0]) : null;
    }

    /** {@inheritDoc} */
    @Override ImpurityMeasureCalculator<MSEImpurityMeasure> getImpurityMeasureCalculator(Dataset<EmptyContext, SimpleLabeledDatasetData> dataset) {
        return new MSEImpurityMeasureCalculator();
    }

    private double[] reduce(double[] a, double[] b) {
        if (a == null)
            return b;
        else if (b == null)
            return a;
        else {
            double aMean = a[0];
            double aCnt = a[1];
            double bMean = b[0];
            double bCnt = b[1];

            double mean = (aMean * aCnt + bMean * bCnt) / (aCnt + bCnt);

            return new double[]{mean, aCnt + bCnt};
        }
    }
}
