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
package org.apache.ignite.ml.knn.regression;

import org.apache.ignite.ml.knn.models.KNNModel;
import org.apache.ignite.ml.knn.models.KNNStrategy;
import org.apache.ignite.ml.math.Matrix;
import org.apache.ignite.ml.math.Vector;
import org.apache.ignite.ml.math.distances.DistanceMeasure;
import org.apache.ignite.ml.structures.LabeledDataset;
import org.apache.ignite.ml.structures.LabeledVector;


public class KNNMultipleLinearRegression extends KNNModel {

    /**
     * @param k               amount of nearest neighbors
     * @param distanceMeasure
     * @param strategy
     * @param training
     */
    public KNNMultipleLinearRegression(int k, DistanceMeasure distanceMeasure, KNNStrategy strategy, LabeledDataset<Matrix, Vector> training) {
        super(k, distanceMeasure, strategy, training);
    }

    /** {@inheritDoc} */
    @Override public Double predict(Vector v) {

        LabeledVector[] neighbors = findKNearestNeighbors(v);
        double classLabel = predictYBasedOn(neighbors, v,  strategy);

        return classLabel;
    }

    private double predictYBasedOn(LabeledVector<Vector, Double>[] neighbors, Vector v, KNNStrategy strategy) {
        double sum = 0.0;
        for (LabeledVector<Vector, Double> neighbor : neighbors) sum += neighbor.label();

        // TODO: add different strategy support with correct calculation and normalization
        return sum/(double)k;
    }
}
