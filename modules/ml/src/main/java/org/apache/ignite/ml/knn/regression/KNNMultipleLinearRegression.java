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
import org.apache.ignite.ml.math.Vector;
import org.apache.ignite.ml.math.distances.DistanceMeasure;
import org.apache.ignite.ml.math.exceptions.UnsupportedOperationException;
import org.apache.ignite.ml.structures.LabeledDataset;
import org.apache.ignite.ml.structures.LabeledVector;

/**
 * This class provides kNN Multiple Linear Regression or Locally [weighted] regression (Simple and Weighted versions).
 *
 * <p> This is an instance-based learning method. </p>
 *
 * <ul>
 *     <li>Local means using nearby points (i.e. a nearest neighbors approach).</li>
 *     <li>Weighted means we value points based upon how far away they are.</li>
 *     <li>Regression means approximating a function.</li>
 * </ul>
 */
public class KNNMultipleLinearRegression extends KNNModel {
    /** */
    public KNNMultipleLinearRegression(int k, DistanceMeasure distanceMeasure, KNNStrategy stgy,
        LabeledDataset training) {
        super(k, distanceMeasure, stgy, training);
    }

    /** {@inheritDoc} */
    @Override public Double apply(Vector v) {
        LabeledVector[] neighbors = findKNearestNeighbors(v, true);

        return predictYBasedOn(neighbors, v);
    }

    /** */
    private double predictYBasedOn(LabeledVector[] neighbors, Vector v) {
        switch (stgy) {
            case SIMPLE:
                return simpleRegression(neighbors);
            case WEIGHTED:
                return weightedRegression(neighbors, v);
            default:
                throw new UnsupportedOperationException("Strategy " + stgy.name() + " is not supported");
        }
    }

    /** */
    private double weightedRegression(LabeledVector<Vector, Double>[] neighbors, Vector v) {
        double sum = 0.0;
        double div = 0.0;
        for (int i = 0; i < neighbors.length; i++) {
            double distance = cachedDistances != null ? cachedDistances[i] : distanceMeasure.compute(v, neighbors[i].features());
            sum += neighbors[i].label() * distance;
            div += distance;
        }
        return sum / div;
    }

    /** */
    private double simpleRegression(LabeledVector<Vector, Double>[] neighbors) {
        double sum = 0.0;
        for (LabeledVector<Vector, Double> neighbor : neighbors)
            sum += neighbor.label();
        return sum / (double)k;
    }
}
