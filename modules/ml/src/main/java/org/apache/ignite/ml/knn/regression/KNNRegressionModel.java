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

import java.util.List;
import org.apache.ignite.ml.dataset.Dataset;
import org.apache.ignite.ml.dataset.primitive.context.EmptyContext;
import org.apache.ignite.ml.knn.classification.KNNClassificationModel;
import org.apache.ignite.ml.math.exceptions.UnsupportedOperationException;
import org.apache.ignite.ml.math.primitives.vector.Vector;
import org.apache.ignite.ml.structures.LabeledDataset;
import org.apache.ignite.ml.structures.LabeledVector;
import org.apache.ignite.ml.util.ModelTrace;

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
public class KNNRegressionModel extends KNNClassificationModel {
    /** */
    private static final long serialVersionUID = -721836321291120543L;

    /**
     * Builds the model via prepared dataset.
     * @param dataset Specially prepared object to run algorithm over it.
     */
    public KNNRegressionModel(Dataset<EmptyContext, LabeledDataset<Double, LabeledVector>> dataset) {
        super(dataset);
    }

    /** {@inheritDoc} */
    @Override public Double apply(Vector v) {
        List<LabeledVector> neighbors = findKNearestNeighbors(v);

        return predictYBasedOn(neighbors, v);
    }

    /** */
    private double predictYBasedOn(List<LabeledVector> neighbors, Vector v) {
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
    private double weightedRegression(List<LabeledVector> neighbors, Vector v) {
        double sum = 0.0;
        double div = 0.0;
        for (LabeledVector<Vector, Double> neighbor : neighbors) {
            double distance = distanceMeasure.compute(v, neighbor.features());
            sum += neighbor.label() * distance;
            div += distance;
        }
        return sum / div;
    }

    /** */
    private double simpleRegression(List<LabeledVector> neighbors) {
        double sum = 0.0;
        for (LabeledVector<Vector, Double> neighbor : neighbors)
            sum += neighbor.label();
        return sum / (double)k;
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return toString(false);
    }

    /** {@inheritDoc} */
    @Override public String toString(boolean pretty) {
        return ModelTrace.builder("KNNClassificationModel", pretty)
            .addField("k", String.valueOf(k))
            .addField("measure", distanceMeasure.getClass().getSimpleName())
            .addField("strategy", stgy.name())
            .toString();
    }
}
