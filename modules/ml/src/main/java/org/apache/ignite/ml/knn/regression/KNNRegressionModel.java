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
import org.apache.ignite.ml.knn.KNNModel;
import org.apache.ignite.ml.knn.utils.indices.SpatialIndex;
import org.apache.ignite.ml.math.distances.DistanceMeasure;
import org.apache.ignite.ml.math.primitives.vector.Vector;
import org.apache.ignite.ml.structures.LabeledVector;

/**
 * KNN regression model. Be aware that this model is linked with cluster environment it's been built on and can't
 * be saved or used in other places. Under the hood it keeps {@link Dataset} that consists of a set of resources
 * allocated across the cluster.
 */
public class KNNRegressionModel extends KNNModel<Double> {
    /** Regression predictor. */
    private final KNNRegressionPredictor predictor;

    /**
     * Constructs a new instance of KNN regression model.
     *
     * @param dataset Dataset with {@link SpatialIndex} as a partition data.
     * @param distanceMeasure Distance measure.
     * @param k Number of neighbours.
     * @param weighted Weighted or not.
     */
    KNNRegressionModel(Dataset<EmptyContext, SpatialIndex<Double>> dataset, DistanceMeasure distanceMeasure, int k,
        boolean weighted) {
        super(dataset, distanceMeasure, k, weighted);
        predictor = weighted ? new KNNRegressionWeightedPredictor() : new KNNRegressionSimplePredictor();
    }

    /** {@inheritDoc} */
    @Override public Double predict(Vector input) {
        List<LabeledVector<Double>> neighbors = findKClosest(k, input);

        return predictor.predict(neighbors, input);
    }

    /**
     * Util class that makes prediction based on the specified list of nearest neighbours.
     */
    private interface KNNRegressionPredictor {
        /**
         * Makes a regression prediction based on the specified list of nearest neighbours.
         *
         * @param neighbors List of nearest neighbours.
         * @param pnt Point to calculate distance to.
         * @return Regression prediction.
         */
        public Double predict(List<LabeledVector<Double>> neighbors, Vector pnt);
    }

    /**
     * Simple (not weighted) implementation of {@link KNNRegressionPredictor} that makes prediction based on the
     * specified list of nearest neighbours.
     */
    private class KNNRegressionSimplePredictor implements KNNRegressionPredictor {
        /** {@inheritDoc} */
        @Override public Double predict(List<LabeledVector<Double>> neighbors, Vector pnt) {
            if (neighbors.isEmpty())
                return null;

            double sum = 0.0;

            for (LabeledVector<Double> neighbor : neighbors)
                sum += neighbor.label();

            return sum / k;
        }
    }

    /**
     * Weighted implementation of {@link KNNRegressionPredictor} that makes prediction based on the specified list of
     * nearest neighbours.
     */
    private class KNNRegressionWeightedPredictor extends KNNRegressionSimplePredictor {
        /** {@inheritDoc} */
        @Override public Double predict(List<LabeledVector<Double>> neighbors, Vector pnt) {
            if (neighbors.isEmpty())
                return null;

            double sum = 0.0;
            double div = 0.0;

            for (LabeledVector<Double> neighbor : neighbors) {
                double distance = distanceMeasure.compute(pnt, neighbor.features());
                sum += neighbor.label() * distance;
                div += distance;
            }

            if (div == 0.0)
                return super.predict(neighbors, pnt);

            return sum / div;
        }
    }
}
