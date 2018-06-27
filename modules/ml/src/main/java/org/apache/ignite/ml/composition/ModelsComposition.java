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

package org.apache.ignite.ml.composition;

import java.util.Collections;
import java.util.List;
import java.util.Map;
import org.apache.ignite.ml.Model;
import org.apache.ignite.ml.composition.predictionsaggregator.PredictionsAggregator;
import org.apache.ignite.ml.math.Vector;
import org.apache.ignite.ml.math.impls.vector.DenseLocalOnHeapVector;

/**
 * Model consisting of several models and prediction aggregation strategy.
 */
public class ModelsComposition implements Model<Vector, Double> {
    /**
     * Predictions aggregator.
     */
    private final PredictionsAggregator predictionsAggregator;
    /**
     * Models.
     */
    private final List<ModelOnFeaturesSubspace> models;

    /**
     * Constructs a new instance of composition of models.
     *
     * @param models Basic models.
     * @param predictionsAggregator Predictions aggregator.
     */
    public ModelsComposition(List<ModelOnFeaturesSubspace> models, PredictionsAggregator predictionsAggregator) {
        this.predictionsAggregator = predictionsAggregator;
        this.models = Collections.unmodifiableList(models);
    }

    /**
     * Applies containing models to features and aggregate them to one prediction.
     *
     * @param features Features vector.
     * @return Estimation.
     */
    @Override public Double apply(Vector features) {
        double[] predictions = new double[models.size()];

        for (int i = 0; i < models.size(); i++)
            predictions[i] = models.get(i).apply(features);

        return predictionsAggregator.apply(predictions);
    }

    /**
     * Returns predictions aggregator.
     */
    public PredictionsAggregator getPredictionsAggregator() {
        return predictionsAggregator;
    }

    /**
     * Returns containing models.
     */
    public List<ModelOnFeaturesSubspace> getModels() {
        return models;
    }

    /**
     * Model trained on a features subspace with mapping from original features space to subspace.
     */
    public static class ModelOnFeaturesSubspace implements Model<Vector, Double> {
        /**
         * Features mapping to subspace.
         */
        private final Map<Integer, Integer> featuresMapping;
        /**
         * Trained model of features subspace.
         */
        private final Model<Vector, Double> mdl;

        /**
         * Constructs new instance of ModelOnFeaturesSubspace.
         *
         * @param featuresMapping Features mapping to subspace.
         * @param mdl Learned model.
         */
        ModelOnFeaturesSubspace(Map<Integer, Integer> featuresMapping, Model<Vector, Double> mdl) {
            this.featuresMapping = Collections.unmodifiableMap(featuresMapping);
            this.mdl = mdl;
        }

        /**
         * Projects features vector to subspace in according to mapping and apply model to it.
         *
         * @param features Features vector.
         * @return Estimation.
         */
        @Override public Double apply(Vector features) {
            double[] newFeatures = new double[featuresMapping.size()];
            featuresMapping.forEach((localId, featureVectorId) -> newFeatures[localId] = features.get(featureVectorId));
            return mdl.apply(new DenseLocalOnHeapVector(newFeatures));
        }

        /**
         * Returns features mapping.
         */
        public Map<Integer, Integer> getFeaturesMapping() {
            return featuresMapping;
        }

        /**
         * Returns model.
         */
        public Model<Vector, Double> getMdl() {
            return mdl;
        }
    }
}
