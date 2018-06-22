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

import org.apache.ignite.ml.Model;
import org.apache.ignite.ml.composition.answercomputer.ModelsCompositionAnswerComputer;

import java.util.Collections;
import java.util.List;
import java.util.Map;

public class ModelsComposition<M extends Model<double[], Double>> implements Model<double[], Double> {
    private final ModelsCompositionAnswerComputer modelsCompositionAnswerComputer;
    private final List<ModelOnFeaturesSubspace<M>> models;

    public ModelsComposition(ModelsCompositionAnswerComputer modelsCompositionAnswerComputer, List<ModelOnFeaturesSubspace<M>> models) {
        this.modelsCompositionAnswerComputer = modelsCompositionAnswerComputer;
        this.models = Collections.unmodifiableList(models);
    }

    @Override
    public Double apply(double[] features) {
        double[] predictions = new double[models.size()];
        for(int i = 0; i < models.size(); i++)
            predictions[i] = models.get(i).apply(features);

        return modelsCompositionAnswerComputer.apply(predictions);
    }

    public ModelsCompositionAnswerComputer getModelsCompositionAnswerComputer() {
        return modelsCompositionAnswerComputer;
    }

    public List<ModelOnFeaturesSubspace<M>> getModels() {
        return models;
    }

    public static class ModelOnFeaturesSubspace<M extends Model<double[], Double>> implements Model<double[], Double> {
        private final Map<Integer, Integer> featuresMapping;
        private final M model;

        public ModelOnFeaturesSubspace(Map<Integer, Integer> featuresMapping, M model) {
            this.featuresMapping = Collections.unmodifiableMap(featuresMapping);
            this.model = model;
        }

        @Override
        public Double apply(double[] features) {
            double[] newFeatures = new double[featuresMapping.size()];
            featuresMapping.forEach((localId, featureVectorId) -> newFeatures[localId] = features[featureVectorId]);
            return model.apply(newFeatures);
        }

        public Map<Integer, Integer> getFeaturesMapping() {
            return featuresMapping;
        }
    }
}
