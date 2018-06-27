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
import java.util.Map;
import org.apache.ignite.ml.Model;

/**
 * Model trained on a features subspace with mapping from original features space to subspace.
 */
public class ModelOnFeaturesSubspace implements Model<double[], Double> {
    /**
     * Features mapping to subspace.
     */
    private final Map<Integer, Integer> featuresMapping;
    /**
     * Trained model of features subspace.
     */
    private final Model<double[], Double> model;

    /**
     * Constructs new instance of ModelOnFeaturesSubspace.
     *
     * @param featuresMapping Features mapping to subspace.
     * @param mdl Learned model.
     */
    public ModelOnFeaturesSubspace(Map<Integer, Integer> featuresMapping, Model<double[], Double> mdl) {
        this.featuresMapping = Collections.unmodifiableMap(featuresMapping);
        this.model = mdl;
    }

    /**
     * Projects features vector to subspace in according to mapping and apply model to it.
     *
     * @param features Features vector.
     * @return Estimation.
     */
    @Override public Double apply(double[] features) {
        double[] newFeatures = new double[featuresMapping.size()];
        featuresMapping.forEach((localId, featureVectorId) -> newFeatures[localId] = features[featureVectorId]);
        return model.apply(newFeatures);
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
    public Model<double[], Double> getModel() {
        return model;
    }
}
