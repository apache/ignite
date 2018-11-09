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
import java.util.stream.Collectors;
import org.apache.ignite.ml.Model;
import org.apache.ignite.ml.math.primitives.vector.Vector;
import org.apache.ignite.ml.math.primitives.vector.VectorUtils;
import org.apache.ignite.ml.util.ModelTrace;

/**
 * Model trained on a features subspace with mapping from original features space to subspace.
 */
public class ModelOnFeaturesSubspace implements Model<Vector, Double> {
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
        return mdl.apply(VectorUtils.of(newFeatures));
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

    /** {@inheritDoc} */
    @Override public String toString() {
        return toString(false);
    }

    /** {@inheritDoc} */
    @Override public String toString(boolean pretty) {
        String mappingStr = featuresMapping.entrySet().stream()
            .map(e -> String.format("%d -> %d", e.getKey(), e.getValue()))
            .collect(Collectors.joining(", ", "{", "}"));

        return ModelTrace.builder("ModelOnFeatureSubspace", pretty)
            .addField("features mapping", mappingStr)
            .addField("model", mdl.toString(false))
            .toString();
    }
}
