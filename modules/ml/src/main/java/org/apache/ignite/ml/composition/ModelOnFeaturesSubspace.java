/*
 *                   GridGain Community Edition Licensing
 *                   Copyright 2019 GridGain Systems, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License") modified with Commons Clause
 * Restriction; you may not use this file except in compliance with the License. You may obtain a
 * copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the
 * License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied. See the License for the specific language governing permissions
 * and limitations under the License.
 *
 * Commons Clause Restriction
 *
 * The Software is provided to you by the Licensor under the License, as defined below, subject to
 * the following condition.
 *
 * Without limiting other conditions in the License, the grant of rights under the License will not
 * include, and the License does not grant to you, the right to Sell the Software.
 * For purposes of the foregoing, “Sell” means practicing any or all of the rights granted to you
 * under the License to provide to third parties, for a fee or other consideration (including without
 * limitation fees for hosting or consulting/ support services related to the Software), a product or
 * service whose value derives, entirely or substantially, from the functionality of the Software.
 * Any license notice or attribution required by the License must also include this Commons Clause
 * License Condition notice.
 *
 * For purposes of the clause above, the “Licensor” is Copyright 2019 GridGain Systems, Inc.,
 * the “License” is the Apache License, Version 2.0, and the Software is the GridGain Community
 * Edition software provided with this notice.
 */

package org.apache.ignite.ml.composition;

import java.util.Collections;
import java.util.Map;
import java.util.stream.Collectors;
import org.apache.ignite.ml.IgniteModel;
import org.apache.ignite.ml.math.primitives.vector.Vector;
import org.apache.ignite.ml.math.primitives.vector.VectorUtils;
import org.apache.ignite.ml.util.ModelTrace;

/**
 * Model trained on a features subspace with mapping from original features space to subspace.
 */
public class ModelOnFeaturesSubspace implements IgniteModel<Vector, Double> {
    /**
     * Features mapping to subspace.
     */
    private final Map<Integer, Integer> featuresMapping;
    /**
     * Trained model of features subspace.
     */
    private final IgniteModel<Vector, Double> mdl;

    /**
     * Constructs new instance of ModelOnFeaturesSubspace.
     *
     * @param featuresMapping Features mapping to subspace.
     * @param mdl Learned model.
     */
    ModelOnFeaturesSubspace(Map<Integer, Integer> featuresMapping, IgniteModel<Vector, Double> mdl) {
        this.featuresMapping = Collections.unmodifiableMap(featuresMapping);
        this.mdl = mdl;
    }

    /**
     * Projects features vector to subspace in according to mapping and apply model to it.
     *
     * @param features Features vector.
     * @return Estimation.
     */
    @Override public Double predict(Vector features) {
        double[] newFeatures = new double[featuresMapping.size()];
        featuresMapping.forEach((localId, featureVectorId) -> newFeatures[localId] = features.get(featureVectorId));
        return mdl.predict(VectorUtils.of(newFeatures));
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
    public IgniteModel<Vector, Double> getMdl() {
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
