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

package org.apache.ignite.ml.multiclass;

import java.io.Serializable;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.TreeMap;
import org.apache.ignite.ml.Exportable;
import org.apache.ignite.ml.Exporter;
import org.apache.ignite.ml.IgniteModel;
import org.apache.ignite.ml.math.primitives.vector.Vector;

/** Base class for multi-classification model for set of classifiers. */
public class MultiClassModel<M extends IgniteModel<Vector, Double>> implements IgniteModel<Vector, Double>, Exportable<MultiClassModel>, Serializable {
    /** */
    private static final long serialVersionUID = -114986533359917L;

    /** List of models associated with each class. */
    private Map<Double, M> models;

    /** */
    public MultiClassModel() {
        this.models = new HashMap<>();
    }

    /**
     * Adds a specific binary classifier to the bunch of same classifiers.
     *
     * @param clsLb The class label for the added model.
     * @param mdl The model.
     */
    public void add(double clsLb, M mdl) {
        models.put(clsLb, mdl);
    }

    /**
     * @param clsLb Class label.
     * @return model for class label if it exists.
     */
    public Optional<M> getModel(Double clsLb) {
        return Optional.ofNullable(models.get(clsLb));
    }

    /** {@inheritDoc} */
    @Override public Double predict(Vector input) {
        TreeMap<Double, Double> maxMargins = new TreeMap<>();

        models.forEach((k, v) -> maxMargins.put(v.predict(input), k));

        // returns value the most closest to 1
        return maxMargins.lastEntry().getValue();
    }

    /** {@inheritDoc} */
    @Override public <P> void saveModel(Exporter<MultiClassModel, P> exporter, P path) {
        exporter.save(this, path);
    }

    /** {@inheritDoc} */
    @Override public boolean equals(Object o) {
        if (this == o)
            return true;

        if (o == null || getClass() != o.getClass())
            return false;

        MultiClassModel mdl = (MultiClassModel)o;

        return Objects.equals(models, mdl.models);
    }

    /** {@inheritDoc} */
    @Override public int hashCode() {
        return Objects.hash(models);
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        StringBuilder wholeStr = new StringBuilder();

        models.forEach((clsLb, mdl) ->
            wholeStr
                .append("The class with label ")
                .append(clsLb)
                .append(" has classifier: ")
                .append(mdl.toString())
                .append(System.lineSeparator())
        );

        return wholeStr.toString();
    }

    /** {@inheritDoc} */
    @Override public String toString(boolean pretty) {
        return toString();
    }
}
