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

package org.apache.ignite.ml.svm;

import java.io.Serializable;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import java.util.TreeMap;
import org.apache.ignite.ml.Exportable;
import org.apache.ignite.ml.Exporter;
import org.apache.ignite.ml.Model;
import org.apache.ignite.ml.math.primitives.vector.Vector;

/** Base class for multi-classification model for set of SVM classifiers. */
public class SVMLinearMultiClassClassificationModel implements Model<Vector, Double>, Exportable<SVMLinearMultiClassClassificationModel>, Serializable {
    /** */
    private static final long serialVersionUID = -667986511191350227L;

    /** List of models associated with each class. */
    private Map<Double, SVMLinearBinaryClassificationModel> models;

    /** */
    public SVMLinearMultiClassClassificationModel() {
        this.models = new HashMap<>();
    }

    /** {@inheritDoc} */
    @Override public Double apply(Vector input) {
        TreeMap<Double, Double> maxMargins = new TreeMap<>();

        models.forEach((k, v) -> maxMargins.put(input.dot(v.weights()) + v.intercept(), k));

        return maxMargins.lastEntry().getValue();
    }

    /** {@inheritDoc} */
    @Override public <P> void saveModel(Exporter<SVMLinearMultiClassClassificationModel, P> exporter, P path) {
        exporter.save(this, path);
    }

    /** {@inheritDoc} */
    @Override public boolean equals(Object o) {
        if (this == o)
            return true;

        if (o == null || getClass() != o.getClass())
            return false;

        SVMLinearMultiClassClassificationModel mdl = (SVMLinearMultiClassClassificationModel)o;

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

    /**
     * Adds a specific SVM binary classifier to the bunch of same classifiers.
     *
     * @param clsLb The class label for the added model.
     * @param mdl The model.
     */
    public void add(double clsLb, SVMLinearBinaryClassificationModel mdl) {
        models.put(clsLb, mdl);
    }
}
