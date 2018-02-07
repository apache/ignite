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
import java.util.TreeMap;
import org.apache.ignite.ml.Exportable;
import org.apache.ignite.ml.Exporter;
import org.apache.ignite.ml.Model;
import org.apache.ignite.ml.math.Vector;

public class SVMLinearMultiClassClassificationModel implements Model<Vector, Double>, Exportable<SVMLinearBinaryClassificationModel>, Serializable {
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

/*    *//** {@inheritDoc} *//*
    @Override public <P> void saveModel(Exporter<SVMLinearBinaryClassificationModel, P> exporter, P path) {
        exporter.save(this, path);
    }*/

/*    *//** {@inheritDoc} *//*
    @Override public boolean equals(Object o) {
        if (this == o)
            return true;
        if (o == null || getClass() != o.getClass())
            return false;
        SVMLinearBinaryClassificationModel mdl = (SVMLinearBinaryClassificationModel)o;
        return Double.compare(mdl.intercept, intercept) == 0
            && Double.compare(mdl.threshold, threshold) == 0
            && Boolean.compare(mdl.isKeepingRawLabels, isKeepingRawLabels) == 0
            && Objects.equals(weights, mdl.weights);
    }

    *//** {@inheritDoc} *//*
    @Override public int hashCode() {
        return Objects.hash(weights, intercept, isKeepingRawLabels, threshold);
    }*/

/*    *//** {@inheritDoc} *//*
    @Override public String toString() {
        if (weights.size() < 10) {
            StringBuilder builder = new StringBuilder();

            for (int i = 0; i < weights.size(); i++) {
                double nextItem = i == weights.size() - 1 ? intercept : weights.get(i + 1);

                builder.append(String.format("%.4f", Math.abs(weights.get(i))))
                    .append("*x")
                    .append(i)
                    .append(nextItem > 0 ? " + " : " - ");
            }

            builder.append(String.format("%.4f", Math.abs(intercept)));
            return builder.toString();
        }

        return "LinearRegressionModel{" +
            "weights=" + weights +
            ", intercept=" + intercept +
            '}';
    }*/

    /**
     *
     * @param clsLb
     * @param mdl
     */
    public void add(double clsLb, SVMLinearBinaryClassificationModel mdl) {
        models.put(clsLb, mdl);
    }

    @Override public <P> void saveModel(Exporter<SVMLinearBinaryClassificationModel, P> exporter, P path) {

    }
}
