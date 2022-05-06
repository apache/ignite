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

package org.apache.ignite.ml.regressions.linear;

import java.io.File;
import java.io.IOException;
import java.nio.file.Path;
import java.util.Objects;
import java.util.UUID;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.ignite.ml.Exportable;
import org.apache.ignite.ml.Exporter;
import org.apache.ignite.ml.IgniteModel;
import org.apache.ignite.ml.inference.json.JSONModel;
import org.apache.ignite.ml.inference.json.JSONWritable;
import org.apache.ignite.ml.math.primitives.vector.Vector;
import org.apache.ignite.ml.math.primitives.vector.VectorUtils;

/**
 * Simple linear regression model which predicts result value Y as a linear combination of input variables:
 * Y = weights * X + intercept.
 */
public final class LinearRegressionModel implements IgniteModel<Vector, Double>, Exportable<LinearRegressionModel>,
    JSONWritable {
    /** */
    private static final long serialVersionUID = -105984600091550226L;

    /** Multiplier of the objects's vector required to make prediction.  */
    private Vector weights;

    /** Intercept of the linear regression model */
    private double intercept;

    /** */
    public LinearRegressionModel(Vector weights, double intercept) {
        this.weights = weights;
        this.intercept = intercept;
    }

    /** */
    private LinearRegressionModel() {
    }

    /** */
    public Vector weights() {
        return weights;
    }

    /** */
    public double intercept() {
        return intercept;
    }

    /**
     * Set up the weights.
     *
     * @param weights The parameter value.
     * @return Model with new weights parameter value.
     */
    public LinearRegressionModel withWeights(Vector weights) {
        this.weights = weights;
        return this;
    }

    /**
     * Set up the intercept.
     *
     * @param intercept The parameter value.
     * @return Model with new intercept parameter value.
     */
    public LinearRegressionModel withIntercept(double intercept) {
        this.intercept = intercept;
        return this;
    }

    /** {@inheritDoc} */
    @Override public Double predict(Vector input) {
        return input.dot(weights) + intercept;
    }

    /** {@inheritDoc} */
    @Override public <P> void saveModel(Exporter<LinearRegressionModel, P> exporter, P path) {
        exporter.save(this, path);
    }

    /** {@inheritDoc} */
    @Override public boolean equals(Object o) {
        if (this == o)
            return true;
        if (o == null || getClass() != o.getClass())
            return false;
        LinearRegressionModel mdl = (LinearRegressionModel)o;
        return Double.compare(mdl.intercept, intercept) == 0 &&
            Objects.equals(weights, mdl.weights);
    }

    /** {@inheritDoc} */
    @Override public int hashCode() {

        return Objects.hash(weights, intercept);
    }

    /** {@inheritDoc} */
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

        return "LinearRegressionModel [" +
            "weights=" + weights +
            ", intercept=" + intercept +
            ']';
    }

    /** {@inheritDoc} */
    @Override public String toString(boolean pretty) {
        return toString();
    }

    /** Loads LinearRegressionModel from JSON file. */
    public static LinearRegressionModel fromJSON(Path path) {
        ObjectMapper mapper = new ObjectMapper();

        LinearRegressionModelJSONExportModel linearRegressionJSONExportModel;
        try {
            linearRegressionJSONExportModel = mapper
                    .readValue(new File(path.toAbsolutePath().toString()), LinearRegressionModelJSONExportModel.class);

            return linearRegressionJSONExportModel.convert();
        }
        catch (IOException e) {
            e.printStackTrace();
        }

        return null;
    }

    /** {@inheritDoc} */
    @Override public void toJSON(Path path) {
        ObjectMapper mapper = new ObjectMapper();
        try {
            LinearRegressionModelJSONExportModel exportModel = new LinearRegressionModelJSONExportModel(
                System.currentTimeMillis(),
                "linreg_" + UUID.randomUUID().toString(),
                LinearRegressionModel.class.getSimpleName()
            );
            exportModel.intercept = intercept;
            exportModel.weights = weights.asArray();

            File file = new File(path.toAbsolutePath().toString());
            mapper.writeValue(file, exportModel);
        }
        catch (IOException e) {
            e.printStackTrace();
        }
    }

    /**  */
    public static class LinearRegressionModelJSONExportModel extends JSONModel {
        /**
         * Multiplier of the objects's vector required to make prediction.
         */
        public double[] weights;

        /**
         * Intercept of the linear regression model.
         */
        public double intercept;

        /** */
        public LinearRegressionModelJSONExportModel(Long timestamp, String uid, String modelClass) {
            super(timestamp, uid, modelClass);
        }

        /** */
        @JsonCreator
        public LinearRegressionModelJSONExportModel() {
        }

        /** {@inheritDoc} */
        @Override public LinearRegressionModel convert() {
            LinearRegressionModel linRegMdl = new LinearRegressionModel();
            linRegMdl.withWeights(VectorUtils.of(weights));
            linRegMdl.withIntercept(intercept);

            return linRegMdl;
        }
    }
}
