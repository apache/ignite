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

package org.apache.ignite.ml.regressions.logistic.binomial;

import java.io.Serializable;
import java.util.Objects;
import org.apache.ignite.ml.Exportable;
import org.apache.ignite.ml.Exporter;
import org.apache.ignite.ml.Model;
import org.apache.ignite.ml.math.primitives.vector.Vector;

/**
 * Logistic regression (logit model) is a generalized linear model used for binomial regression.
 */
public class LogisticRegressionModel implements Model<Vector, Double>, Exportable<LogisticRegressionModel>, Serializable {
    /** */
    private static final long serialVersionUID = -133984600091550776L;

    /** Multiplier of the objects's vector required to make prediction. */
    private Vector weights;

    /** Intercept of the linear regression model. */
    private double intercept;

    /** Output label format. 0 and 1 for false value and raw sigmoid regression value otherwise. */
    private boolean isKeepingRawLabels = false;

    /** Threshold to assign '1' label to the observation if raw value more than this threshold. */
    private double threshold = 0.5;

    /** */
    public LogisticRegressionModel(Vector weights, double intercept) {
        this.weights = weights;
        this.intercept = intercept;
    }

    /**
     * Set up the output label format.
     *
     * @param isKeepingRawLabels The parameter value.
     * @return Model with new isKeepingRawLabels parameter value.
     */
    public LogisticRegressionModel withRawLabels(boolean isKeepingRawLabels) {
        this.isKeepingRawLabels = isKeepingRawLabels;
        return this;
    }

    /**
     * Set up the threshold.
     *
     * @param threshold The parameter value.
     * @return Model with new threshold parameter value.
     */
    public LogisticRegressionModel withThreshold(double threshold) {
        this.threshold = threshold;
        return this;
    }

    /**
     * Set up the weights.
     *
     * @param weights The parameter value.
     * @return Model with new weights parameter value.
     */
    public LogisticRegressionModel withWeights(Vector weights) {
        this.weights = weights;
        return this;
    }

    /**
     * Set up the intercept.
     *
     * @param intercept The parameter value.
     * @return Model with new intercept parameter value.
     */
    public LogisticRegressionModel withIntercept(double intercept) {
        this.intercept = intercept;
        return this;
    }

    /**
     * Gets the output label format mode.
     *
     * @return The parameter value.
     */
    public boolean isKeepingRawLabels() {
        return isKeepingRawLabels;
    }

    /**
     * Gets the threshold.
     *
     * @return The parameter value.
     */
    public double threshold() {
        return threshold;
    }

    /**
     * Gets the weights.
     *
     * @return The parameter value.
     */
    public Vector weights() {
        return weights;
    }

    /**
     * Gets the intercept.
     *
     * @return The parameter value.
     */
    public double intercept() {
        return intercept;
    }

    /** {@inheritDoc} */
    @Override public Double apply(Vector input) {

        final double res = sigmoid(input.dot(weights) + intercept);

        if (isKeepingRawLabels)
            return res;
        else
            return res - threshold > 0 ? 1.0 : 0;
    }

    /**
     * Sigmoid function.
     * @param z The regression value.
     * @return The result.
     */
    private static double sigmoid(double z) {
        return 1.0 / (1.0 + Math.exp(-z));
    }

    /** {@inheritDoc} */
    @Override public <P> void saveModel(Exporter<LogisticRegressionModel, P> exporter, P path) {
        exporter.save(this, path);
    }

    /** {@inheritDoc} */
    @Override public boolean equals(Object o) {
        if (this == o)
            return true;
        if (o == null || getClass() != o.getClass())
            return false;

        LogisticRegressionModel mdl = (LogisticRegressionModel)o;

        return Double.compare(mdl.intercept, intercept) == 0
            && Double.compare(mdl.threshold, threshold) == 0
            && Boolean.compare(mdl.isKeepingRawLabels, isKeepingRawLabels) == 0
            && Objects.equals(weights, mdl.weights);
    }

    /** {@inheritDoc} */
    @Override public int hashCode() {
        return Objects.hash(weights, intercept, isKeepingRawLabels, threshold);
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        if (weights.size() < 20) {
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

        return "LogisticRegressionModel [" +
            "weights=" + weights +
            ", intercept=" + intercept +
            ']';
    }

    /** {@inheritDoc} */
    @Override public String toString(boolean pretty) {
        return toString();
    }
}
