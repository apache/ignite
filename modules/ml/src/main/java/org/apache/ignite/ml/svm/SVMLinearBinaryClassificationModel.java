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
import java.util.Objects;
import org.apache.ignite.ml.Exportable;
import org.apache.ignite.ml.Exporter;
import org.apache.ignite.ml.Model;
import org.apache.ignite.ml.math.primitives.vector.Vector;

/**
 * Base class for SVM linear classification model.
 */
public class SVMLinearBinaryClassificationModel implements Model<Vector, Double>, Exportable<SVMLinearBinaryClassificationModel>, Serializable {
    /** */
    private static final long serialVersionUID = -996984622291440226L;

    /** Output label format. -1 and +1 for false value and raw distances from the separating hyperplane otherwise. */
    private boolean isKeepingRawLabels = false;

    /** Threshold to assign +1 label to the observation if raw value more than this threshold. */
    private double threshold = 0.0;

    /** Multiplier of the objects's vector required to make prediction. */
    private Vector weights;

    /** Intercept of the linear regression model. */
    private double intercept;

    /** */
    public SVMLinearBinaryClassificationModel(Vector weights, double intercept) {
        this.weights = weights;
        this.intercept = intercept;
    }

    /**
     * Set up the output label format.
     *
     * @param isKeepingRawLabels The parameter value.
     * @return Model with new isKeepingRawLabels parameter value.
     */
    public SVMLinearBinaryClassificationModel withRawLabels(boolean isKeepingRawLabels) {
        this.isKeepingRawLabels = isKeepingRawLabels;
        return this;
    }

    /**
     * Set up the threshold.
     *
     * @param threshold The parameter value.
     * @return Model with new threshold parameter value.
     */
    public SVMLinearBinaryClassificationModel withThreshold(double threshold) {
        this.threshold = threshold;
        return this;
    }

    /**
     * Set up the weights.
     *
     * @param weights The parameter value.
     * @return Model with new weights parameter value.
     */
    public SVMLinearBinaryClassificationModel withWeights(Vector weights) {
        this.weights = weights;
        return this;
    }

    /**
     * Set up the intercept.
     *
     * @param intercept The parameter value.
     * @return Model with new intercept parameter value.
     */
    public SVMLinearBinaryClassificationModel withIntercept(double intercept) {
        this.intercept = intercept;
        return this;
    }

    /** {@inheritDoc} */
    @Override public Double apply(Vector input) {
        final double res = input.dot(weights) + intercept;
        if (isKeepingRawLabels)
            return res;
        else
            return res - threshold > 0 ? 1.0 : -1.0;
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
    @Override public <P> void saveModel(Exporter<SVMLinearBinaryClassificationModel, P> exporter, P path) {
        exporter.save(this, path);
    }

    /** {@inheritDoc} */
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

        return "SVMModel [" +
            "weights=" + weights +
            ", intercept=" + intercept +
            ']';
    }

    /** {@inheritDoc} */
    @Override public String toString(boolean pretty) {
        return toString();
    }
}
