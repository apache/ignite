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
import org.apache.ignite.ml.Exportable;
import org.apache.ignite.ml.Exporter;
import org.apache.ignite.ml.Model;
import org.apache.ignite.ml.math.Vector;

/**
 * Base class for SVM linear classification model.
 */
public class SVMLinearClassificationModel implements Model<Vector, Double>, Exportable<SVMLinearClassificationModel>, Serializable {
    /** Output label format. -1 and +1 for false value and raw distances from the separating hyperplane otherwise. */
    private boolean isKeepingRawLabels = false;

    /** Threshold to assign +1 label to the observation if raw value more than this threshold.*/
    private double threshold = 0.0;

    /** Regularization parameter. */
    private double lambda = 1.0;

    /** Multiplier of the objects's vector required to make prediction.  */
    private Vector weights;

    /** Intercept of the linear regression model. */
    private double intercept;

    public SVMLinearClassificationModel(Vector weights, double intercept) {
        this.weights = weights;
        this.intercept = intercept;
    }

    /** */
    public SVMLinearClassificationModel withRawLabels(boolean isKeepingRawLabels){
        this.isKeepingRawLabels = isKeepingRawLabels;
        return this;
    }

    /** */
    public SVMLinearClassificationModel withLambda(double lambda){
        assert lambda > 0.0;
        this.lambda = lambda;
        return this;
    }

    /** */
    public SVMLinearClassificationModel withThreshold(double threshold){
        this.threshold = threshold;
        return this;
    }

    /** */
    public SVMLinearClassificationModel withWeights(Vector weights) {
        this.weights = weights;
        return this;
    }

    /** */
    public SVMLinearClassificationModel withIntercept(double intercept) {
        this.intercept = intercept;
        return this;
    }

    /** {@inheritDoc} */
    @Override public Double apply(Vector input) {
        final double result = input.dot(weights) + intercept;
        if(isKeepingRawLabels)
            return result;
        else
            return Math.signum(result);
    }

    /** */
    public double lambda() {
        return lambda;
    }

    /** */
    public boolean isKeepingRawLabels() {
        return isKeepingRawLabels;
    }

    /** */
    public double threshold() {
        return threshold;
    }

    /** */
    public Vector weights() {
        return weights;
    }

    /** */
    public double intercept() {
        return intercept;
    }

    /**  {@inheritDoc} */
    @Override public <P> void saveModel(Exporter<SVMLinearClassificationModel, P> exporter, P path) {

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

        return "LinearRegressionModel{" +
            "weights=" + weights +
            ", intercept=" + intercept +
            '}';
    }
}
