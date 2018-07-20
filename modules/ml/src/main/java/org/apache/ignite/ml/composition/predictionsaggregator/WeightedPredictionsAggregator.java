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

package org.apache.ignite.ml.composition.predictionsaggregator;

import org.apache.ignite.internal.util.typedef.internal.A;

/**
 * Predictions aggregator returning weighted sum of predictions.
 * result(p1, ..., pn) = bias + p1*w1 + ... + pn*wn
 */
public class WeightedPredictionsAggregator implements PredictionsAggregator {
    /** Weights for predictions. */
    private final double[] weights;
    /** Bias. */
    private final double bias;

    /**
     * Constructs WeightedPredictionsAggregator instance.
     *
     * @param weights Weights.
     */
    public WeightedPredictionsAggregator(double[] weights) {
        this.weights = weights;
        this.bias = 0.0;
    }

    /**
     * Constructs WeightedPredictionsAggregator instance.
     *
     * @param weights Weights.
     * @param bias Bias.
     */
    public WeightedPredictionsAggregator(double[] weights, double bias) {
        this.weights = weights;
        this.bias = bias;
    }

    /** {@inheritDoc} */
    @Override public Double apply(double[] answers) {
        A.ensure(answers.length == weights.length,
            "Composition vector must have same size as weights vector");

        double res = bias;

        for(int i = 0; i< answers.length; i++)
            res += weights[i] * answers[i];

        return res;
    }
}
