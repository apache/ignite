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

package org.apache.ignite.ml.optimization;

import org.apache.ignite.ml.math.primitives.vector.Vector;

/**
 * Updater based in Barzilai-Borwein method which guarantees convergence.
 */
public class BarzilaiBorweinUpdater implements Updater {
    /** */
    private static final long serialVersionUID = 5046575099408708472L;

    /**
     * Learning rate used on the first iteration.
     */
    private static final double INITIAL_LEARNING_RATE = 1.0;

    /**
     * {@inheritDoc}
     */
    @Override public Vector compute(Vector oldWeights, Vector oldGradient, Vector weights, Vector gradient,
        int iteration) {
        double learningRate = computeLearningRate(oldWeights != null ? oldWeights.copy() : null,
            oldGradient != null ? oldGradient.copy() : null, weights.copy(), gradient.copy());

        return weights.copy().minus(gradient.copy().times(learningRate));
    }

    /** */
    private double computeLearningRate(Vector oldWeights, Vector oldGradient, Vector weights, Vector gradient) {
        if (oldWeights == null || oldGradient == null)
            return INITIAL_LEARNING_RATE;
        else {
            Vector gradientDiff = gradient.minus(oldGradient);

            return weights.minus(oldWeights).dot(gradientDiff) / Math.pow(gradientDiff.kNorm(2.0), 2.0);
        }
    }
}
