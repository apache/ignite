/*
 * Copyright 2019 GridGain Systems, Inc. and Contributors.
 *
 * Licensed under the GridGain Community Edition License (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     https://www.gridgain.com/products/software/community-edition/gridgain-community-edition-license
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.ignite.ml.nn;

import org.apache.ignite.ml.math.primitives.matrix.Matrix;
import org.apache.ignite.ml.math.primitives.vector.Vector;
import org.apache.ignite.ml.nn.initializers.MLPInitializer;

/**
 * Initialize weights and biases with specified constant.
 */
class MLPConstInitializer implements MLPInitializer {
    /**
     * Constant to be used as bias for all layers.
     */
    private final double bias;

    /**
     * Constant to be used as weight from any neuron to any neuron in next layer.
     */
    private final double weight;

    /**
     * Construct MLPConstInitializer.
     *
     * @param weight Constant to be used as weight from any neuron to any neuron in next layer.
     * @param bias Constant to be used as bias for all layers.
     */
    MLPConstInitializer(double weight, double bias) {
        this.bias = bias;
        this.weight = weight;
    }

    /**
     * Construct MLPConstInitializer with biases constant equal to 0.0.
     *
     * @param weight Constant to be used as weight from any neuron to any neuron in next layer.
     */
    MLPConstInitializer(double weight) {
        this(weight, 0.0);
    }

    /** {@inheritDoc} */
    @Override public void initWeights(Matrix weights) {
        weights.assign(weight);
    }

    /** {@inheritDoc} */
    @Override public void initBiases(Vector biases) {
        biases.assign(bias);
    }
}
