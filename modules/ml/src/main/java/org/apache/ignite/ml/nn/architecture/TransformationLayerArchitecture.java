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

package org.apache.ignite.ml.nn.architecture;

import org.apache.ignite.ml.math.functions.IgniteDifferentiableDoubleToDoubleFunction;

/**
 * Class encapsulation architecture of transformation layer (i.e. non-input layer).
 */
public class TransformationLayerArchitecture extends LayerArchitecture {
    /**
     * Flag indicating presence of bias in layer.
     */
    private boolean hasBias;

    /**
     * Activation function for layer.
     */
    private IgniteDifferentiableDoubleToDoubleFunction activationFunction;

    /**
     * Construct TransformationLayerArchitecture.
     *
     * @param neuronsCnt Count of neurons in this layer.
     * @param hasBias Flag indicating presence of bias in layer.
     * @param activationFunction Activation function for layer.
     */
    public TransformationLayerArchitecture(int neuronsCnt, boolean hasBias,
        IgniteDifferentiableDoubleToDoubleFunction activationFunction) {
        super(neuronsCnt);

        this.hasBias = hasBias;
        this.activationFunction = activationFunction;
    }

    /**
     * Checks if this layer has a bias.
     *
     * @return Value of predicate "this layer has a bias".
     */
    public boolean hasBias() {
        return hasBias;
    }

    /**
     * Get activation function for this layer.
     *
     * @return Activation function for this layer.
     */
    public IgniteDifferentiableDoubleToDoubleFunction activationFunction() {
        return activationFunction;
    }
}
