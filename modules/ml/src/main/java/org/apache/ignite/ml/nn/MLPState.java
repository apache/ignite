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

package org.apache.ignite.ml.nn;

import java.util.ArrayList;
import java.util.List;
import org.apache.ignite.ml.math.primitives.matrix.Matrix;

/**
 * State of MLP after computation.
 */
public class MLPState {
    /**
     * Output of activators.
     */
    protected List<Matrix> activatorsOutput;

    /**
     * Output of linear transformations.
     */
    protected List<Matrix> linearOutput;

    /**
     * Input.
     */
    protected Matrix input;

    /**
     * Construct MLP state.
     *
     * @param input Matrix of inputs.
     */
    public MLPState(Matrix input) {
        this.input = input != null ? input.copy() : null;
        linearOutput = new ArrayList<>();
        activatorsOutput = new ArrayList<>();
    }

    /**
     * Output of activators of given layer. If layer index is 0, inputs are returned.
     *
     * @param layer Index of layer to get activators outputs from.
     * @return Activators output.
     */
    public Matrix activatorsOutput(int layer) {
        return layer > 0 ? activatorsOutput.get(layer - 1) : input;
    }

    /**
     * Output of linear transformation of given layer. If layer index is 0, inputs are returned.
     *
     * @param layer Index of layer to get linear transformation outputs from.
     * @return Linear transformation output.
     */
    public Matrix linearOutput(int layer) {
        return layer == 0 ? input : linearOutput.get(layer - 1);
    }
}
