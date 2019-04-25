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

import java.io.Serializable;
import org.apache.ignite.ml.math.primitives.matrix.Matrix;
import org.apache.ignite.ml.math.primitives.vector.Vector;

/**
 * Class containing information about layer.
 */
public class MLPLayer implements Serializable {
    /**
     * Weights matrix.
     */
    protected Matrix weights;

    /**
     * Biases vector.
     */
    protected Vector biases;

    /**
     * Construct MLPLayer from weights and biases.
     *
     * @param weights Weights.
     * @param biases Biases.
     */
    public MLPLayer(Matrix weights, Vector biases) {
        this.weights = weights;
        this.biases = biases;
    }
}
