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

package org.apache.ignite.ml.nn.initializers;

import java.util.Random;
import org.apache.ignite.ml.math.primitives.matrix.Matrix;
import org.apache.ignite.ml.math.primitives.vector.Vector;

/**
 * Class for initialization of MLP parameters with random uniformly distributed numbers from -1 to 1.
 */
public class RandomInitializer implements MLPInitializer {
    /**
     * RNG.
     */
    private final Random rnd;

    /**
     * Construct RandomInitializer from given RNG.
     *
     * @param rnd RNG.
     */
    public RandomInitializer(Random rnd) {
        this.rnd = rnd;
    }

    /**
     * Constructs RandomInitializer with the given seed.
     *
     * @param seed Seed.
     */
    public RandomInitializer(long seed) {
        this.rnd = new Random(seed);
    }

    /**
     * Constructs RandomInitializer with random seed.
     */
    public RandomInitializer() {
        this.rnd = new Random();
    }

    /** {@inheritDoc} */
    @Override public void initWeights(Matrix weights) {
        weights.map(value -> 2 * (rnd.nextDouble() - 0.5));
    }

    /** {@inheritDoc} */
    @Override public void initBiases(Vector biases) {
        biases.map(value -> 2 * (rnd.nextDouble() - 0.5));
    }
}
