/*
 *                   GridGain Community Edition Licensing
 *                   Copyright 2019 GridGain Systems, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License") modified with Commons Clause
 * Restriction; you may not use this file except in compliance with the License. You may obtain a
 * copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the
 * License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied. See the License for the specific language governing permissions
 * and limitations under the License.
 *
 * Commons Clause Restriction
 *
 * The Software is provided to you by the Licensor under the License, as defined below, subject to
 * the following condition.
 *
 * Without limiting other conditions in the License, the grant of rights under the License will not
 * include, and the License does not grant to you, the right to Sell the Software.
 * For purposes of the foregoing, “Sell” means practicing any or all of the rights granted to you
 * under the License to provide to third parties, for a fee or other consideration (including without
 * limitation fees for hosting or consulting/ support services related to the Software), a product or
 * service whose value derives, entirely or substantially, from the functionality of the Software.
 * Any license notice or attribution required by the License must also include this Commons Clause
 * License Condition notice.
 *
 * For purposes of the clause above, the “Licensor” is Copyright 2019 GridGain Systems, Inc.,
 * the “License” is the Apache License, Version 2.0, and the Software is the GridGain Community
 * Edition software provided with this notice.
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
