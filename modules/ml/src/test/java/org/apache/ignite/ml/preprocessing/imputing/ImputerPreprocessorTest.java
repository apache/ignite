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

package org.apache.ignite.ml.preprocessing.imputing;

import org.apache.ignite.ml.math.primitives.vector.Vector;
import org.apache.ignite.ml.math.primitives.vector.VectorUtils;
import org.junit.Test;

import static org.junit.Assert.assertArrayEquals;

/**
 * Tests for {@link ImputerPreprocessor}.
 */
public class ImputerPreprocessorTest {
    /** Tests {@code apply()} method. */
    @Test
    public void testApply() {
        double[][] data = new double[][]{
            {Double.NaN, 20, 3},
            {2, Double.NaN, 8},
            {Double.NaN, Double.NaN, Double.NaN},
        };

        ImputerPreprocessor<Integer, Vector> preprocessor = new ImputerPreprocessor<>(
            VectorUtils.of(1.1, 10.1, 100.1),
            (k, v) -> v
        );

        double[][] postProcessedData = new double[][]{
            {1.1, 20, 3},
            {2, 10.1, 8},
            {1.1, 10.1, 100.1},
        };

       for (int i = 0; i < data.length; i++)
           assertArrayEquals(postProcessedData[i], preprocessor.apply(i, VectorUtils.of(data[i])).asArray(), 1e-8);
    }
}
