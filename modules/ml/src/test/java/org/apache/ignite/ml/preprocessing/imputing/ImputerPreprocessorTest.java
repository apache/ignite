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
