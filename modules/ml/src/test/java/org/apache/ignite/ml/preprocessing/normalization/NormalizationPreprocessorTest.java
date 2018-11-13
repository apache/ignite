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

package org.apache.ignite.ml.preprocessing.normalization;

import org.apache.ignite.ml.math.primitives.vector.Vector;
import org.apache.ignite.ml.math.primitives.vector.VectorUtils;
import org.apache.ignite.ml.preprocessing.binarization.BinarizationPreprocessor;
import org.junit.Test;

import static org.junit.Assert.assertArrayEquals;

/**
 * Tests for {@link BinarizationPreprocessor}.
 */
public class NormalizationPreprocessorTest {
    /** Tests {@code apply()} method. */
    @Test
    public void testApply() {
        double[][] data = new double[][]{
            {1, 2, 1},
            {1, 1, 1},
            {1, 0, 0},
        };

        NormalizationPreprocessor<Integer, Vector> preprocessor = new NormalizationPreprocessor<>(
            1,
            (k, v) -> v
        );

        double[][] postProcessedData = new double[][]{
            {0.25, 0.5, 0.25},
            {0.33, 0.33, 0.33},
            {1, 0, 0}
        };

       for (int i = 0; i < data.length; i++)
           assertArrayEquals(postProcessedData[i], preprocessor.apply(i, VectorUtils.of(data[i])).asArray(), 1e-2);
    }
}
