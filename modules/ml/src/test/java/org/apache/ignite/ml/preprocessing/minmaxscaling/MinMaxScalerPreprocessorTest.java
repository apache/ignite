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

package org.apache.ignite.ml.preprocessing.minmaxscaling;

import org.apache.ignite.ml.math.primitives.vector.Vector;
import org.apache.ignite.ml.math.primitives.vector.VectorUtils;
import org.junit.Test;

import static org.junit.Assert.assertArrayEquals;

/**
 * Tests for {@link MinMaxScalerPreprocessor}.
 */
public class MinMaxScalerPreprocessorTest {
    /** Tests {@code apply()} method. */
    @Test
    public void testApply() {
        double[][] data = new double[][]{
            {2., 4., 1.},
            {1., 8., 22.},
            {4., 10., 100.},
            {0., 22., 300.}
        };

        MinMaxScalerPreprocessor<Integer, Vector> preprocessor = new MinMaxScalerPreprocessor<>(
            new double[] {0, 4, 1},
            new double[] {4, 22, 300},
            (k, v) -> v
        );

        double[][] standardData = new double[][]{
            {2. / 4, (4. - 4.) / 18.,  0.},
            {1. / 4, (8. - 4.) / 18.,  (22. - 1.) / 299.},
            {1.,     (10. - 4.) / 18., (100. - 1.) / 299.},
            {0.,     (22. - 4.) / 18., (300. - 1.) / 299.}
        };

       for (int i = 0; i < data.length; i++)
           assertArrayEquals(standardData[i], preprocessor.apply(i, VectorUtils.of(data[i])).asArray(), 1e-8);
    }
}
