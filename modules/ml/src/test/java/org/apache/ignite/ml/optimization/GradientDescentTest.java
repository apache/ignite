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

package org.apache.ignite.ml.optimization;

import org.apache.ignite.ml.TestUtils;
import org.apache.ignite.ml.math.Vector;
import org.apache.ignite.ml.math.impls.matrix.DenseLocalOnHeapMatrix;
import org.apache.ignite.ml.math.impls.vector.DenseLocalOnHeapVector;
import org.junit.Test;

/**
 * Tests for {@link GradientDescent}.
 */
public class GradientDescentTest {
    /** */
    private static final double PRECISION = 1e-6;

    /**
     * Test gradient descent optimization on function y = x^2 with gradient function 2 * x.
     */
    @Test
    public void testOptimize() {
        GradientDescent gradientDescent = new GradientDescent(
            (inputs, groundTruth, point) -> point.times(2),
            new SimpleUpdater(0.01)
        );

        Vector res = gradientDescent.optimize(new DenseLocalOnHeapMatrix(new double[1][1]),
            new DenseLocalOnHeapVector(new double[]{ 2.0 }));

        TestUtils.assertEquals(0, res.get(0), PRECISION);
    }

    /**
     * Test gradient descent optimization on function y = (x - 2)^2 with gradient function 2 * (x - 2).
     */
    @Test
    public void testOptimizeWithOffset() {
        GradientDescent gradientDescent = new GradientDescent(
            (inputs, groundTruth, point) -> point.minus(new DenseLocalOnHeapVector(new double[]{ 2.0 })).times(2.0),
            new SimpleUpdater(0.01)
        );

        Vector res = gradientDescent.optimize(new DenseLocalOnHeapMatrix(new double[1][1]),
            new DenseLocalOnHeapVector(new double[]{ 2.0 }));

        TestUtils.assertEquals(2, res.get(0), PRECISION);
    }
}
