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

package org.apache.ignite.ml.tree.impurity.util;

import org.junit.Test;

import static org.junit.Assert.assertArrayEquals;

/**
 * Tests for {@link SimpleStepFunctionCompressor}.
 */
public class SimpleStepFunctionCompressorTest {
    /** */
    @Test
    @SuppressWarnings("unchecked")
    public void testDefaultCompress() {
        StepFunction<TestImpurityMeasure> function = new StepFunction<>(
            new double[]{1, 2, 3, 4},
            TestImpurityMeasure.asTestImpurityMeasures(1, 2, 3, 4)
        );

        SimpleStepFunctionCompressor<TestImpurityMeasure> compressor = new SimpleStepFunctionCompressor<>();

        StepFunction<TestImpurityMeasure> resFunction = compressor.compress(new StepFunction [] {function})[0];

        assertArrayEquals(new double[]{1, 2, 3, 4}, resFunction.getX(), 1e-10);
        assertArrayEquals(TestImpurityMeasure.asTestImpurityMeasures(1, 2, 3, 4), resFunction.getY());
    }

    /** */
    @Test
    public void testDefaults() {
        StepFunction<TestImpurityMeasure> function = new StepFunction<>(
            new double[]{1, 2, 3, 4},
            TestImpurityMeasure.asTestImpurityMeasures(1, 2, 3, 4)
        );

        SimpleStepFunctionCompressor<TestImpurityMeasure> compressor = new SimpleStepFunctionCompressor<>();

        StepFunction<TestImpurityMeasure> resFunction = compressor.compress(function);

        assertArrayEquals(new double[]{1, 2, 3, 4}, resFunction.getX(), 1e-10);
        assertArrayEquals(TestImpurityMeasure.asTestImpurityMeasures(1, 2, 3, 4), resFunction.getY());
    }

    /** */
    @Test
    public void testCompressSmallFunction() {
        StepFunction<TestImpurityMeasure> function = new StepFunction<>(
            new double[]{1, 2, 3, 4},
            TestImpurityMeasure.asTestImpurityMeasures(1, 2, 3, 4)
        );

        SimpleStepFunctionCompressor<TestImpurityMeasure> compressor = new SimpleStepFunctionCompressor<>(5, 0, 0);

        StepFunction<TestImpurityMeasure> resFunction = compressor.compress(function);

        assertArrayEquals(new double[]{1, 2, 3, 4}, resFunction.getX(), 1e-10);
        assertArrayEquals(TestImpurityMeasure.asTestImpurityMeasures(1, 2, 3, 4), resFunction.getY());
    }

    /** */
    @Test
    public void testCompressIncreasingFunction() {
        StepFunction<TestImpurityMeasure> function = new StepFunction<>(
            new double[]{1, 2, 3, 4, 5},
            TestImpurityMeasure.asTestImpurityMeasures(1, 2, 3, 4, 5)
        );

        SimpleStepFunctionCompressor<TestImpurityMeasure> compressor = new SimpleStepFunctionCompressor<>(1, 0.4, 0);

        StepFunction<TestImpurityMeasure> resFunction = compressor.compress(function);

        assertArrayEquals(new double[]{1, 3, 5}, resFunction.getX(), 1e-10);
        assertArrayEquals(TestImpurityMeasure.asTestImpurityMeasures(1, 3, 5), resFunction.getY());
    }

    /** */
    @Test
    public void testCompressDecreasingFunction() {
        StepFunction<TestImpurityMeasure> function = new StepFunction<>(
            new double[]{1, 2, 3, 4, 5},
            TestImpurityMeasure.asTestImpurityMeasures(5, 4, 3, 2, 1)
        );

        SimpleStepFunctionCompressor<TestImpurityMeasure> compressor = new SimpleStepFunctionCompressor<>(1, 0, 0.4);

        StepFunction<TestImpurityMeasure> resFunction = compressor.compress(function);

        assertArrayEquals(new double[]{1, 3, 5}, resFunction.getX(), 1e-10);
        assertArrayEquals(TestImpurityMeasure.asTestImpurityMeasures(5, 3, 1), resFunction.getY());
    }
}
