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

package org.apache.ignite.ml.preprocessing.maxabsscaling;

import org.apache.ignite.ml.dataset.feature.extractor.Vectorizer;
import org.apache.ignite.ml.dataset.feature.extractor.impl.DoubleArrayVectorizer;
import org.junit.Test;

import static org.junit.Assert.assertArrayEquals;

/**
 * Tests for {@link MaxAbsScalerPreprocessor}.
 */
public class MaxAbsScalerPreprocessorTest {
    /** Tests {@code apply()} method. */
    @Test
    public void testApply() {
        double[][] data = new double[][] {
            {2., 4., 1.},
            {1., 8., 22.},
            {-4., 10., 100.},
            {0., 22., 300.}
        };
        double[] maxAbs = new double[] {4, 22, 300};

        Vectorizer<Integer, double[], Integer, Double> vectorizer = new DoubleArrayVectorizer<>(0, 1, 2);

        MaxAbsScalerPreprocessor<Integer, double[]> preprocessor = new MaxAbsScalerPreprocessor<>(
            maxAbs,
            vectorizer
        );

        double[][] expData = new double[][] {
            {.5, 4. / 22, 1. / 300},
            {.25, 8. / 22, 22. / 300},
            {-1., 10. / 22, 100. / 300},
            {0., 22. / 22, 300. / 300}
        };

        for (int i = 0; i < data.length; i++)
            assertArrayEquals(expData[i], preprocessor.apply(i, data[i]).features().asArray(), 1e-8);
    }
}
