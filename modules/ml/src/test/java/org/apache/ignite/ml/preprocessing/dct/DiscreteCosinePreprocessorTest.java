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

package org.apache.ignite.ml.preprocessing.dct;

import org.apache.ignite.ml.dataset.feature.extractor.Vectorizer;
import org.apache.ignite.ml.dataset.feature.extractor.impl.DoubleArrayVectorizer;
import org.junit.Test;

import static org.junit.Assert.assertArrayEquals;

/**
 * Test for {@link DiscreteCosinePreprocessor}
 */
public class DiscreteCosinePreprocessorTest {
    /** */
    private final double[][] data = new double[][]{
            {1, 2, 1},
            {1, 1, 1},
            {1, 0, 0},
    };

    /** */
    private final Vectorizer<Integer, double[], Integer, Double> vectorizer = new DoubleArrayVectorizer<>(0, 1, 2);

    /**
     * Tests {@code apply()} method for DCT type 1.
     */
    @Test
    public void testApplyType1() {
        DiscreteCosinePreprocessor<Integer, double[]> preprocessor = new DiscreteCosinePreprocessor<>(
                vectorizer, 1
        );

        double[][] postProcessedData = new double[][]{
                {6, 0, -2},
                {4, 0, 0},
                {1, 1, 1}
        };

        for (int i = 0; i < data.length; i++)
            assertArrayEquals(postProcessedData[i], preprocessor.apply(i, data[i]).features().asArray(), 1e-2);
    }

    /**
     * Tests {@code apply()} method for DCT type 2.
     */
    @Test
    public void testApplyType2() {
        DiscreteCosinePreprocessor<Integer, double[]> preprocessor = new DiscreteCosinePreprocessor<>(
                vectorizer, 2
        );

        double[][] postProcessedData = new double[][]{
                {8, 0, -2},
                {6, 0, 0},
                {2, 1.73205081, 1}
        };

        for (int i = 0; i < data.length; i++)
            assertArrayEquals(postProcessedData[i], preprocessor.apply(i, data[i]).features().asArray(), 1e-2);
    }

    /**
     * Tests {@code apply()} method for DCT type 3.
     */
    @Test
    public void testApplyType3() {
        DiscreteCosinePreprocessor<Integer, double[]> preprocessor = new DiscreteCosinePreprocessor<>(
                vectorizer, 3
        );

        double[][] postProcessedData = new double[][]{
                {5.46410162, -1, -1.46410162},
                {3.73205081, -1, 0.26794919},
                {1, 1, 1}
        };

        for (int i = 0; i < data.length; i++)
            assertArrayEquals(postProcessedData[i], preprocessor.apply(i, data[i]).features().asArray(), 1e-2);
    }

    /**
     * Tests {@code apply()} method for DCT type 4.
     */
    @Test
    public void testApplyType4() {
        DiscreteCosinePreprocessor<Integer, double[]> preprocessor = new DiscreteCosinePreprocessor<>(
                vectorizer, 4
        );

        double[][] postProcessedData = new double[][]{
                {5.27791687, -2.82842712, -0.37893738},
                {3.86370331, -1.41421356, 1.03527618},
                {1.93185165, 1.41421356, 0.51763809}
        };

        for (int i = 0; i < data.length; i++)
            assertArrayEquals(postProcessedData[i], preprocessor.apply(i, data[i]).features().asArray(), 1e-2);
    }
}