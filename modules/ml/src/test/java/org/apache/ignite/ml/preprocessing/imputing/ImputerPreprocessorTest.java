/*
 * Copyright 2019 GridGain Systems, Inc. and Contributors.
 * 
 * Licensed under the GridGain Community Edition License (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * 
 *     https://www.gridgain.com/products/software/community-edition/gridgain-community-edition-license
 * 
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.ignite.ml.preprocessing.imputing;

import org.apache.ignite.ml.dataset.feature.extractor.Vectorizer;
import org.apache.ignite.ml.dataset.feature.extractor.impl.DoubleArrayVectorizer;
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

        Vectorizer<Integer, double[], Integer, Double> vectorizer = new DoubleArrayVectorizer<>(0, 1, 2);

        ImputerPreprocessor<Integer,  double[]> preprocessor = new ImputerPreprocessor<>(
            VectorUtils.of(1.1, 10.1, 100.1),
            vectorizer
        );

        double[][] postProcessedData = new double[][]{
            {1.1, 20, 3},
            {2, 10.1, 8},
            {1.1, 10.1, 100.1},
        };

       for (int i = 0; i < data.length; i++)
           assertArrayEquals(postProcessedData[i], preprocessor.apply(i, data[i]).features().asArray(), 1e-8);
    }
}
