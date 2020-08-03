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

package org.apache.ignite.ml.preprocessing.encoding;

import java.io.Serializable;
import java.util.HashMap;
import java.util.HashSet;
import org.apache.ignite.ml.dataset.feature.extractor.Vectorizer;
import org.apache.ignite.ml.dataset.feature.extractor.impl.DummyVectorizer;
import org.apache.ignite.ml.math.primitives.vector.Vector;
import org.apache.ignite.ml.math.primitives.vector.impl.DenseVector;
import org.apache.ignite.ml.preprocessing.encoding.stringencoder.StringEncoderPreprocessor;
import org.junit.Test;

import static org.junit.Assert.assertArrayEquals;

/**
 * Tests for {@link StringEncoderPreprocessor}.
 */
public class StringEncoderPreprocessorTest {
    /** Tests {@code apply()} method. */
    @Test
    public void testApply() {
        Vector[] data = new Vector[] {
            new DenseVector(new Serializable[] {"1", "Moscow", "A"}),
            new DenseVector(new Serializable[] {"2", "Moscow", "B"}),
            new DenseVector(new Serializable[] {"2", "Moscow", "B"}),
        };

        Vectorizer<Integer, Vector, Integer, Double> vectorizer = new DummyVectorizer<>(0, 1, 2);

        StringEncoderPreprocessor<Integer, Vector> preprocessor = new StringEncoderPreprocessor<Integer, Vector>(
            new HashMap[] {
                new HashMap() {
                    {
                        put("1", 1);
                        put("2", 0);
                    }
                }, new HashMap() {
                {
                    put("Moscow", 0);
                }
            }, new HashMap() {
                {
                    put("A", 1);
                    put("B", 0);
                }
            } },
            vectorizer,
            new HashSet() {
                {
                    add(0);
                    add(1);
                    add(2);
                }
            });

        double[][] postProcessedData = new double[][] {
            {1.0, 0.0, 1.0},
            {0.0, 0.0, 0.0},
            {0.0, 0.0, 0.0},
        };

        for (int i = 0; i < data.length; i++)
            assertArrayEquals(postProcessedData[i], preprocessor.apply(i, data[i]).features().asArray(), 1e-8);
    }
}
