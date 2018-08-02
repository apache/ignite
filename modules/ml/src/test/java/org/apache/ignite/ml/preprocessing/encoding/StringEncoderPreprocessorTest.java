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

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import org.apache.ignite.ml.math.exceptions.preprocessing.UnknownStringValue;
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
        String[][] data = new String[][] {
            {"1", "Moscow", "A"},
            {"2", "Moscow", "B"},
            {"2", "Moscow", "B"},
        };

        StringEncoderPreprocessor<Integer, String[]> preprocessor = new StringEncoderPreprocessor<>(
            encodingValues(),
            (k, v) -> v,
            handledIndices());

        double[][] postProcessedData = new double[][] {
            {1.0, 0.0, 1.0},
            {0.0, 0.0, 0.0},
            {0.0, 0.0, 0.0},
        };

        for (int i = 0; i < data.length; i++)
            assertArrayEquals(postProcessedData[i], preprocessor.apply(i, data[i]).asArray(), 1e-8);
    }

    /** Tests {@code apply()} method throwing expected exception. */
    @Test(expected = UnknownStringValue.class)
    public void testUnknownStringValue() {
        String[][] data = new String[][] {
            {"3", "Moscow", "A"},
            {"2", "Moscow", "B"},
            {"2", "Moscow", "B"},
        };

        new StringEncoderPreprocessor<Integer, String[]>(
            encodingValues(),
            (k, v) -> v,
            handledIndices()).apply(0, data[0]);
    }

    /** */
    @SuppressWarnings("unchecked")
    private Map<String, Integer>[] encodingValues() {
        return new HashMap[] {
            new HashMap<String, Integer>() {
                {
                    put("1", 1);
                    put("2", 0);
                }
            }, new HashMap<String, Integer>() {
            {
                put("Moscow", 0);
            }
        }, new HashMap<String, Integer>() {
            {
                put("A", 1);
                put("B", 0);
            }
        }};
    }

    /** */
    private Set<Integer> handledIndices() {
        return new HashSet<Integer>() {
            {
                add(0);
                add(1);
                add(2);
            }
        };
    }
}
