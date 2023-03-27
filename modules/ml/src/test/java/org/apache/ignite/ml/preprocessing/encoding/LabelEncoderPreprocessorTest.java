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
import java.util.Map;
import org.apache.ignite.ml.dataset.feature.extractor.Vectorizer;
import org.apache.ignite.ml.dataset.feature.extractor.impl.ObjectArrayVectorizer;
import org.apache.ignite.ml.preprocessing.encoding.label.LabelEncoderPreprocessor;
import org.junit.Test;

import static org.junit.Assert.assertEquals;

/**
 * Tests for {@link LabelEncoderPreprocessor}.
 */
public class LabelEncoderPreprocessorTest {
    /** Tests {@code apply()} method. */
    @Test
    public void testApply() {
        Map<Integer, Object[]> data = new HashMap<>();
        data.put(0, new Object[] {1, "A"});
        data.put(1, new Object[] {2, "B"});
        data.put(2, new Object[] {3, "B"});

        final Vectorizer<Integer, Object[], Integer, Object> vectorizer = new ObjectArrayVectorizer<Integer>(0).labeled(1);

        LabelEncoderPreprocessor<Integer, Object[]> preprocessor = new LabelEncoderPreprocessor<Integer, Object[]>(
            new HashMap() {
                {
                    put("A", 1);
                    put("B", 0);
                }
            },
            vectorizer
        );

        double[] postProcessedData = new double[] {
            1.0,
            0.0,
            0.0
        };

        for (int i = 0; i < data.size(); i++)
            assertEquals(postProcessedData[i], (Double)preprocessor.apply(i, data.get(i)).label(), 1e-8);
    }
}
