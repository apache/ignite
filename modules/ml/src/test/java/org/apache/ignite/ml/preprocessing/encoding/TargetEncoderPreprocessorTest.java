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
import org.apache.ignite.ml.preprocessing.encoding.target.TargetEncoderPreprocessor;
import org.apache.ignite.ml.preprocessing.encoding.target.TargetEncodingMeta;
import org.junit.Test;
import static org.junit.Assert.assertArrayEquals;

/**
 * Tests for {@link TargetEncoderPreprocessor}.
 */
public class TargetEncoderPreprocessorTest {
    /** Tests {@code apply()} method. */
    @Test
    public void testApply() {
        Vector[] data = new Vector[] {
            new DenseVector(new Serializable[] {"1", "Moscow", "A"}),
            new DenseVector(new Serializable[] {"2", "Moscow", "B"}),
            new DenseVector(new Serializable[] {"3", "Moscow", "B"}),
        };

      Vectorizer<Integer, Vector, Integer, Double> vectorizer = new DummyVectorizer<>(0, 1, 2);

        TargetEncoderPreprocessor<Integer, Vector> preprocessor = new TargetEncoderPreprocessor<>(
            new TargetEncodingMeta[]{
                // feature 0
                new TargetEncodingMeta()
                    .withGlobalMean(0.5)
                    .withCategoryMean(new HashMap<String, Double>() {
                      {
                        put("1", 1.0); // category "1" avg mean = 1.0
                        put("2", 0.0); // category "2" avg mean = 0.0
                      }
                    }),
                // feature 1
                new TargetEncodingMeta()
                    .withGlobalMean(0.1)
                    .withCategoryMean(new HashMap<String, Double>() {}),
                // feature 2
                new TargetEncodingMeta()
                    .withGlobalMean(0.1)
                    .withCategoryMean(new HashMap<String, Double>() {
                      {
                        put("A", 1.0); // category "A" avg mean 1.0
                        put("B", 2.0); // category "B" avg mean 2.0
                      }
                    })
            },
            vectorizer,
            new HashSet<Integer>() {
                {
                    add(0);
                    add(1);
                    add(2);
                }
            });

      double[][] postProcessedData = new double[][] {
          {
              1.0, // "1" contains in dict => use category mean 1.0
              0.1, // "Moscow" not contains in dict => use global 0.1
              1.0 // "A" contains in dict => use category mean 1.0
          },
          {
              0.0, // "2" contains in dict => use category mean 0.0
              0.1, // "Moscow" not contains in dict => use global 0.1
              2.0 // "B" contains in dict => use category mean 2.0
          },
          {
              0.5, // "3" not contains in dict => use global mean 0.5
              0.1, // "Moscow" not contains in dict => use global 0.1
              2.0 // "B" contains in dict => use category mean 2.0
          },
      };

        for (int i = 0; i < data.length; i++)
            assertArrayEquals(postProcessedData[i], preprocessor.apply(i, data[i]).features().asArray(), 1e-8);
    }
}
