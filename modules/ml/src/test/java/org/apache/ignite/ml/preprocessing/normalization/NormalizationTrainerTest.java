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

package org.apache.ignite.ml.preprocessing.normalization;

import java.util.HashMap;
import java.util.Map;
import org.apache.ignite.ml.TestUtils;
import org.apache.ignite.ml.common.TrainerTest;
import org.apache.ignite.ml.dataset.DatasetBuilder;
import org.apache.ignite.ml.dataset.feature.extractor.Vectorizer;
import org.apache.ignite.ml.dataset.feature.extractor.impl.DummyVectorizer;
import org.apache.ignite.ml.dataset.impl.local.LocalDatasetBuilder;
import org.apache.ignite.ml.math.primitives.vector.Vector;
import org.apache.ignite.ml.math.primitives.vector.VectorUtils;
import org.apache.ignite.ml.preprocessing.binarization.BinarizationTrainer;
import org.junit.Test;

import static org.apache.ignite.ml.TestUtils.assertEquals;
import static org.junit.Assert.assertArrayEquals;

/**
 * Tests for {@link BinarizationTrainer}.
 */
public class NormalizationTrainerTest extends TrainerTest {
    /** Tests {@code fit()} method. */
    @Test
    public void testFit() {
        Map<Integer, Vector> data = new HashMap<>();
        data.put(1, VectorUtils.of(2, 4, 1));
        data.put(2, VectorUtils.of(1, 8, 22));
        data.put(3, VectorUtils.of(4, 10, 100));
        data.put(4, VectorUtils.of(0, 22, 300));

        DatasetBuilder<Integer, Vector> datasetBuilder = new LocalDatasetBuilder<>(data, parts);

        final Vectorizer<Integer, Vector, Integer, Double> vectorizer = new DummyVectorizer<>(0, 1, 2);

        NormalizationTrainer<Integer, Vector> normalizationTrainer = new NormalizationTrainer<Integer, Vector>()
            .withP(3);

        assertEquals(3., normalizationTrainer.p(), 0);

        NormalizationPreprocessor<Integer, Vector> preprocessor = normalizationTrainer.fit(
            TestUtils.testEnvBuilder(),
            datasetBuilder,
            vectorizer
        );

        assertEquals(normalizationTrainer.p(), preprocessor.p(), 0);

        assertArrayEquals(new double[] {0.125, 0.99, 0.125}, preprocessor.apply(5, VectorUtils.of(1., 8., 1.)).features().asArray(), 1e-2);
    }
}
