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

package org.apache.ignite.ml.preprocessing.standardscaling;

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
import org.junit.Before;
import org.junit.Test;

import static org.junit.Assert.assertArrayEquals;

/**
 * Tests for {@link StandardScalerTrainer}.
 */
public class StandardScalerTrainerTest extends TrainerTest {
    /** Data. */
    private DatasetBuilder<Integer, Vector> datasetBuilder;

    /** Trainer to be tested. */
    private StandardScalerTrainer<Integer, Vector> standardizationTrainer;

    /** */
    @Before
    public void prepareDataset() {
        Map<Integer, Vector> data = new HashMap<>();
        data.put(1, VectorUtils.of(0, 2., 4., .1));
        data.put(2, VectorUtils.of(0, 1., -18., 2.2));
        data.put(3, VectorUtils.of(1, 4., 10., -.1));
        data.put(4, VectorUtils.of(1, 0., 22., 1.3));
        datasetBuilder = new LocalDatasetBuilder<>(data, parts);
    }

    /** */
    @Before
    public void createTrainer() {
        standardizationTrainer = new StandardScalerTrainer<>();
    }

    /** Test {@code fit()} method. */
    @Test
    public void testCalculatesCorrectMeans() {
        double[] expectedMeans = new double[] {0.5, 1.75, 4.5, 0.875};

        final Vectorizer<Integer, Vector, Integer, Double> vectorizer = new DummyVectorizer<>(0, 1, 2, 3);

        StandardScalerPreprocessor<Integer, Vector> preprocessor = standardizationTrainer.fit(
            TestUtils.testEnvBuilder(),
            datasetBuilder,
            vectorizer
        );

        assertArrayEquals(expectedMeans, preprocessor.getMeans(), 1e-8);
    }

    /** Test {@code fit()} method. */
    @Test
    public void testCalculatesCorrectStandardDeviations() {
        double[] expectedSigmas = new double[] {0.5, 1.47901995, 14.51723114, 0.93374247};

        final Vectorizer<Integer, Vector, Integer, Double> vectorizer = new DummyVectorizer<>(0, 1, 2, 3);

        StandardScalerPreprocessor<Integer, Vector> preprocessor = standardizationTrainer.fit(
            TestUtils.testEnvBuilder(),
            datasetBuilder,
            vectorizer
        );

        assertArrayEquals(expectedSigmas, preprocessor.getSigmas(), 1e-8);
    }
}
