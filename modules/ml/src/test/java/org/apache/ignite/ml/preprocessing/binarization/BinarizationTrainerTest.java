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

package org.apache.ignite.ml.preprocessing.binarization;

import java.util.HashMap;
import java.util.Map;
import org.apache.ignite.ml.TestUtils;
import org.apache.ignite.ml.common.TrainerTest;
import org.apache.ignite.ml.dataset.DatasetBuilder;
import org.apache.ignite.ml.dataset.feature.extractor.Vectorizer;
import org.apache.ignite.ml.dataset.feature.extractor.impl.DummyVectorizer;
import org.apache.ignite.ml.dataset.impl.local.LocalDatasetBuilder;
import org.apache.ignite.ml.math.functions.IgniteBiFunction;
import org.apache.ignite.ml.math.primitives.vector.Vector;
import org.apache.ignite.ml.math.primitives.vector.VectorUtils;
import org.junit.Test;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;

/**
 * Tests for {@link BinarizationTrainer}.
 */
public class BinarizationTrainerTest extends TrainerTest {
    /** Tests {@code fit()} method. */
    @Test
    public void testFit() {
        Map<Integer, Vector> data = new HashMap<>();
        data.put(1, VectorUtils.of(2, 4, 1));
        data.put(2, VectorUtils.of(1, 8, 22));
        data.put(3, VectorUtils.of(4, 10, 100));
        data.put(4, VectorUtils.of(0, 22, 300));

        final Vectorizer<Integer, Vector, Integer, Double> vectorizer = new DummyVectorizer<>(0, 1, 2);

        DatasetBuilder<Integer, Vector> datasetBuilder = new LocalDatasetBuilder<>(data, parts);

        BinarizationTrainer<Integer, Vector> binarizationTrainer = new BinarizationTrainer<Integer, Vector>()
            .withThreshold(10);

        assertEquals(10., binarizationTrainer.getThreshold(), 0);

        BinarizationPreprocessor<Integer, Vector> preprocessor = binarizationTrainer.fit(
            TestUtils.testEnvBuilder(),
            datasetBuilder,
            vectorizer
        );

        assertEquals(binarizationTrainer.getThreshold(), preprocessor.getThreshold(), 0);

        assertArrayEquals(new double[] {0, 0, 1}, preprocessor.apply(5, VectorUtils.of(1, 10, 100)).features().asArray(), 1e-8);
    }

    /** Tests default implementation of {@code fit()} method. */
    @Test
    public void testFitDefault() {
        Map<Integer, Vector> data = new HashMap<>();
        data.put(1, VectorUtils.of(2, 4, 1));
        data.put(2, VectorUtils.of(1, 8, 22));
        data.put(3, VectorUtils.of(4, 10, 100));
        data.put(4, VectorUtils.of(0, 22, 300));

        final Vectorizer<Integer, Vector, Integer, Double> vectorizer = new DummyVectorizer<>(0, 1, 2);

        BinarizationTrainer<Integer, Vector> binarizationTrainer = new BinarizationTrainer<Integer, Vector>()
            .withThreshold(10);

        assertEquals(10., binarizationTrainer.getThreshold(), 0);

        BinarizationPreprocessor<Integer, Vector> preprocessor = (BinarizationPreprocessor<Integer, Vector>)binarizationTrainer.fit(
            TestUtils.testEnvBuilder(),
            data,
            parts,
            vectorizer
        );

        assertArrayEquals(new double[] {0, 0, 1}, preprocessor.apply(5, VectorUtils.of(1, 10, 100)).features().asArray(), 1e-8);
    }
}
