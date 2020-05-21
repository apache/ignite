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

package org.apache.ignite.ml.dataset.primitive;

import java.util.HashMap;
import java.util.Map;
import org.apache.ignite.ml.TestUtils;
import org.apache.ignite.ml.dataset.DatasetFactory;
import org.apache.ignite.ml.dataset.feature.extractor.Vectorizer;
import org.apache.ignite.ml.dataset.feature.extractor.impl.DummyVectorizer;
import org.apache.ignite.ml.math.functions.IgniteFunction;
import org.apache.ignite.ml.math.primitives.vector.Vector;
import org.apache.ignite.ml.math.primitives.vector.VectorUtils;
import org.apache.ignite.ml.preprocessing.developer.PatchedPreprocessor;
import org.apache.ignite.ml.structures.LabeledVector;
import org.junit.Test;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertNull;

/**
 * Tests for {@link SimpleLabeledDataset}.
 */
public class SimpleLabeledDatasetTest {
    /** Basic test for SimpleLabeledDataset features. */
    @Test
    public void basicTest() throws Exception {
        Map<Integer, Vector> dataPoints = new HashMap<Integer, Vector>() {{
            put(5, VectorUtils.of(42, 10000));
            put(6, VectorUtils.of(32, 64000));
            put(7, VectorUtils.of(53, 120000));
            put(8, VectorUtils.of(24, 70000));
        }};

        double[][] actualFeatures = new double[2][];
        double[][] actualLabels = new double[2][];
        int[] actualRows = new int[2];

        Vectorizer<Integer, Vector, Integer, Double> vectorizer = new DummyVectorizer<Integer>().labeled(Vectorizer.LabelCoordinate.FIRST);

        IgniteFunction<LabeledVector<Double>, LabeledVector<double[]>> func = lv -> new LabeledVector<>(lv.features(), new double[] { lv.label()});

        PatchedPreprocessor<Integer, Vector, Double, double[]> patchedPreprocessor = new PatchedPreprocessor<>(func, vectorizer);

        // Creates a local simple dataset containing features and providing standard dataset API.
        try (SimpleLabeledDataset<?> dataset = DatasetFactory.createSimpleLabeledDataset(
            dataPoints,
            TestUtils.testEnvBuilder(),
            2,
            patchedPreprocessor
        )) {
            assertNull(dataset.compute((data, env) -> {
                int part = env.partition();
                actualFeatures[part] = data.getFeatures();
                actualLabels[part] = data.getLabels();
                actualRows[part] = data.getRows();
                return null;
            }, (k, v) -> null));
        }

        double[][] expFeatures = new double[][] {
            new double[] {10000.0, 64000.0},
            new double[] {120000.0, 70000.0}
        };
        int rowFeat = 0;
        for (double[] row : actualFeatures)
            assertArrayEquals("Features partition index " + rowFeat,
                expFeatures[rowFeat++], row, 0);

        double[][] expLabels = new double[][] {
            new double[] {42.0, 32.0},
            new double[] {53.0, 24.0}
        };
        int rowLbl = 0;
        for (double[] row : actualLabels)
            assertArrayEquals("Labels partition index " + rowLbl,
                expLabels[rowLbl++], row, 0);

        assertArrayEquals("Rows per partitions", new int[] {2, 2}, actualRows);
    }
}
