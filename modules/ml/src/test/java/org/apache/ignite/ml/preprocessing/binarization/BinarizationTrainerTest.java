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

package org.apache.ignite.ml.preprocessing.binarization;

import java.util.HashMap;
import java.util.Map;
import org.apache.ignite.ml.common.TrainerTest;
import org.apache.ignite.ml.dataset.DatasetBuilder;
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
        Map<Integer, double[]> data = new HashMap<>();
        data.put(1, new double[] {2, 4, 1});
        data.put(2, new double[] {1, 8, 22});
        data.put(3, new double[] {4, 10, 100});
        data.put(4, new double[] {0, 22, 300});

        DatasetBuilder<Integer, double[]> datasetBuilder = new LocalDatasetBuilder<>(data, parts);

        BinarizationTrainer<Integer, double[]> binarizationTrainer = new BinarizationTrainer<Integer, double[]>()
            .withThreshold(10);

        assertEquals(10., binarizationTrainer.getThreshold(), 0);

        BinarizationPreprocessor<Integer, double[]> preprocessor = binarizationTrainer.fit(
            datasetBuilder,
            (k, v) -> VectorUtils.of(v)
        );

        assertEquals(binarizationTrainer.getThreshold(), preprocessor.getThreshold(), 0);

        assertArrayEquals(new double[] {0, 0, 1}, preprocessor.apply(5, new double[] {1, 10, 100}).asArray(), 1e-8);
    }

    /** Tests default implementation of {@code fit()} method. */
    @Test
    public void testFitDefault() {
        Map<Integer, double[]> data = new HashMap<>();
        data.put(1, new double[] {2, 4, 1});
        data.put(2, new double[] {1, 8, 22});
        data.put(3, new double[] {4, 10, 100});
        data.put(4, new double[] {0, 22, 300});

        BinarizationTrainer<Integer, double[]> binarizationTrainer = new BinarizationTrainer<Integer, double[]>()
            .withThreshold(10);

        assertEquals(10., binarizationTrainer.getThreshold(), 0);

        IgniteBiFunction<Integer, double[], Vector> preprocessor = binarizationTrainer.fit(
            data,
            parts,
            (k, v) -> VectorUtils.of(v)
        );

        assertArrayEquals(new double[] {0, 0, 1}, preprocessor.apply(5, new double[] {1, 10, 100}).asArray(), 1e-8);
    }
}
