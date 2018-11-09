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

package org.apache.ignite.ml.preprocessing.imputing;

import java.util.HashMap;
import java.util.Map;
import org.apache.ignite.ml.common.TrainerTest;
import org.apache.ignite.ml.dataset.DatasetBuilder;
import org.apache.ignite.ml.dataset.impl.local.LocalDatasetBuilder;
import org.apache.ignite.ml.math.primitives.vector.Vector;
import org.apache.ignite.ml.math.primitives.vector.VectorUtils;
import org.junit.Test;

import static org.junit.Assert.assertArrayEquals;

/**
 * Tests for {@link ImputerTrainer}.
 */
public class ImputerTrainerTest extends TrainerTest {
    /** Tests {@code fit()} method. */
    @Test
    public void testFit() {
        Map<Integer, Vector> data = new HashMap<>();
        data.put(1, VectorUtils.of(1, 2, Double.NaN));
        data.put(2, VectorUtils.of(1, Double.NaN, 22));
        data.put(3, VectorUtils.of(Double.NaN, 10, 100));
        data.put(4, VectorUtils.of(0, 2, 100));

        DatasetBuilder<Integer, Vector> datasetBuilder = new LocalDatasetBuilder<>(data, parts);

        ImputerTrainer<Integer, Vector> imputerTrainer = new ImputerTrainer<Integer, Vector>()
            .withImputingStrategy(ImputingStrategy.MOST_FREQUENT);

        ImputerPreprocessor<Integer, Vector> preprocessor = imputerTrainer.fit(
            datasetBuilder,
            (k, v) -> v
        );

        assertArrayEquals(new double[] {1, 0, 100}, preprocessor.apply(5, VectorUtils.of(Double.NaN, 0, Double.NaN)).asArray(), 1e-8);
    }
}
