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

import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import org.apache.ignite.ml.dataset.DatasetBuilder;
import org.apache.ignite.ml.dataset.impl.local.LocalDatasetBuilder;
import org.apache.ignite.ml.preprocessing.binarization.BinarizationPreprocessor;
import org.apache.ignite.ml.preprocessing.binarization.BinarizationTrainer;
import org.apache.ignite.ml.preprocessing.imputer.ImputerPreprocessor;
import org.apache.ignite.ml.preprocessing.imputer.ImputerTrainer;
import org.apache.ignite.ml.preprocessing.imputer.ImputingStrategy;
import org.apache.ignite.ml.preprocessing.normalization.NormalizationTrainer;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import static org.junit.Assert.assertArrayEquals;

/**
 * Tests for {@link ImputerTrainer}.
 */
@RunWith(Parameterized.class)
public class ImputerTrainerTest {
    /** Parameters. */
    @Parameterized.Parameters(name = "Data divided on {0} partitions")
    public static Iterable<Integer[]> data() {
        return Arrays.asList(
            new Integer[] {1},
            new Integer[] {2},
            new Integer[] {3},
            new Integer[] {5},
            new Integer[] {7},
            new Integer[] {100},
            new Integer[] {1000}
        );
    }

    /** Number of partitions. */
    @Parameterized.Parameter
    public int parts;

    /** Tests {@code fit()} method. */
    @Test
    public void testFit() {
        Map<Integer, double[]> data = new HashMap<>();
        data.put(1, new double[] {1, 2, Double.NaN,});
        data.put(2, new double[] {1, Double.NaN, 22});
        data.put(3, new double[] {Double.NaN, 10, 100});
        data.put(4, new double[] {0, 2, 100});

        DatasetBuilder<Integer, double[]> datasetBuilder = new LocalDatasetBuilder<>(data, parts);

        ImputerTrainer<Integer, double[]> imputerTrainer = new ImputerTrainer<Integer, double[]>()
            .withImputingStrategy(ImputingStrategy.MOST_FREQUENT);

        ImputerPreprocessor<Integer, double[]> preprocessor = imputerTrainer.fit(
            datasetBuilder,
            (k, v) -> v
        );

        assertArrayEquals(new double[] {1, 0, 100}, preprocessor.apply(5, new double[] {Double.NaN, 0, Double.NaN}), 1e-8);
    }
}
