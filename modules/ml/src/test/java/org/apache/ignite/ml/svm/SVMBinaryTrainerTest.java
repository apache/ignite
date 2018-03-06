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

package org.apache.ignite.ml.svm;

import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ThreadLocalRandom;
import org.apache.ignite.ml.TestUtils;
import org.apache.ignite.ml.dataset.impl.local.LocalDatasetBuilder;
import org.apache.ignite.ml.math.impls.vector.DenseLocalOnHeapVector;
import org.junit.Test;

/**
 * Tests for {@link SVMLinearBinaryClassificationTrainer}.
 */
public class SVMBinaryTrainerTest {
    /** Fixed size of Dataset. */
    private static final int AMOUNT_OF_OBSERVATIONS = 1000;

    /** Fixed size of columns in Dataset. */
    private static final int AMOUNT_OF_FEATURES = 2;

    /**
     * Test trainer on classification model y = x.
     */
    @Test
    public void testTrainWithTheLinearlySeparableCase() {
        Map<Integer, double[]> data = new HashMap<>();


        ThreadLocalRandom rndX = ThreadLocalRandom.current();
        ThreadLocalRandom rndY = ThreadLocalRandom.current();

        for (int i = 0; i < AMOUNT_OF_OBSERVATIONS; i++) {
            double x = rndX.nextDouble(-1000, 1000);
            double y = rndY.nextDouble(-1000, 1000);
            double[] vec = new double[AMOUNT_OF_FEATURES + 1];
            vec[0] = y - x > 0 ? 1 : -1; // assign label.
            vec[1] = x;
            vec[2] = y;
            data.put(i, vec);
        }


        SVMLinearBinaryClassificationTrainer<Integer, double[]> trainer = new SVMLinearBinaryClassificationTrainer<>();

        SVMLinearBinaryClassificationModel mdl = trainer.fit(
            new LocalDatasetBuilder<>(data, 10),
            (k, v) -> Arrays.copyOfRange(v, 1, v.length),
            (k, v) -> v[0],
            AMOUNT_OF_FEATURES);

        double precision = 1e-2;

        TestUtils.assertEquals(-1, mdl.apply(new DenseLocalOnHeapVector(new double[]{100, 10})), precision);
        TestUtils.assertEquals(1, mdl.apply(new DenseLocalOnHeapVector(new double[]{10, 100})), precision);
    }
}
