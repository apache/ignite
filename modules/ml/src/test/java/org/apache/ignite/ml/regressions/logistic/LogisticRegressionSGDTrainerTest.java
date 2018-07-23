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

package org.apache.ignite.ml.regressions.logistic;

import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ThreadLocalRandom;
import org.apache.ignite.ml.TestUtils;
import org.apache.ignite.ml.math.primitives.vector.VectorUtils;
import org.apache.ignite.ml.math.primitives.vector.impl.DenseVector;
import org.apache.ignite.ml.nn.UpdatesStrategy;
import org.apache.ignite.ml.optimization.updatecalculators.SimpleGDParameterUpdate;
import org.apache.ignite.ml.optimization.updatecalculators.SimpleGDUpdateCalculator;
import org.apache.ignite.ml.regressions.logistic.binomial.LogisticRegressionModel;
import org.apache.ignite.ml.regressions.logistic.binomial.LogisticRegressionSGDTrainer;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

/**
 * Tests for {@LogisticRegressionSGDTrainer}.
 */
@RunWith(Parameterized.class)
public class LogisticRegressionSGDTrainerTest {
    /** Fixed size of Dataset. */
    private static final int AMOUNT_OF_OBSERVATIONS = 1000;

    /** Fixed size of columns in Dataset. */
    private static final int AMOUNT_OF_FEATURES = 2;

    /** Precision in test checks. */
    private static final double PRECISION = 1e-2;

    /** Parameters. */
    @Parameterized.Parameters(name = "Data divided on {0} partitions")
    public static Iterable<Integer[]> data() {
        return Arrays.asList(
            new Integer[] {1},
            new Integer[] {2},
            new Integer[] {3},
            new Integer[] {5},
            new Integer[] {7},
            new Integer[] {100}
        );
    }

    /** Number of partitions. */
    @Parameterized.Parameter
    public int parts;

    /**
     * Test trainer on classification model y = x.
     */
    @Test
    public void trainWithTheLinearlySeparableCase() {
        Map<Integer, double[]> data = new HashMap<>();

        ThreadLocalRandom rndX = ThreadLocalRandom.current();
        ThreadLocalRandom rndY = ThreadLocalRandom.current();

        for (int i = 0; i < AMOUNT_OF_OBSERVATIONS; i++) {
            double x = rndX.nextDouble(-1000, 1000);
            double y = rndY.nextDouble(-1000, 1000);
            double[] vec = new double[AMOUNT_OF_FEATURES + 1];
            vec[0] = y - x > 0 ? 1 : 0; // assign label.
            vec[1] = x;
            vec[2] = y;
            data.put(i, vec);
        }

        LogisticRegressionSGDTrainer<?> trainer = new LogisticRegressionSGDTrainer<>(new UpdatesStrategy<>(
            new SimpleGDUpdateCalculator(0.2),
            SimpleGDParameterUpdate::sumLocal,
            SimpleGDParameterUpdate::avg
        ), 100000,  10, 100, 123L);

        LogisticRegressionModel mdl = trainer.fit(
            data,
            10,
            (k, v) -> VectorUtils.of(Arrays.copyOfRange(v, 1, v.length)),
            (k, v) -> v[0]
        );

        TestUtils.assertEquals(0, mdl.apply(new DenseVector(new double[]{100, 10})), PRECISION);
        TestUtils.assertEquals(1, mdl.apply(new DenseVector(new double[]{10, 100})), PRECISION);
    }
}
