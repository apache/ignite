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
import org.apache.ignite.ml.TestUtils;
import org.apache.ignite.ml.common.TrainerTest;
import org.apache.ignite.ml.math.primitives.vector.VectorUtils;
import org.apache.ignite.ml.nn.UpdatesStrategy;
import org.apache.ignite.ml.optimization.SmoothParametrized;
import org.apache.ignite.ml.optimization.updatecalculators.SimpleGDParameterUpdate;
import org.apache.ignite.ml.optimization.updatecalculators.SimpleGDUpdateCalculator;
import org.apache.ignite.ml.regressions.logistic.multiclass.LogRegressionMultiClassModel;
import org.apache.ignite.ml.regressions.logistic.multiclass.LogRegressionMultiClassTrainer;
import org.junit.Assert;
import org.junit.Test;

/**
 * Tests for {@link LogRegressionMultiClassTrainer}.
 */
public class LogRegMultiClassTrainerTest extends TrainerTest {
    /**
     * Test trainer on 4 sets grouped around of square vertices.
     */
    @Test
    public void testTrainWithTheLinearlySeparableCase() {
        Map<Integer, double[]> cacheMock = new HashMap<>();

        for (int i = 0; i < fourSetsInSquareVertices.length; i++)
            cacheMock.put(i, fourSetsInSquareVertices[i]);

        final UpdatesStrategy<SmoothParametrized, SimpleGDParameterUpdate> stgy = new UpdatesStrategy<>(
            new SimpleGDUpdateCalculator(0.2),
            SimpleGDParameterUpdate::sumLocal,
            SimpleGDParameterUpdate::avg
        );

        LogRegressionMultiClassTrainer<?> trainer = new LogRegressionMultiClassTrainer<>()
            .withUpdatesStgy(stgy)
            .withAmountOfIterations(1000)
            .withAmountOfLocIterations(10)
            .withBatchSize(100)
            .withSeed(123L);

        Assert.assertEquals(trainer.amountOfIterations(), 1000);
        Assert.assertEquals(trainer.amountOfLocIterations(), 10);
        Assert.assertEquals(trainer.batchSize(), 100, PRECISION);
        Assert.assertEquals(trainer.seed(), 123L);
        Assert.assertEquals(trainer.updatesStgy(), stgy);

        LogRegressionMultiClassModel mdl = trainer.fit(
            cacheMock,
            parts,
            (k, v) -> VectorUtils.of(Arrays.copyOfRange(v, 1, v.length)),
            (k, v) -> v[0]
        );

        Assert.assertTrue(mdl.toString().length() > 0);
        Assert.assertTrue(mdl.toString(true).length() > 0);
        Assert.assertTrue(mdl.toString(false).length() > 0);

        TestUtils.assertEquals(1, mdl.apply(VectorUtils.of(10, 10)), PRECISION);
        TestUtils.assertEquals(1, mdl.apply(VectorUtils.of(-10, 10)), PRECISION);
        TestUtils.assertEquals(2, mdl.apply(VectorUtils.of(-10, -10)), PRECISION);
        TestUtils.assertEquals(3, mdl.apply(VectorUtils.of(10, -10)), PRECISION);
    }
}
