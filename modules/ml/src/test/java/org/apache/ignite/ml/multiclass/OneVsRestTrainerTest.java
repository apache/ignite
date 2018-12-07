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

package org.apache.ignite.ml.multiclass;

import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.ignite.ml.TestUtils;
import org.apache.ignite.ml.common.TrainerTest;
import org.apache.ignite.ml.math.primitives.vector.Vector;
import org.apache.ignite.ml.math.primitives.vector.VectorUtils;
import org.apache.ignite.ml.nn.UpdatesStrategy;
import org.apache.ignite.ml.optimization.updatecalculators.SimpleGDParameterUpdate;
import org.apache.ignite.ml.optimization.updatecalculators.SimpleGDUpdateCalculator;
import org.apache.ignite.ml.regressions.logistic.LogisticRegressionModel;
import org.apache.ignite.ml.regressions.logistic.LogisticRegressionSGDTrainer;
import org.junit.Assert;
import org.junit.Test;

/**
 * Tests for {@link OneVsRestTrainer}.
 */
public class OneVsRestTrainerTest extends TrainerTest {
    /**
     * Test trainer on 2 linearly separable sets.
     */
    @Test
    public void testTrainWithTheLinearlySeparableCase() {
        Map<Integer, double[]> cacheMock = new HashMap<>();

        for (int i = 0; i < twoLinearlySeparableClasses.length; i++)
            cacheMock.put(i, twoLinearlySeparableClasses[i]);

        LogisticRegressionSGDTrainer<?> binaryTrainer = new LogisticRegressionSGDTrainer<>()
            .withUpdatesStgy(new UpdatesStrategy<>(new SimpleGDUpdateCalculator(0.2),
                SimpleGDParameterUpdate::sumLocal, SimpleGDParameterUpdate::avg))
            .withMaxIterations(1000)
            .withLocIterations(10)
            .withBatchSize(100)
            .withSeed(123L);

        OneVsRestTrainer<LogisticRegressionModel> trainer = new OneVsRestTrainer<>(binaryTrainer);

        MultiClassModel mdl = trainer.fit(
            cacheMock,
            parts,
            (k, v) -> VectorUtils.of(Arrays.copyOfRange(v, 1, v.length)),
            (k, v) -> v[0]
        );

        Assert.assertTrue(mdl.toString().length() > 0);
        Assert.assertTrue(mdl.toString(true).length() > 0);
        Assert.assertTrue(mdl.toString(false).length() > 0);

        TestUtils.assertEquals(1, mdl.apply(VectorUtils.of(-100, 0)), PRECISION);
        TestUtils.assertEquals(0, mdl.apply(VectorUtils.of(100, 0)), PRECISION);
    }

    /** */
    @Test
    public void testUpdate() {
        Map<Integer, double[]> cacheMock = new HashMap<>();

        for (int i = 0; i < twoLinearlySeparableClasses.length; i++)
            cacheMock.put(i, twoLinearlySeparableClasses[i]);

        LogisticRegressionSGDTrainer<?> binaryTrainer = new LogisticRegressionSGDTrainer<>()
            .withUpdatesStgy(new UpdatesStrategy<>(new SimpleGDUpdateCalculator(0.2),
                SimpleGDParameterUpdate::sumLocal, SimpleGDParameterUpdate::avg))
            .withMaxIterations(1000)
            .withLocIterations(10)
            .withBatchSize(100)
            .withSeed(123L);

        OneVsRestTrainer<LogisticRegressionModel> trainer = new OneVsRestTrainer<>(binaryTrainer);

        MultiClassModel originalMdl = trainer.fit(
            cacheMock,
            parts,
            (k, v) -> VectorUtils.of(Arrays.copyOfRange(v, 1, v.length)),
            (k, v) -> v[0]
        );

        MultiClassModel updatedOnSameDS = trainer.update(
            originalMdl,
            cacheMock,
            parts,
            (k, v) -> VectorUtils.of(Arrays.copyOfRange(v, 1, v.length)),
            (k, v) -> v[0]
        );

        MultiClassModel updatedOnEmptyDS = trainer.update(
            originalMdl,
            new HashMap<Integer, double[]>(),
            parts,
            (k, v) -> VectorUtils.of(Arrays.copyOfRange(v, 1, v.length)),
            (k, v) -> v[0]
        );

        List<Vector> vectors = Arrays.asList(
            VectorUtils.of(-100, 0),
            VectorUtils.of(100, 0)
        );

        for (Vector vec : vectors) {
            TestUtils.assertEquals(originalMdl.apply(vec), updatedOnSameDS.apply(vec), PRECISION);
            TestUtils.assertEquals(originalMdl.apply(vec), updatedOnEmptyDS.apply(vec), PRECISION);
        }
    }
}
