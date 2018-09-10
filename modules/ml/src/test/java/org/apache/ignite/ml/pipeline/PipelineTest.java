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

package org.apache.ignite.ml.pipeline;

import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import org.apache.ignite.ml.TestUtils;
import org.apache.ignite.ml.common.TrainerTest;
import org.apache.ignite.ml.math.primitives.vector.Vector;
import org.apache.ignite.ml.math.primitives.vector.VectorUtils;
import org.apache.ignite.ml.nn.UpdatesStrategy;
import org.apache.ignite.ml.optimization.updatecalculators.SimpleGDParameterUpdate;
import org.apache.ignite.ml.optimization.updatecalculators.SimpleGDUpdateCalculator;
import org.apache.ignite.ml.preprocessing.minmaxscaling.MinMaxScalerTrainer;
import org.apache.ignite.ml.preprocessing.normalization.NormalizationTrainer;
import org.apache.ignite.ml.regressions.logistic.binomial.LogisticRegressionSGDTrainer;
import org.junit.Test;

/**
 * Tests for {@link Pipeline}.
 */
public class PipelineTest extends TrainerTest {
    /**
     * Test trainer on classification model y = x.
     */
    @Test
    public void testTrainWithTheLinearlySeparableCase() {
        Map<Integer, Double[]> cacheMock = new HashMap<>();

        for (int i = 0; i < twoLinearlySeparableClasses.length; i++) {
            double[] row = twoLinearlySeparableClasses[i];
            Double[] convertedRow = new Double[row.length];
            for (int j = 0; j < row.length; j++)
                convertedRow[j] = row[j];
            cacheMock.put(i, convertedRow);
        }

        LogisticRegressionSGDTrainer<?> trainer = new LogisticRegressionSGDTrainer<>(new UpdatesStrategy<>(
            new SimpleGDUpdateCalculator().withLearningRate(0.2),
            SimpleGDParameterUpdate::sumLocal,
            SimpleGDParameterUpdate::avg
        ), 100000, 10, 100, 123L);

        PipelineMdl<Integer, Double[]> mdl = new Pipeline<Integer, Double[], Vector>()
            .addFeatureExtractor((k, v) -> VectorUtils.of(Arrays.copyOfRange(v, 1, v.length)))
            .addLabelExtractor((k, v) -> v[0])
            .addPreprocessor(new MinMaxScalerTrainer<Integer, Object[]>())
            .addPreprocessor(new NormalizationTrainer<Integer, Object[]>()
                .withP(1))
            .addTrainer(trainer)
            .fit(
                cacheMock,
                parts
            );

        TestUtils.assertEquals(0, mdl.apply(VectorUtils.of(100, 10)), PRECISION);
        TestUtils.assertEquals(1, mdl.apply(VectorUtils.of(10, 100)), PRECISION);
    }

    /**
     * Test the missed final state.
     */
    @Test(expected = IllegalStateException.class)
    public void testTrainWithMissedFinalStage() {
        Map<Integer, Double[]> cacheMock = new HashMap<>();

        for (int i = 0; i < twoLinearlySeparableClasses.length; i++) {
            double[] row = twoLinearlySeparableClasses[i];
            Double[] convertedRow = new Double[row.length];
            for (int j = 0; j < row.length; j++)
                convertedRow[j] = row[j];
            cacheMock.put(i, convertedRow);
        }

        LogisticRegressionSGDTrainer<?> trainer = new LogisticRegressionSGDTrainer<>(new UpdatesStrategy<>(
            new SimpleGDUpdateCalculator().withLearningRate(0.2),
            SimpleGDParameterUpdate::sumLocal,
            SimpleGDParameterUpdate::avg
        ), 100000, 10, 100, 123L);

        PipelineMdl<Integer, Double[]> mdl = new Pipeline<Integer, Double[], Vector>()
            .addFeatureExtractor((k, v) -> VectorUtils.of(Arrays.copyOfRange(v, 1, v.length)))
            .addLabelExtractor((k, v) -> v[0])
            .addPreprocessor(new MinMaxScalerTrainer<Integer, Object[]>())
            .addPreprocessor(new NormalizationTrainer<Integer, Object[]>()
                .withP(1))
            .fit(
                cacheMock,
                parts
            );

        TestUtils.assertEquals(0, mdl.apply(VectorUtils.of(100, 10)), PRECISION);
        TestUtils.assertEquals(1, mdl.apply(VectorUtils.of(10, 100)), PRECISION);
    }
}
