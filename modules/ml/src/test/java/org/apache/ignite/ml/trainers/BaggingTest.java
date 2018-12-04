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

package org.apache.ignite.ml.trainers;

import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import org.apache.ignite.ml.Model;
import org.apache.ignite.ml.TestUtils;
import org.apache.ignite.ml.common.TrainerTest;
import org.apache.ignite.ml.composition.ModelsComposition;
import org.apache.ignite.ml.composition.predictionsaggregator.MeanValuePredictionsAggregator;
import org.apache.ignite.ml.composition.predictionsaggregator.OnMajorityPredictionsAggregator;
import org.apache.ignite.ml.dataset.Dataset;
import org.apache.ignite.ml.dataset.DatasetBuilder;
import org.apache.ignite.ml.environment.LearningEnvironment;
import org.apache.ignite.ml.environment.LearningEnvironmentBuilder;
import org.apache.ignite.ml.math.functions.IgniteBiFunction;
import org.apache.ignite.ml.math.functions.IgniteTriFunction;
import org.apache.ignite.ml.math.primitives.vector.Vector;
import org.apache.ignite.ml.math.primitives.vector.VectorUtils;
import org.apache.ignite.ml.nn.UpdatesStrategy;
import org.apache.ignite.ml.optimization.updatecalculators.SimpleGDParameterUpdate;
import org.apache.ignite.ml.optimization.updatecalculators.SimpleGDUpdateCalculator;
import org.apache.ignite.ml.regressions.logistic.binomial.LogisticRegressionModel;
import org.apache.ignite.ml.regressions.logistic.binomial.LogisticRegressionSGDTrainer;
import org.junit.Test;

/**
 * Tests for bagging algorithm.
 */
public class BaggingTest extends TrainerTest {
    /**
     * Test that count of entries in context is equal to initial dataset size * subsampleRatio.
     */
    @Test
    public void testBaggingContextCount() {
        count((ctxCount, countData, integer) -> ctxCount);
    }

    /**
     * Test that count of entries in data is equal to initial dataset size * subsampleRatio.
     */
    @Test
    public void testBaggingDataCount() {
        count((ctxCount, countData, integer) -> countData.cnt);
    }

    /**
     * Test that bagged log regression makes correct predictions.
     */
    @Test
    public void testNaiveBaggingLogRegression() {
        Map<Integer, Double[]> cacheMock = getCacheMock();

        DatasetTrainer<LogisticRegressionModel, Double> trainer =
            (LogisticRegressionSGDTrainer<?>)new LogisticRegressionSGDTrainer<>()
                .withUpdatesStgy(new UpdatesStrategy<>(new SimpleGDUpdateCalculator(0.2),
                    SimpleGDParameterUpdate::sumLocal, SimpleGDParameterUpdate::avg))
                .withMaxIterations(30000)
                .withLocIterations(100)
                .withBatchSize(10)
                .withSeed(123L);

        trainer.withEnvironmentBuilder(TestUtils.testEnvBuilder());

        DatasetTrainer<ModelsComposition, Double> baggedTrainer =
            TrainerTransformers.makeBagged(
                trainer,
                10,
                0.7,
                2,
                2,
                new OnMajorityPredictionsAggregator());

        ModelsComposition mdl = baggedTrainer.fit(
            cacheMock,
            parts,
            (k, v) -> VectorUtils.of(Arrays.copyOfRange(v, 1, v.length)),
            (k, v) -> v[0]
        );

        TestUtils.assertEquals(0, mdl.apply(VectorUtils.of(100, 10)), PRECISION);
        TestUtils.assertEquals(1, mdl.apply(VectorUtils.of(10, 100)), PRECISION);
    }

    /**
     * Method used to test counts of data passed in context and in data builders.
     *
     * @param counter Function specifying which data we should count.
     */
    protected void count(IgniteTriFunction<Long, CountData, LearningEnvironment, Long> counter) {
        Map<Integer, Double[]> cacheMock = getCacheMock();

        CountTrainer countTrainer = new CountTrainer(counter);

        double subsampleRatio = 0.3;

        ModelsComposition model = TrainerTransformers.makeBagged(
            countTrainer,
            100,
            subsampleRatio,
            2,
            2,
            new MeanValuePredictionsAggregator())
            .fit(cacheMock, parts, null, null);

        Double res = model.apply(null);

        TestUtils.assertEquals(twoLinearlySeparableClasses.length * subsampleRatio, res, twoLinearlySeparableClasses.length / 10);
    }

    /**
     * Create cache mock.
     *
     * @return Cache mock.
     */
    private Map<Integer, Double[]> getCacheMock() {
        Map<Integer, Double[]> cacheMock = new HashMap<>();

        for (int i = 0; i < twoLinearlySeparableClasses.length; i++) {
            double[] row = twoLinearlySeparableClasses[i];
            Double[] convertedRow = new Double[row.length];
            for (int j = 0; j < row.length; j++)
                convertedRow[j] = row[j];
            cacheMock.put(i, convertedRow);
        }
        return cacheMock;
    }

    /**
     * Get sum of two Long values each of which can be null.
     *
     * @param a First value.
     * @param b Second value.
     * @return Sum of parameters.
     */
    protected static Long plusOfNullables(Long a, Long b) {
        if (a == null)
            return b;

        if (b == null)
            return a;

        return a + b;
    }

    /**
     * Trainer used to count entries in context or in data.
     */
    protected static class CountTrainer extends DatasetTrainer<Model<Vector, Double>, Double> {
        /**
         * Function specifying which entries to count.
         */
        private final IgniteTriFunction<Long, CountData, LearningEnvironment, Long> counter;

        /**
         * Construct instance of this class.
         *
         * @param counter Function specifying which entries to count.
         */
        public CountTrainer(IgniteTriFunction<Long, CountData, LearningEnvironment, Long> counter) {
            this.counter = counter;
        }

        /** {@inheritDoc} */
        @Override public <K, V> Model<Vector, Double> fit(
            DatasetBuilder<K, V> datasetBuilder,
            IgniteBiFunction<K, V, Vector> featureExtractor,
            IgniteBiFunction<K, V, Double> lbExtractor) {
            Dataset<Long, CountData> dataset = datasetBuilder.build(
                TestUtils.testEnvBuilder(),
                (env, upstreamData, upstreamDataSize) -> upstreamDataSize,
                (env, upstreamData, upstreamDataSize, ctx) -> new CountData(upstreamDataSize)
            );

            Long cnt = dataset.computeWithCtx(counter, BaggingTest::plusOfNullables);

            return x -> Double.valueOf(cnt);
        }

        /** {@inheritDoc} */
        @Override protected boolean checkState(Model<Vector, Double> mdl) {
            return true;
        }

        /** {@inheritDoc} */
        @Override protected <K, V> Model<Vector, Double> updateModel(
            Model<Vector, Double> mdl,
            DatasetBuilder<K, V> datasetBuilder,
            IgniteBiFunction<K, V, Vector> featureExtractor, IgniteBiFunction<K, V, Double> lbExtractor) {
            return fit(datasetBuilder, featureExtractor, lbExtractor);
        }

        /** {@inheritDoc} */
        @Override public CountTrainer withEnvironmentBuilder(LearningEnvironmentBuilder envBuilder) {
            return (CountTrainer)super.withEnvironmentBuilder(envBuilder);
        }
    }

    /** Data for count trainer. */
    protected static class CountData implements AutoCloseable {
        /** Counter. */
        private long cnt;

        /**
         * Construct instance of this class.
         *
         * @param cnt Counter.
         */
        public CountData(long cnt) {
            this.cnt = cnt;
        }

        /** {@inheritDoc} */
        @Override public void close() throws Exception {
            // No-op
        }
    }
}
