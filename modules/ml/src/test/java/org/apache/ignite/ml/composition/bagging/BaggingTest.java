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

package org.apache.ignite.ml.composition.bagging;

import java.util.HashMap;
import java.util.Map;
import org.apache.ignite.ml.IgniteModel;
import org.apache.ignite.ml.TestUtils;
import org.apache.ignite.ml.common.TrainerTest;
import org.apache.ignite.ml.composition.combinators.parallel.ModelsParallelComposition;
import org.apache.ignite.ml.composition.predictionsaggregator.MeanValuePredictionsAggregator;
import org.apache.ignite.ml.composition.predictionsaggregator.OnMajorityPredictionsAggregator;
import org.apache.ignite.ml.dataset.Dataset;
import org.apache.ignite.ml.dataset.DatasetBuilder;
import org.apache.ignite.ml.dataset.feature.extractor.Vectorizer;
import org.apache.ignite.ml.dataset.feature.extractor.impl.DoubleArrayVectorizer;
import org.apache.ignite.ml.environment.LearningEnvironment;
import org.apache.ignite.ml.environment.LearningEnvironmentBuilder;
import org.apache.ignite.ml.math.functions.IgniteTriFunction;
import org.apache.ignite.ml.math.primitives.vector.Vector;
import org.apache.ignite.ml.math.primitives.vector.VectorUtils;
import org.apache.ignite.ml.nn.UpdatesStrategy;
import org.apache.ignite.ml.optimization.updatecalculators.SimpleGDParameterUpdate;
import org.apache.ignite.ml.optimization.updatecalculators.SimpleGDUpdateCalculator;
import org.apache.ignite.ml.preprocessing.Preprocessor;
import org.apache.ignite.ml.regressions.logistic.LogisticRegressionModel;
import org.apache.ignite.ml.regressions.logistic.LogisticRegressionSGDTrainer;
import org.apache.ignite.ml.trainers.AdaptableDatasetModel;
import org.apache.ignite.ml.trainers.DatasetTrainer;
import org.apache.ignite.ml.trainers.TrainerTransformers;
import org.junit.Test;

/**
 * Tests for bagging algorithm.
 */
public class BaggingTest extends TrainerTest {
    /**
     * Dependency of weights of first model in ensemble after training in
     * {@link BaggingTest#testNaiveBaggingLogRegression()}. This dependency is tested to ensure that it is
     * fully determined by provided seeds.
     */
    private static Map<Integer, Vector> firstMdlWeights;

    static {
        firstMdlWeights = new HashMap<>();

        firstMdlWeights.put(1, VectorUtils.of(-0.14721735583126058, 4.366377931980097));
        firstMdlWeights.put(2, VectorUtils.of(0.37824664453495443, 2.9422474282114495));
        firstMdlWeights.put(3, VectorUtils.of(-1.584467989609169, 2.8467326345685824));
        firstMdlWeights.put(4, VectorUtils.of(-2.543461229777167, 0.1317660102621108));
        firstMdlWeights.put(13, VectorUtils.of(-1.6329364937353634, 0.39278455436019116));
    }

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
        Map<Integer, double[]> cacheMock = getCacheMock(twoLinearlySeparableClasses);

        DatasetTrainer<LogisticRegressionModel, Double> trainer =
            new LogisticRegressionSGDTrainer()
                .withUpdatesStgy(new UpdatesStrategy<>(new SimpleGDUpdateCalculator(0.2),
                    SimpleGDParameterUpdate.SUM_LOCAL, SimpleGDParameterUpdate.AVG))
                .withMaxIterations(30000)
                .withLocIterations(100)
                .withBatchSize(10)
                .withSeed(123L);

        BaggedTrainer<Double> baggedTrainer = TrainerTransformers.makeBagged(
            trainer,
            7,
            0.7,
            2,
            2,
            new OnMajorityPredictionsAggregator())
            .withEnvironmentBuilder(TestUtils.testEnvBuilder());

        BaggedModel mdl = baggedTrainer.fit(
            cacheMock,
            parts,
            new DoubleArrayVectorizer<Integer>().labeled(Vectorizer.LabelCoordinate.FIRST)
        );

        Vector weights = ((LogisticRegressionModel)((AdaptableDatasetModel)((ModelsParallelComposition)((AdaptableDatasetModel)mdl
            .model()).innerModel()).submodels().get(0)).innerModel()).weights();

        TestUtils.assertEquals(firstMdlWeights.get(parts), weights, 0.0);
        TestUtils.assertEquals(0, mdl.predict(VectorUtils.of(100, 10)), PRECISION);
        TestUtils.assertEquals(1, mdl.predict(VectorUtils.of(10, 100)), PRECISION);
    }

    /**
     * Method used to test counts of data passed in context and in data builders.
     *
     * @param cntr Function specifying which data we should count.
     */
    protected void count(IgniteTriFunction<Long, CountData, LearningEnvironment, Long> cntr) {
        Map<Integer, double[]> cacheMock = getCacheMock(twoLinearlySeparableClasses);

        CountTrainer cntTrainer = new CountTrainer(cntr);

        double subsampleRatio = 0.3;

        BaggedModel mdl = TrainerTransformers.makeBagged(
            cntTrainer,
            100,
            subsampleRatio,
            2,
            2,
            new MeanValuePredictionsAggregator())
            .fit(cacheMock, parts, new DoubleArrayVectorizer<Integer>().labeled(Vectorizer.LabelCoordinate.LAST));

        Double res = mdl.predict(null);

        TestUtils.assertEquals(twoLinearlySeparableClasses.length * subsampleRatio, res, twoLinearlySeparableClasses.length / 10);
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
    protected static class CountTrainer extends DatasetTrainer<IgniteModel<Vector, Double>, Double> {
        /**
         * Function specifying which entries to count.
         */
        private final IgniteTriFunction<Long, CountData, LearningEnvironment, Long> cntr;

        /**
         * Construct instance of this class.
         *
         * @param cntr Function specifying which entries to count.
         */
        public CountTrainer(IgniteTriFunction<Long, CountData, LearningEnvironment, Long> cntr) {
            this.cntr = cntr;
        }

        /** {@inheritDoc} */
        @Override public <K, V> IgniteModel<Vector, Double> fitWithInitializedDeployingContext(
            DatasetBuilder<K, V> datasetBuilder,
            Preprocessor<K, V> extractor) {
            Dataset<Long, CountData> dataset = datasetBuilder.build(
                TestUtils.testEnvBuilder(),
                (env, upstreamData, upstreamDataSize) -> upstreamDataSize,
                (env, upstreamData, upstreamDataSize, ctx) -> new CountData(upstreamDataSize),
                TestUtils.testEnvBuilder().buildForTrainer()
            );

            Long cnt = dataset.computeWithCtx(cntr, BaggingTest::plusOfNullables);

            return x -> Double.valueOf(cnt);
        }

        /** {@inheritDoc} */
        @Override public boolean isUpdateable(IgniteModel<Vector, Double> mdl) {
            return true;
        }

        /** {@inheritDoc} */
        @Override protected <K, V> IgniteModel<Vector, Double> updateModel(
            IgniteModel<Vector, Double> mdl,
            DatasetBuilder<K, V> datasetBuilder,
            Preprocessor<K, V> extractor) {
            return fit(datasetBuilder, extractor);
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
        @Override public void close() {
            // No-op
        }
    }
}
