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

package org.apache.ignite.ml.environment;

import java.util.Map;
import java.util.Random;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import org.apache.ignite.ml.Model;
import org.apache.ignite.ml.TestUtils;
import org.apache.ignite.ml.dataset.Dataset;
import org.apache.ignite.ml.dataset.DatasetBuilder;
import org.apache.ignite.ml.dataset.PartitionDataBuilder;
import org.apache.ignite.ml.dataset.feature.FeatureMeta;
import org.apache.ignite.ml.dataset.primitive.builder.context.EmptyContextBuilder;
import org.apache.ignite.ml.dataset.primitive.context.EmptyContext;
import org.apache.ignite.ml.environment.logging.ConsoleLogger;
import org.apache.ignite.ml.environment.logging.MLLogger;
import org.apache.ignite.ml.environment.parallelism.DefaultParallelismStrategy;
import org.apache.ignite.ml.environment.parallelism.ParallelismStrategy;
import org.apache.ignite.ml.math.functions.IgniteBiFunction;
import org.apache.ignite.ml.math.functions.IgniteFunction;
import org.apache.ignite.ml.math.functions.IgniteSupplier;
import org.apache.ignite.ml.math.primitives.vector.Vector;
import org.apache.ignite.ml.math.primitives.vector.VectorUtils;
import org.apache.ignite.ml.trainers.DatasetTrainer;
import org.apache.ignite.ml.tree.randomforest.RandomForestRegressionTrainer;
import org.apache.ignite.ml.tree.randomforest.data.FeaturesCountSelectionStrategies;
import org.junit.Test;

import static org.junit.Assert.assertEquals;

/**
 * Tests for {@link LearningEnvironment} that require to start the whole Ignite infrastructure. IMPL NOTE based on
 * RandomForestRegressionExample example.
 */
public class LearningEnvironmentTest {
    /** */
    @Test
    public void testBasic() throws InterruptedException {
        RandomForestRegressionTrainer trainer = new RandomForestRegressionTrainer(
            IntStream.range(0, 0).mapToObj(
                x -> new FeatureMeta("", 0, false)).collect(Collectors.toList())
        ).withAmountOfTrees(101)
            .withFeaturesCountSelectionStrgy(FeaturesCountSelectionStrategies.ONE_THIRD)
            .withMaxDepth(4)
            .withMinImpurityDelta(0.)
            .withSubSampleSize(0.3)
            .withSeed(0);

        LearningEnvironmentBuilder envBuilder = LearningEnvironmentBuilder.defaultBuilder()
            .withParallelismStrategyType(ParallelismStrategy.Type.ON_DEFAULT_POOL)
            .withLoggingFactory(ConsoleLogger.factory(MLLogger.VerboseLevel.LOW));

        trainer.setEnvironmentBuilder(envBuilder);

        assertEquals(DefaultParallelismStrategy.class, trainer.learningEnvironment().parallelismStrategy().getClass());
        assertEquals(ConsoleLogger.class, trainer.learningEnvironment().logger().getClass());
    }

    /**
     * Test random number generator provided by  {@link LearningEnvironment}.
     */
    @Test
    public void testRandomNumbersGenerator() {
        // We make such builders that provide as functions returning partition index as random number generator nextInt
        LearningEnvironmentBuilder envBuilder = getBuilder();
        int partitions = 10;
        int iterations = 2;

        DatasetTrainer<Model<Object, Vector>, Void> trainer = new DatasetTrainer<Model<Object, Vector>, Void>() {
            /** {@inheritDoc} */
            @Override public <K, V> Model<Object, Vector> fit(DatasetBuilder<K, V> datasetBuilder,
                IgniteBiFunction<K, V, Vector> featureExtractor, IgniteBiFunction<K, V, Void> lbExtractor) {
                Dataset<EmptyContext, TestUtils.DataWrapper<Integer>> ds = datasetBuilder.build(envBuilder,
                    new EmptyContextBuilder<>(),
                    (PartitionDataBuilder<K, V, EmptyContext, TestUtils.DataWrapper<Integer>>)(env, upstreamData, upstreamDataSize, ctx) ->
                        TestUtils.DataWrapper.of(env.partition()));

                Vector v = null;
                for (int iter = 0; iter < iterations; iter++) {
                    v = ds.compute((dw, env) -> VectorUtils.fill(-1, partitions).set(env.partition(), env.randomNumbersGenerator().nextInt()),
                        (v1, v2) -> v1 != null ? (v2 != null ? VectorUtils.zipWith(v1, v2, (d1, d2) -> d1 != -1 ? d1 : d2) : v1) : v2);
                }
                return Model.constantModel(v);
            }

            /** {@inheritDoc} */
            @Override protected boolean checkState(Model<Object, Vector> mdl) {
                return false;
            }

            /** {@inheritDoc} */
            @Override protected <K, V> Model<Object, Vector> updateModel(Model<Object, Vector> mdl,
                DatasetBuilder<K, V> datasetBuilder,
                IgniteBiFunction<K, V, Vector> featureExtractor, IgniteBiFunction<K, V, Void> lbExtractor) {
                return null;
            }
        };
        trainer.setEnvironmentBuilder(envBuilder);
        Model<Object, Vector> mdl = trainer.fit(getCacheMock(partitions), partitions, null, null);

        Vector expected = VectorUtils.zeroes(partitions);
        for (int i = 0; i < partitions; i++) {
            expected.set(i, i * iterations);
        }

        Vector result = mdl.apply(null);
        assertEquals(expected, result);
    }

    private Map<Integer, Integer> getCacheMock(int partsCount) {
        return IntStream.range(0, partsCount).boxed().collect(Collectors.toMap(x -> x, x -> x));
    }

    private LearningEnvironmentBuilder getBuilder() {
        return new PartitionDependentLearningEnvironmentBuilder(part -> TestUtils.testEnvBuilder().withRNGSupplier(() -> new MockRandom(part)));
    }

    private static class PartitionDependentLearningEnvironmentBuilder implements LearningEnvironmentBuilder {
        /** Dependency between partition and {@link LearningEnvironmentBuilder} which should be used on it. */
        private IgniteFunction<Integer, LearningEnvironmentBuilder> builderDep;

        public PartitionDependentLearningEnvironmentBuilder(IgniteFunction<Integer, LearningEnvironmentBuilder> builderDep) {
            this.builderDep = builderDep;
        }

        @Override public LearningEnvironment buildForWorker(int part) {
            return builderDep.apply(part).buildForWorker(part);
        }

        @Override public LearningEnvironmentBuilder withParallelismStrategyType(ParallelismStrategy.Type stgyType) {
            return compose(x -> x.withParallelismStrategyType(stgyType));
        }

        @Override public LearningEnvironmentBuilder withParallelismStrategy(ParallelismStrategy stgy) {
            return compose(x -> x.withParallelismStrategy(stgy));
        }

        @Override public LearningEnvironmentBuilder withLoggingFactory(MLLogger.Factory loggingFactory) {
            return compose(x -> x.withLoggingFactory(loggingFactory));
        }

        @Override public LearningEnvironmentBuilder withRNGSeed(long seed) {
            return compose(x -> x.withRNGSeed(seed));
        }

        @Override public LearningEnvironmentBuilder withRNGSupplier(IgniteSupplier<Random> rngSupplier) {
            return compose(x -> x.withRNGSupplier(rngSupplier));
        }

        private PartitionDependentLearningEnvironmentBuilder compose(IgniteFunction<LearningEnvironmentBuilder, LearningEnvironmentBuilder> other) {
            builderDep.andThen(other);

            return this;
        }
    }

    private static class MockRandom extends Random {
        private int startConzt;
        private int iter;

        public MockRandom(int startConzt) {
            this.startConzt = startConzt;
            iter = 0;
        }

        @Override public int nextInt() {
            iter++;
            return startConzt * iter;
        }
    }
}

