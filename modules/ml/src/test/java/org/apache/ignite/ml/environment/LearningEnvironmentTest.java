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
import org.apache.ignite.ml.IgniteModel;
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
import org.apache.ignite.ml.math.primitives.vector.Vector;
import org.apache.ignite.ml.math.primitives.vector.VectorUtils;
import org.apache.ignite.ml.preprocessing.Preprocessor;
import org.apache.ignite.ml.trainers.DatasetTrainer;
import org.apache.ignite.ml.tree.randomforest.RandomForestRegressionTrainer;
import org.apache.ignite.ml.tree.randomforest.data.FeaturesCountSelectionStrategies;
import org.junit.Test;

import static org.apache.ignite.ml.TestUtils.constantModel;
import static org.junit.Assert.assertEquals;

/**
 * Tests for {@link LearningEnvironment} that require to start the whole Ignite infrastructure. IMPL NOTE based on
 * RandomForestRegressionExample example.
 */
public class LearningEnvironmentTest {
    /** */
    @Test
    public void testBasic() {
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
            .withLoggingFactoryDependency(part -> ConsoleLogger.factory(MLLogger.VerboseLevel.LOW));

        trainer.withEnvironmentBuilder(envBuilder);

        assertEquals(DefaultParallelismStrategy.class, trainer.learningEnvironment().parallelismStrategy().getClass());
        assertEquals(ConsoleLogger.class, trainer.learningEnvironment().logger().getClass());
    }

    /**
     * Test random number generator provided by  {@link LearningEnvironment}.
     * We test that:
     * 1. Correct random generator is returned for each partition.
     * 2. Its state is saved between compute calls (for this we do several iterations of compute).
     */
    @Test
    public void testRandomNumbersGenerator() {
        // We make such builders that provide as functions returning partition index * iteration as random number generator nextInt
        LearningEnvironmentBuilder envBuilder = TestUtils.testEnvBuilder().withRandomDependency(MockRandom::new);
        int partitions = 10;
        int iterations = 2;

        DatasetTrainer<IgniteModel<Object, Vector>, Void> trainer = new DatasetTrainer<IgniteModel<Object, Vector>, Void>() {
            /** {@inheritDoc} */
             @Override public <K, V> IgniteModel<Object, Vector> fitWithInitializedDeployingContext(DatasetBuilder<K, V> datasetBuilder, Preprocessor<K, V> preprocessor) {
                Dataset<EmptyContext, TestUtils.DataWrapper<Integer>> ds = datasetBuilder.build(envBuilder,
                    new EmptyContextBuilder<>(),
                    (PartitionDataBuilder<K, V, EmptyContext, TestUtils.DataWrapper<Integer>>)(env, upstreamData, upstreamDataSize, ctx) ->
                        TestUtils.DataWrapper.of(env.partition()),
                    envBuilder.buildForTrainer());

                Vector v = null;
                for (int iter = 0; iter < iterations; iter++) {
                    v = ds.compute((dw, env) -> VectorUtils.fill(-1, partitions).set(env.partition(), env.randomNumbersGenerator().nextInt()),
                        (v1, v2) -> zipOverridingEmpty(v1, v2, -1));
                }
                return constantModel(v);
            }

            /** {@inheritDoc} */
            @Override public boolean isUpdateable(IgniteModel<Object, Vector> mdl) {
                return false;
            }

            /** {@inheritDoc} */
             @Override protected <K, V> IgniteModel<Object, Vector> updateModel(IgniteModel<Object, Vector> mdl,
                DatasetBuilder<K, V> datasetBuilder, Preprocessor<K, V> preprocessor) {
                return null;
            }
        };
        trainer.withEnvironmentBuilder(envBuilder);
        IgniteModel<Object, Vector> mdl = trainer.fit(getCacheMock(partitions), partitions, null);

        Vector exp = VectorUtils.zeroes(partitions);
        for (int i = 0; i < partitions; i++)
            exp.set(i, i * iterations);

        Vector res = mdl.predict(null);
        assertEquals(exp, res);
    }

    /**
     * For given two vectors {@code v2, v2} produce vector {@code v} where each component of {@code v}
     * is produced from corresponding components {@code c1, c2} of {@code v1, v2} respectfully in following way
     * {@code c = c1 != empty ? c1 : c2}. For example, zipping [2, -1, -1], [-1, 3, -1] will result in [2, 3, -1].
     *
     * @param v1 First vector.
     * @param v2 Second vector.
     * @param empty Value treated as empty.
     * @return Result of zipping as described above.
     */
    private static Vector zipOverridingEmpty(Vector v1, Vector v2, double empty) {
        return v1 != null ? (v2 != null ? VectorUtils.zipWith(v1, v2, (d1, d2) -> d1 != empty ? d1 : d2) : v1) : v2;
    }

    /** Get cache mock */
    private Map<Integer, Integer> getCacheMock(int partsCnt) {
        return IntStream.range(0, partsCnt).boxed().collect(Collectors.toMap(x -> x, x -> x));
    }

    /** Mock random numbers generator. */
    private static class MockRandom extends Random {
        /** Serial version uuid. */
        private static final long serialVersionUID = -7738558243461112988L;

        /** Start value. */
        private int startVal;

        /** Iteration. */
        private int iter;

        /**
         * Constructs instance of this class with a specified start value.
         *
         * @param startVal Start value.
         */
        MockRandom(int startVal) {
            this.startVal = startVal;
            iter = 0;
        }

        /** {@inheritDoc} */
        @Override public int nextInt() {
            iter++;
            return startVal * iter;
        }
    }
}

