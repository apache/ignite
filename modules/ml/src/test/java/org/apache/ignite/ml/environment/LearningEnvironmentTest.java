/*
 * Copyright 2019 GridGain Systems, Inc. and Contributors.
 * 
 * Licensed under the GridGain Community Edition License (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * 
 *     https://www.gridgain.com/products/software/community-edition/gridgain-community-edition-license
 * 
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.ignite.ml.environment;

import java.util.stream.Collectors;
import java.util.stream.IntStream;
import org.apache.ignite.ml.dataset.feature.FeatureMeta;
import org.apache.ignite.ml.environment.logging.ConsoleLogger;
import org.apache.ignite.ml.environment.logging.MLLogger;
import org.apache.ignite.ml.environment.parallelism.DefaultParallelismStrategy;
import org.apache.ignite.ml.environment.parallelism.ParallelismStrategy;
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

        LearningEnvironment environment = LearningEnvironment.builder()
            .withParallelismStrategy(ParallelismStrategy.Type.ON_DEFAULT_POOL)
            .withLoggingFactory(ConsoleLogger.factory(MLLogger.VerboseLevel.LOW))
            .build();
        trainer.setEnvironment(environment);
        assertEquals(DefaultParallelismStrategy.class, environment.parallelismStrategy().getClass());
        assertEquals(ConsoleLogger.class, environment.logger().getClass());
    }
}

