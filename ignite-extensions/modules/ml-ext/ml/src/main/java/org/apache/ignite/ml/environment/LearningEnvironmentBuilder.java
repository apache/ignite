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

import java.io.Serializable;
import java.util.Random;
import org.apache.ignite.ml.environment.logging.MLLogger;
import org.apache.ignite.ml.environment.parallelism.ParallelismStrategy;
import org.apache.ignite.ml.math.functions.IgniteFunction;

import static org.apache.ignite.ml.math.functions.IgniteFunction.constant;

/**
 * Builder of learning environment.
 */
public interface LearningEnvironmentBuilder extends Serializable {
    /**
     * Builds {@link LearningEnvironment} for worker on given partition.
     *
     * @param part Partition.
     * @return {@link LearningEnvironment} for worker on given partition.
     */
    public LearningEnvironment buildForWorker(int part);

    /**
     * Builds learning environment for trainer.
     *
     * @return Learning environment for trainer.
     */
    public default LearningEnvironment buildForTrainer() {
        return buildForWorker(-1);
    }

    /**
     * Specifies dependency (partition -> Parallelism Strategy Type for LearningEnvironment).
     *
     * @param stgyType Function describing dependency (partition -> Parallelism Strategy Type).
     * @return This object.
     */
    public LearningEnvironmentBuilder withParallelismStrategyTypeDependency(
        IgniteFunction<Integer, ParallelismStrategy.Type> stgyType);

    /**
     * Specifies Parallelism Strategy Type for LearningEnvironment. Same strategy type will be used for all partitions.
     *
     * @param stgyType Parallelism Strategy Type.
     * @return This object.
     */
    public default LearningEnvironmentBuilder withParallelismStrategyType(ParallelismStrategy.Type stgyType) {
        return withParallelismStrategyTypeDependency(constant(stgyType));
    }

    /**
     * Specifies dependency (partition -> Parallelism Strategy for LearningEnvironment).
     *
     * @param stgy Function describing dependency (partition -> Parallelism Strategy).
     * @return This object.
     */
    public LearningEnvironmentBuilder withParallelismStrategyDependency(IgniteFunction<Integer, ParallelismStrategy> stgy);

    /**
     * Specifies Parallelism Strategy for LearningEnvironment. Same strategy type will be used for all partitions.
     *
     * @param stgy Parallelism Strategy.
     * @param <T> Parallelism strategy type.
     * @return This object.
     */
    public default <T extends ParallelismStrategy & Serializable> LearningEnvironmentBuilder withParallelismStrategy(T stgy) {
        return withParallelismStrategyDependency(constant(stgy));
    }

    /**
     * Specify dependency (partition -> logging factory).
     *
     * @param loggingFactory Function describing (partition -> logging factory).
     * @return This object.
     */
    public LearningEnvironmentBuilder withLoggingFactoryDependency(IgniteFunction<Integer, MLLogger.Factory> loggingFactory);

    /**
     * Specify logging factory.
     *
     * @param loggingFactory Logging factory.
     * @return This object.
     */
    public default <T extends MLLogger.Factory & Serializable> LearningEnvironmentBuilder withLoggingFactory(T loggingFactory) {
        return withLoggingFactoryDependency(constant(loggingFactory));
    }

    /**
     * Specify dependency (partition -> seed for random number generator). Same seed will be used for all partitions.
     *
     * @param seed Function describing dependency (partition -> seed for random number generator).
     * @return This object.
     */
    public LearningEnvironmentBuilder withRNGSeedDependency(IgniteFunction<Integer, Long> seed);

    /**
     * Specify seed for random number generator.
     *
     * @param seed Seed for random number generator.
     * @return This object.
     */
    public default LearningEnvironmentBuilder withRNGSeed(long seed) {
        return withRNGSeedDependency(constant(seed));
    }

    /**
     * Specify dependency (partition -> random numbers generator).
     *
     * @param rngSupplier Function describing dependency (partition -> random numbers generator).
     * @return This object.
     */
    public LearningEnvironmentBuilder withRandomDependency(IgniteFunction<Integer, Random> rngSupplier);

    /**
     * Specify random numbers generator for learning environment. Same random will be used for all partitions.
     *
     * @param random Rrandom numbers generator for learning environment.
     * @return This object.
     */
    public default LearningEnvironmentBuilder withRandom(Random random) {
        return withRandomDependency(constant(random));
    }

    /**
     * Specify partition data time-to-live in seconds (-1 for an infinite lifetime).
     *
     * @param dataTtl Partition data time-to-live in seconds (-1 for an infinite lifetime).
     * @return This object.
     */
    public LearningEnvironmentBuilder withDataTtl(long dataTtl);

    /**
     * Get default {@link LearningEnvironmentBuilder}.
     *
     * @return Default {@link LearningEnvironmentBuilder}.
     */
    public static LearningEnvironmentBuilder defaultBuilder() {
        return new DefaultLearningEnvironmentBuilder();
    }
}
