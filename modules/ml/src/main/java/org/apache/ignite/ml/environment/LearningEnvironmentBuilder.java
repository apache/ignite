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

import java.util.Random;
import org.apache.ignite.ml.environment.logging.MLLogger;
import org.apache.ignite.ml.environment.parallelism.ParallelismStrategy;
import org.apache.ignite.ml.math.functions.IgniteFunction;
import org.apache.ignite.ml.math.functions.IgniteSupplier;

public interface LearningEnvironmentBuilder {
    /**
     * Builds {@link LearningEnvironment} for worker on given partition.
     *
     * @param part Partition.
     * @return {@link LearningEnvironment} for worker on given partition.
     */
    default public LearningEnvironment buildForWorker(int part) {
        return buildForTrainer();
    }

    /**
     * Builds learning environment for trainer.
     *
     * @return Learning environment for trainer.
     */
    default public LearningEnvironment buildForTrainer() {
        return buildForWorker(-1);
    }

    /**
     * Specifies Parallelism Strategy Type for LearningEnvironment.
     *
     * @param stgyType Parallelism Strategy Type.
     * @return This object.
     */
    public LearningEnvironmentBuilder withParallelismStrategyType(ParallelismStrategy.Type stgyType);

    /**
     * Specifies Parallelism Strategy for LearningEnvironment.
     *
     * @param stgy Parallelism Strategy.
     * @return This object.
     */
    public LearningEnvironmentBuilder withParallelismStrategy(ParallelismStrategy stgy);

    /**
     * Specify logging factory.
     *
     * @param loggingFactory Logging factory.
     * @return This object.
     */
    public LearningEnvironmentBuilder withLoggingFactory(MLLogger.Factory loggingFactory);

    /**
     * Specify seed for random number generator.
     *
     * @param seed Seed for random number generator.
     * @return This object.
     */
    public LearningEnvironmentBuilder withRNGSeed(long seed);

    /**
     * Specify supplier of random numbers generator.
     *
     * @param rngSupplier Supplier of random numbers generator.
     * @return This object.
     */
    public LearningEnvironmentBuilder withRNGSupplier(IgniteSupplier<Random> rngSupplier);

    /**
     * Get default {@link LearningEnvironmentBuilder}.
     *
     * @return Default {@link LearningEnvironmentBuilder}.
     */
    public static LearningEnvironmentBuilder defaultBuilder() {
        return new DefaultLearningEnvironmentBuilder();
    }
}
