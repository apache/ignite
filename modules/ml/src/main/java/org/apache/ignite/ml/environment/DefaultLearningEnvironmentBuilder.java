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
import org.apache.ignite.ml.environment.logging.NoOpLogger;
import org.apache.ignite.ml.environment.parallelism.DefaultParallelismStrategy;
import org.apache.ignite.ml.environment.parallelism.NoParallelismStrategy;
import org.apache.ignite.ml.environment.parallelism.ParallelismStrategy;

/**
 * Builder for LearningEnvironment.
 */
public class DefaultLearningEnvironmentBuilder implements LearningEnvironmentBuilder {
    /** Parallelism strategy. */
    private ParallelismStrategy parallelismStgy;
    /** Logging factory. */
    private MLLogger.Factory loggingFactory;
    /** Random number generator seed. */
    private long seed;

    /**
     * Creates an instance of DefaultLearningEnvironmentBuilder.
     */
    DefaultLearningEnvironmentBuilder() {
        parallelismStgy = NoParallelismStrategy.INSTANCE;
        loggingFactory = NoOpLogger.factory();
        this.seed = new Random().nextLong();
    }

    @Override public LearningEnvironmentBuilder withRNGSeed(long seed) {
        this.seed = seed;

        return this;
    }

    /** {@inheritDoc} */
    @Override public DefaultLearningEnvironmentBuilder withParallelismStrategy(ParallelismStrategy stgy) {
        this.parallelismStgy = stgy;

        return this;
    }

    /** {@inheritDoc} */
    @Override public DefaultLearningEnvironmentBuilder withParallelismStrategy(ParallelismStrategy.Type stgyType) {
        switch (stgyType) {
            case NO_PARALLELISM:
                this.parallelismStgy = NoParallelismStrategy.INSTANCE;
                break;
            case ON_DEFAULT_POOL:
                this.parallelismStgy = new DefaultParallelismStrategy();
                break;
        }
        return this;
    }


    /** {@inheritDoc} */
    @Override public DefaultLearningEnvironmentBuilder withLoggingFactory(MLLogger.Factory loggingFactory) {
        this.loggingFactory = loggingFactory;
        return this;
    }

    /** {@inheritDoc} */
    @Override public LearningEnvironment buildForWorker(int part) {
        return new LearningEnvironmentImpl(part, seed, parallelismStgy, loggingFactory);
    }

    /**
     * Default LearningEnvironment implementation.
     */
    private class LearningEnvironmentImpl implements LearningEnvironment {
        /** Parallelism strategy. */
        private final ParallelismStrategy parallelismStgy;
        /** Logging factory. */
        private final MLLogger.Factory loggingFactory;
        /** Partition. */
        private final int part;
        /** Random numbers generator. */
        private final Random randomNumGen;

        /**
         * Creates an instance of LearningEnvironmentImpl.
         *
         * @param part Partition.
         * @param parallelismStgy Parallelism strategy.
         * @param loggingFactory Logging factory.
         */
        private LearningEnvironmentImpl(
            int part,
            long seed,
            ParallelismStrategy parallelismStgy,
            MLLogger.Factory loggingFactory) {
            this.part = part;
            this.parallelismStgy = parallelismStgy;
            this.loggingFactory = loggingFactory;
            randomNumGen = new Random(seed + part);
        }

        /** {@inheritDoc} */
        @Override public ParallelismStrategy parallelismStrategy() {
            return parallelismStgy;
        }

        /** {@inheritDoc} */
        @Override public MLLogger logger() {
            return loggingFactory.create(getClass());
        }

        /** {@inheritDoc} */
        @Override public Random randomNumbersGenerator() {
            return randomNumGen;
        }

        /** {@inheritDoc} */
        @Override public <T> MLLogger logger(Class<T> clazz) {
            return loggingFactory.create(clazz);
        }

        /** {@inheritDoc} */
        @Override public int partition() {
            return part;
        }
    }
}
