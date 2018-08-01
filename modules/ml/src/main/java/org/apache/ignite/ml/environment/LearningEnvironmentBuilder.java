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

import org.apache.ignite.ml.environment.logging.MLLogger;
import org.apache.ignite.ml.environment.logging.NoOpLogger;
import org.apache.ignite.ml.environment.parallelism.DefaultParallelismStrategy;
import org.apache.ignite.ml.environment.parallelism.NoParallelismStrategy;
import org.apache.ignite.ml.environment.parallelism.ParallelismStrategy;

/**
 * Builder for LearningEnvironment.
 */
public class LearningEnvironmentBuilder {
    /** Parallelism strategy. */
    private ParallelismStrategy parallelismStgy;
    /** Logging factory. */
    private MLLogger.Factory loggingFactory;

    /**
     * Creates an instance of LearningEnvironmentBuilder.
     */
    LearningEnvironmentBuilder() {
        parallelismStgy = NoParallelismStrategy.INSTANCE;
        loggingFactory = NoOpLogger.factory();
    }

    /**
     * Specifies Parallelism Strategy for LearningEnvironment.
     *
     * @param stgy Parallelism Strategy.
     */
    public <T> LearningEnvironmentBuilder withParallelismStrategy(ParallelismStrategy stgy) {
        this.parallelismStgy = stgy;

        return this;
    }

    /**
     * Specifies Parallelism Strategy for LearningEnvironment.
     *
     * @param stgyType Parallelism Strategy Type.
     */
    public LearningEnvironmentBuilder withParallelismStrategy(ParallelismStrategy.Type stgyType) {
        switch (stgyType) {
            case NO_PARALLELISM:
                this.parallelismStgy = NoParallelismStrategy.INSTANCE;
            case ON_DEFAULT_POOL:
                this.parallelismStgy = new DefaultParallelismStrategy();
        }
        return this;
    }


    /**
     * Specifies Logging factory for LearningEnvironment.
     *
     * @param loggingFactory Logging Factory.
     */
    public LearningEnvironmentBuilder withLoggingFactory(MLLogger.Factory loggingFactory) {
        this.loggingFactory = loggingFactory;
        return this;
    }

    /**
     * Create an instance of LearningEnvironment.
     */
    public LearningEnvironment build() {
        return new LearningEnvironmentImpl(parallelismStgy, loggingFactory);
    }

    /**
     * Default LearningEnvironment implementation.
     */
    private class LearningEnvironmentImpl implements LearningEnvironment {
        /** Parallelism strategy. */
        private final ParallelismStrategy parallelismStgy;
        /** Logging factory. */
        private final MLLogger.Factory loggingFactory;

        /**
         * Creates an instance of LearningEnvironmentImpl.
         *
         * @param parallelismStgy Parallelism strategy.
         * @param loggingFactory Logging factory.
         */
        private LearningEnvironmentImpl(ParallelismStrategy parallelismStgy,
            MLLogger.Factory loggingFactory) {
            this.parallelismStgy = parallelismStgy;
            this.loggingFactory = loggingFactory;
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
        @Override public <T> MLLogger logger(Class<T> clazz) {
            return loggingFactory.create(clazz);
        }
    }
}
