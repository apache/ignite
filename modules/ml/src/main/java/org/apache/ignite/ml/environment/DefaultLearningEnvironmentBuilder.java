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
import org.apache.ignite.ml.environment.deploy.DeployingContext;
import org.apache.ignite.ml.environment.logging.MLLogger;
import org.apache.ignite.ml.environment.logging.NoOpLogger;
import org.apache.ignite.ml.environment.parallelism.DefaultParallelismStrategy;
import org.apache.ignite.ml.environment.parallelism.NoParallelismStrategy;
import org.apache.ignite.ml.environment.parallelism.ParallelismStrategy;
import org.apache.ignite.ml.math.functions.IgniteFunction;

import static org.apache.ignite.ml.math.functions.IgniteFunction.constant;

/**
 * Builder for {@link LearningEnvironment}.
 */
public class DefaultLearningEnvironmentBuilder implements LearningEnvironmentBuilder {
    /** Serial version id. */
    private static final long serialVersionUID = 8502532880517447662L;

    /** Default partition data TTL (infinite). */
    private static final long INFINITE_TTL = -1;

    /** Dependency (partition -> Parallelism strategy). */
    private IgniteFunction<Integer, ParallelismStrategy> parallelismStgy;

    /** Dependency (partition -> Logging factory). */
    private IgniteFunction<Integer, MLLogger.Factory> loggingFactory;

    /** Dependency (partition -> Random number generator seed). */
    private IgniteFunction<Integer, Long> seed;

    /** Dependency (partition -> Random numbers generator supplier). */
    private IgniteFunction<Integer, Random> rngSupplier;

    /** Partition data time-to-live in seconds (-1 for an infinite lifetime). */
    private long dataTtl;

    /**
     * Creates an instance of DefaultLearningEnvironmentBuilder.
     */
    DefaultLearningEnvironmentBuilder() {
        parallelismStgy = constant(NoParallelismStrategy.INSTANCE);
        loggingFactory = constant(NoOpLogger.factory());
        seed = constant(new Random().nextLong());
        rngSupplier = p -> new Random();
        dataTtl = INFINITE_TTL;
    }

    /** {@inheritDoc} */
    @Override public LearningEnvironmentBuilder withRNGSeedDependency(IgniteFunction<Integer, Long> seed) {
        this.seed = seed;

        return this;
    }

    /** {@inheritDoc} */
    @Override public LearningEnvironmentBuilder withRandomDependency(IgniteFunction<Integer, Random> rngSupplier) {
        this.rngSupplier = rngSupplier;

        return this;
    }

    /** {@inheritDoc} */
    @Override public LearningEnvironmentBuilder withDataTtl(long dataTtl) {
        this.dataTtl = dataTtl;

        return this;
    }

    /** {@inheritDoc} */
    @Override public DefaultLearningEnvironmentBuilder withParallelismStrategyDependency(
        IgniteFunction<Integer, ParallelismStrategy> stgy) {
        this.parallelismStgy = stgy;

        return this;
    }

    /** {@inheritDoc} */
    @Override public DefaultLearningEnvironmentBuilder withParallelismStrategyTypeDependency(
        IgniteFunction<Integer, ParallelismStrategy.Type> stgyType) {
        this.parallelismStgy = part -> strategyByType(stgyType.apply(part));

        return this;
    }

    /**
     * Get parallelism strategy by {@link ParallelismStrategy.Type}.
     *
     * @param stgyType Strategy type.
     * @return {@link ParallelismStrategy}.
     */
    private static ParallelismStrategy strategyByType(ParallelismStrategy.Type stgyType) {
        switch (stgyType) {
            case NO_PARALLELISM:
                return NoParallelismStrategy.INSTANCE;
            case ON_DEFAULT_POOL:
                return new DefaultParallelismStrategy();
        }
        throw new IllegalStateException("Wrong type");
    }


    /** {@inheritDoc} */
    @Override public DefaultLearningEnvironmentBuilder withLoggingFactoryDependency(
        IgniteFunction<Integer, MLLogger.Factory> loggingFactory) {
        this.loggingFactory = loggingFactory;
        return this;
    }

    /** {@inheritDoc} */
    @Override public LearningEnvironment buildForWorker(int part) {
        Random random = rngSupplier.apply(part);
        random.setSeed(seed.apply(part));
        return new LearningEnvironmentImpl(part, dataTtl, random, parallelismStgy.apply(part), loggingFactory.apply(part));
    }

    /** Default LearningEnvironment implementation. */
    private class LearningEnvironmentImpl implements LearningEnvironment {
        /** Parallelism strategy. */
        private final ParallelismStrategy parallelismStgy;

        /** Logging factory. */
        private final MLLogger.Factory loggingFactory;

        /** Partition. */
        private final int part;

        /** Partition data time-to-live in seconds (-1 for an infinite lifetime). */
        private final long dataTtl;

        /** Random numbers generator. */
        private final Random randomNumGen;

        /** Deploy context. */
        private final DeployingContext deployingCtx = DeployingContext.unitialized();

        /**
         * Creates an instance of LearningEnvironmentImpl.
         *
         * @param part Partition.
         * @param dataTtl Partition data time-to-live in seconds (-1 for an infinite lifetime).
         * @param rng Random numbers generator.
         * @param parallelismStgy Parallelism strategy.
         * @param loggingFactory Logging factory.
         */
        private LearningEnvironmentImpl(
            int part,
            long dataTtl,
            Random rng,
            ParallelismStrategy parallelismStgy,
            MLLogger.Factory loggingFactory) {
            this.part = part;
            this.dataTtl = dataTtl;
            this.parallelismStgy = parallelismStgy;
            this.loggingFactory = loggingFactory;
            randomNumGen = rng;
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

        /** {@inheritDoc} */
        @Override public long dataTtl() {
            return dataTtl;
        }

        /** {@inheritDoc} */
        @Override public DeployingContext deployingContext() {
            return deployingCtx;
        }
    }
}
