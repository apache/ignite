/*
 *                   GridGain Community Edition Licensing
 *                   Copyright 2019 GridGain Systems, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License") modified with Commons Clause
 * Restriction; you may not use this file except in compliance with the License. You may obtain a
 * copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the
 * License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied. See the License for the specific language governing permissions
 * and limitations under the License.
 *
 * Commons Clause Restriction
 *
 * The Software is provided to you by the Licensor under the License, as defined below, subject to
 * the following condition.
 *
 * Without limiting other conditions in the License, the grant of rights under the License will not
 * include, and the License does not grant to you, the right to Sell the Software.
 * For purposes of the foregoing, “Sell” means practicing any or all of the rights granted to you
 * under the License to provide to third parties, for a fee or other consideration (including without
 * limitation fees for hosting or consulting/ support services related to the Software), a product or
 * service whose value derives, entirely or substantially, from the functionality of the Software.
 * Any license notice or attribution required by the License must also include this Commons Clause
 * License Condition notice.
 *
 * For purposes of the clause above, the “Licensor” is Copyright 2019 GridGain Systems, Inc.,
 * the “License” is the Apache License, Version 2.0, and the Software is the GridGain Community
 * Edition software provided with this notice.
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
                break;
            case ON_DEFAULT_POOL:
                this.parallelismStgy = new DefaultParallelismStrategy();
                break;
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
