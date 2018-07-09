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
import org.apache.ignite.ml.environment.parallelism.ParallelismStrategy;

public class LearningEnvironmentBuilder {
    private ParallelismStrategy parallelismStgy;
    private MLLogger.Factory loggingFactory;

    LearningEnvironmentBuilder() {
        parallelismStgy = new DefaultParallelismStrategy();
        loggingFactory = NoOpLogger.FACTORY;
    }

    public LearningEnvironmentBuilder withParallelismStrategy(ParallelismStrategy stgy) {
        this.parallelismStgy = stgy;
        return this;
    }

    public LearningEnvironmentBuilder withLoggingFactory(MLLogger.Factory loggingFactory) {
        this.loggingFactory = loggingFactory;
        return this;
    }

    public LearningEnvironmentBuilder withParallelismStrategy(Class<? extends ParallelismStrategy> clazz) {
        try {
            this.parallelismStgy = clazz.newInstance();
            return this;
        } catch (InstantiationException | IllegalAccessException e) {
            throw new RuntimeException(e);
        }
    }

    public LearningEnvironment build() {
        return new LearningEnvironmentImpl(parallelismStgy, loggingFactory);
    }

    private class LearningEnvironmentImpl implements LearningEnvironment {
        private final ParallelismStrategy parallelismStrategy;
        private final MLLogger.Factory loggingFactory;

        private LearningEnvironmentImpl(ParallelismStrategy parallelismStrategy,
            MLLogger.Factory loggingFactory) {
            this.parallelismStrategy = parallelismStrategy;
            this.loggingFactory = loggingFactory;
        }

        @Override public ParallelismStrategy parallelismStgy() {
            return parallelismStrategy;
        }

        @Override public MLLogger logger() {
            return loggingFactory.create(getClass());
        }

        @Override public <T> MLLogger logger(Class<T> clazz) {
            return loggingFactory.create(clazz);
        }
    }
}
