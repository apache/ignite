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

import org.apache.ignite.logger.NullLogger;
import org.apache.ignite.ml.environment.logging.ConsoleLogger;
import org.apache.ignite.ml.environment.logging.CustomMLLogger;
import org.apache.ignite.ml.environment.logging.MLLogger;
import org.apache.ignite.ml.environment.logging.NoOpLogger;
import org.apache.ignite.ml.environment.parallelism.DefaultParallelismStrategy;
import org.apache.ignite.ml.environment.parallelism.NoParallelismStrategy;
import org.junit.Test;

import static org.apache.ignite.ml.environment.parallelism.ParallelismStrategy.Type.NO_PARALLELISM;
import static org.apache.ignite.ml.environment.parallelism.ParallelismStrategy.Type.ON_DEFAULT_POOL;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

/**
 * Tests for {@link LearningEnvironmentBuilder}.
 */
public class LearningEnvironmentBuilderTest {
    /** */
    @Test
    public void basic() {
        LearningEnvironment env = LearningEnvironment.DEFAULT_TRAINER_ENV;

        assertNotNull("Strategy", env.parallelismStrategy());
        assertNotNull("Logger", env.logger());
        assertNotNull("Logger for class", env.logger(this.getClass()));
    }

    /** */
    @Test
    public void withParallelismStrategy() {
        assertTrue(LearningEnvironmentBuilder.defaultBuilder().withParallelismStrategyDependency(part -> NoParallelismStrategy.INSTANCE)
            .buildForTrainer()
            .parallelismStrategy() instanceof NoParallelismStrategy);

        assertTrue(LearningEnvironmentBuilder.defaultBuilder().withParallelismStrategyDependency(part -> new DefaultParallelismStrategy())
            .buildForTrainer()
            .parallelismStrategy() instanceof DefaultParallelismStrategy);
    }

    /** */
    @Test
    public void withParallelismStrategyType() {
        assertTrue(LearningEnvironmentBuilder.defaultBuilder().withParallelismStrategyType(NO_PARALLELISM).buildForTrainer()
            .parallelismStrategy() instanceof NoParallelismStrategy);

        assertTrue(LearningEnvironmentBuilder.defaultBuilder().withParallelismStrategyType(ON_DEFAULT_POOL).buildForTrainer()
            .parallelismStrategy() instanceof DefaultParallelismStrategy);
    }

    /** */
    @Test
    public void withLoggingFactory() {
        assertTrue(LearningEnvironmentBuilder.defaultBuilder().withLoggingFactoryDependency(part -> ConsoleLogger.factory(MLLogger.VerboseLevel.HIGH))
            .buildForTrainer().logger() instanceof ConsoleLogger);

        assertTrue(LearningEnvironmentBuilder.defaultBuilder().withLoggingFactoryDependency(part -> ConsoleLogger.factory(MLLogger.VerboseLevel.HIGH))
            .buildForTrainer().logger(this.getClass()) instanceof ConsoleLogger);

        assertTrue(LearningEnvironmentBuilder.defaultBuilder().withLoggingFactoryDependency(part -> NoOpLogger.factory())
            .buildForTrainer().logger() instanceof NoOpLogger);

        assertTrue(LearningEnvironmentBuilder.defaultBuilder().withLoggingFactoryDependency(part -> NoOpLogger.factory())
            .buildForTrainer().logger(this.getClass()) instanceof NoOpLogger);

        assertTrue(LearningEnvironmentBuilder.defaultBuilder().withLoggingFactoryDependency(part -> CustomMLLogger.factory(new NullLogger()))
            .buildForTrainer().logger() instanceof CustomMLLogger);

        assertTrue(LearningEnvironmentBuilder.defaultBuilder().withLoggingFactoryDependency(part -> CustomMLLogger.factory(new NullLogger()))
            .buildForTrainer().logger(this.getClass()) instanceof CustomMLLogger);
    }
}
