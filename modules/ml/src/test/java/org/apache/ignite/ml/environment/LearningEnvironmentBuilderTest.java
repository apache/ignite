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
        LearningEnvironment env = LearningEnvironment.DEFAULT;

        assertNotNull("Strategy", env.parallelismStrategy());
        assertNotNull("Logger", env.logger());
        assertNotNull("Logger for class", env.logger(this.getClass()));
    }

    /** */
    @Test
    public void withParallelismStrategy() {
        assertTrue(LearningEnvironment.builder().withParallelismStrategy(NoParallelismStrategy.INSTANCE).build()
            .parallelismStrategy() instanceof NoParallelismStrategy);

        assertTrue(LearningEnvironment.builder().withParallelismStrategy(new DefaultParallelismStrategy()).build()
            .parallelismStrategy() instanceof DefaultParallelismStrategy);
    }

    /** */
    @Test
    public void withParallelismStrategyType() {
        assertTrue(LearningEnvironment.builder().withParallelismStrategy(NO_PARALLELISM).build()
            .parallelismStrategy() instanceof NoParallelismStrategy);

        assertTrue(LearningEnvironment.builder().withParallelismStrategy(ON_DEFAULT_POOL).build()
            .parallelismStrategy() instanceof DefaultParallelismStrategy);
    }

    /** */
    @Test
    public void withLoggingFactory() {
        assertTrue(LearningEnvironment.builder().withLoggingFactory(ConsoleLogger.factory(MLLogger.VerboseLevel.HIGH))
            .build().logger() instanceof ConsoleLogger);

        assertTrue(LearningEnvironment.builder().withLoggingFactory(ConsoleLogger.factory(MLLogger.VerboseLevel.HIGH))
            .build().logger(this.getClass()) instanceof ConsoleLogger);

        assertTrue(LearningEnvironment.builder().withLoggingFactory(NoOpLogger.factory())
            .build().logger() instanceof NoOpLogger);

        assertTrue(LearningEnvironment.builder().withLoggingFactory(NoOpLogger.factory())
            .build().logger(this.getClass()) instanceof NoOpLogger);

        assertTrue(LearningEnvironment.builder().withLoggingFactory(CustomMLLogger.factory(new NullLogger()))
            .build().logger() instanceof CustomMLLogger);

        assertTrue(LearningEnvironment.builder().withLoggingFactory(CustomMLLogger.factory(new NullLogger()))
            .build().logger(this.getClass()) instanceof CustomMLLogger);
    }
}
