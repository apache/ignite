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
import org.apache.ignite.ml.dataset.Dataset;
import org.apache.ignite.ml.environment.deploy.DeployingContext;
import org.apache.ignite.ml.environment.logging.MLLogger;
import org.apache.ignite.ml.environment.parallelism.ParallelismStrategy;

/**
 * Specifies a set of utility-objects helpful at runtime but optional for learning algorithm
 * (like thread pool for parallel learning in bagging model or logger).
 */
public interface LearningEnvironment {
    /** Default environment */
    public static final LearningEnvironment DEFAULT_TRAINER_ENV = LearningEnvironmentBuilder.defaultBuilder().buildForTrainer();

    /**
     * Returns Parallelism Strategy instance.
     */
    public ParallelismStrategy parallelismStrategy();

    /**
     * Returns an instance of logger.
     */
    public MLLogger logger();

    /**
     * Random numbers generator.
     *
     * @return Random numbers generator.
     */
    public Random randomNumbersGenerator();

    /**
     * Returns an instance of logger for specific class.
     *
     * @param forCls Logging class context.
     */
    public <T> MLLogger logger(Class<T> forCls);

    /**
     * Gets current partition. If this is called not in one of compute tasks of {@link Dataset}, will return -1.
     *
     * @return Partition.
     */
    public int partition();

    /**
     * Returns partition data time-to-live in seconds (-1 for an infinite lifetime).
     *
     * @return Partition data time-to-live in seconds (-1 for an infinite lifetime).
     */
    public long dataTtl();

    /**
     * Returns deploy context instance.
     *
     * @return Deploy context.
     */
    public DeployingContext deployingContext();

    /**
     * Initializes deploying context by object representing current client computation
     * with classes unknown for server side.
     *
     * @param clientSideObj Client side object.
     */
    public default void initDeployingContext(Object clientSideObj) {
        deployingContext().initByClientObject(clientSideObj);
    }
}
