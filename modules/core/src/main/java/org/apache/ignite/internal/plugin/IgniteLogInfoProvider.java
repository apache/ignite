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

package org.apache.ignite.internal.plugin;

import java.util.ServiceLoader;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteLogger;
import org.apache.ignite.configuration.IgniteConfiguration;

/**
 * Pluggable Ignite component that is responsible for printing custom information during the Ignite lifecycle start phase.
 * The Ignite info is loaded using JDK {@link ServiceLoader}.
 */
public interface IgniteLogInfoProvider {
    /**
     * @param log Ignite logger.
     * @param cfg Ignite configuration.
     */
    public void ackKernalInited(IgniteLogger log, IgniteConfiguration cfg);

    /**
     * @param log Ignite logger.
     * @param ignite Ignite instance.
     */
    public void ackKernalStarted(IgniteLogger log, Ignite ignite);

    /**
     * @param log Ignite logger.
     * @param ignite Ignite instance.
     * @param err {@code True} if error occurred during the Ignite instance stop process.
     */
    public void ackKernalStopped(IgniteLogger log, Ignite ignite, boolean err);

    /**
     * @param log Ignite logger.
     * @param ignite Ignite instance.
     */
    public void ackNodeBasicMetrics(IgniteLogger log, Ignite ignite);

    /**
     * @param log Ignite logger.
     * @param ignite Ignite instance.
     */
    public void ackNodeDataStorageMetrics(IgniteLogger log, Ignite ignite);

    /**
     * @param log Ignite logger.
     * @param ignite Ignite instance.
     */
    public void ackNodeMemoryStatisticsMetrics(IgniteLogger log, Ignite ignite);
}
