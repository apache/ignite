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
package org.apache.ignite.internal.processors.query.stat;

import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.internal.processors.query.stat.config.StatisticsObjectConfiguration;

/**
 * Statistics manager. Coordinate statistics collection and act as source of statistics.
 */
public interface IgniteStatisticsManager {
    /**
     * Gather object statistics.
     *
     * @param targets Target to params map to gather statistics by.
     * @throws IgniteCheckedException  Throws in case of errors.
     */
    public void collectStatistics(StatisticsObjectConfiguration... targets) throws IgniteCheckedException;

    /**
     * Clear object statistics.
     *
     * @param targets Collection of target to collect statistics by (schema, obj, columns).
     * @throws IgniteCheckedException In case of errors (for example: unsupported feature)
     */
    public void dropStatistics(StatisticsTarget... targets) throws IgniteCheckedException;

    /**
     * Refresh object statistics.
     *
     * @param targets Target to refresh statistics by.
     * @throws IgniteCheckedException  Throws in case of errors.
     */
    public void refreshStatistics(StatisticsTarget... targets) throws IgniteCheckedException;

    /**
     * Drop all statistics.
     */
    public void dropAll() throws IgniteCheckedException;

    /**
     * Get local statistics by object.
     *
     * @param key Statistic key.
     * @return Object statistics or {@code null} if there are no available statistics by specified object.
     */
    public ObjectStatistics getLocalStatistics(StatisticsKey key);

    /**
     * Stop statistic manager.
     */
    public void stop();

    /**
     * Set statistics usage state.
     *
     * @param state Statistics state.
     */
    public void usageState(StatisticsUsageState state) throws IgniteCheckedException;

    /**
     * @return Statistics usage state.
     */
    public StatisticsUsageState usageState();

    /**
     * To track statistics invalidation. Skip value if no statistics for the given table exists.
     *
     * @param schemaName Schema name.
     * @param objName Object name.
     * @param partId Partition id.
     * @param keyBytes Row key bytes.
     */
    public void onRowUpdated(String schemaName, String objName, int partId, byte[] keyBytes);
}
