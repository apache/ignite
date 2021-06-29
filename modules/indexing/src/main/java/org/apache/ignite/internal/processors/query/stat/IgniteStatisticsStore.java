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

import java.util.Collection;
import java.util.Map;

import org.apache.ignite.internal.util.collection.IntMap;

/**
 * Statistics store interface.
 */
public interface IgniteStatisticsStore {
    /**
     * Clear statistics of any type for any objects;
     */
    public void clearAllStatistics();

    /**
     * Get all local partition statistics.
     *
     * @param schema Schema name, if {@code null} - returl local partitions statistics for all schemas.
     * @return Map with all local partitions statistics.
     */
    public Map<StatisticsKey, Collection<ObjectPartitionStatisticsImpl>> getAllLocalPartitionsStatistics(String schema);

    /**
     * Replace all tables partition statistics with specified ones.
     *
     * @param key Statistics key to replace statistics by.
     * @param statistics Collection of partition level statistics.
     */
    public void replaceLocalPartitionsStatistics(StatisticsKey key, Collection<ObjectPartitionStatisticsImpl> statistics);

    /**
     * Get local partition statistics by specified object.
     *
     * @param key Key to get statistics by.
     * @return Collection of partitions statistics.
     */
    public Collection<ObjectPartitionStatisticsImpl> getLocalPartitionsStatistics(StatisticsKey key);

    /**
     * Clear partition statistics for specified object.
     *
     * @param key Key to clear statistics by.
     */
    public void clearLocalPartitionsStatistics(StatisticsKey key);

    /**
     * Get partition statistics.
     *
     * @param key Key to get partition statistics by.
     * @param partId Partition id.
     * @return Object partition statistics or {@code null} if there are no statistics collected for such partition.
     */
    public ObjectPartitionStatisticsImpl getLocalPartitionStatistics(StatisticsKey key, int partId);

    /**
     * Clear partition statistics.
     *
     * @param key Object which statistics needs to be cleaned.
     * @param partId Partition id.
     */
    public void clearLocalPartitionStatistics(StatisticsKey key, int partId);

    /**
     * Clear partitions statistics.
     *
     * @param key Object which statistics need to be cleaned.
     * @param partIds Collection of partition ids.
     */
    public void clearLocalPartitionsStatistics(StatisticsKey key, Collection<Integer> partIds);

    /**
     * Save partition statistics.
     *
     * @param key Object which partition statistics belongs to.
     * @param statistics Statistics to save.
     */
    public void saveLocalPartitionStatistics(StatisticsKey key, ObjectPartitionStatisticsImpl statistics);

    /**
     *
     * @param obsolescence
     */
    public void saveObsolescenceInfo(
        Map<StatisticsKey, IntMap<ObjectPartitionStatisticsObsolescence>> obsolescence
    );

    /**
     * Remove obsolescence info for the given key and partitions (if specified).
     *
     * @param key Statistics key to remove obsolescense info by.
     * @param partIds Partition ids, if {@code null} - remove all partitions info for specified key.
     */
    public void clearObsolescenceInfo(StatisticsKey key, Collection<Integer> partIds);

    /**
     * Load all obsolescence info from store.
     *
     * @return StatisticsKey to partitionId to obsolescence info map.
     */
    public Map<StatisticsKey, IntMap<ObjectPartitionStatisticsObsolescence>> loadAllObsolescence();
}
