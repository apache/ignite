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
 *
 */

package org.apache.ignite.internal.stat;

import java.time.LocalDateTime;
import java.util.Collections;
import java.util.EnumMap;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Collectors;
import org.apache.ignite.IgniteException;
import org.apache.ignite.IgniteIllegalStateException;

/**
 * IO statistics manager to manage of gathering IO statistics.
 */
public class IoStatisticsManager {
    /** All statistic holders */
    private final Map<IoStatisticsType, Map<IoStatisticsHolderKey, IoStatisticsHolder>> statByType;

    /** Time of since statistics start gathering. */
    private volatile LocalDateTime statsSince = LocalDateTime.now();

    /**
     * Constructor.
     */
    public IoStatisticsManager() {
        statByType = new EnumMap<>(IoStatisticsType.class);

        for (IoStatisticsType types : IoStatisticsType.values())
            statByType.put(types, new ConcurrentHashMap<>());
    }

    /**
     * Create and register statistics holder.
     *
     * @param type Type of statistics.
     * @param name Name of element of statistics.
     * @return created statistics holder.
     */
    public IoStatisticsHolder register(IoStatisticsType type, String name) {
        return register(type, name, null);
    }

    /**
     * Create and register statistics holder.
     *
     * @param type Type of statistics.
     * @param name Name of element of statistics.
     * @param subName Subname of element of statistics.
     * @return created statistics holder.
     */
    public IoStatisticsHolder register(IoStatisticsType type, String name, String subName) {
        if (statByType.isEmpty())
            throw new IgniteIllegalStateException("IO Statistics manager has been stopped and can'be used");

        IoStatisticsHolder statHolder;

        IoStatisticsHolderKey statisticsHolderKey;
        switch (type) {
            case CACHE_GROUP:
                statHolder = new IoStatisticsHolderCache(name);

                statisticsHolderKey = new IoStatisticsHolderKey(name);

                break;
            case HASH_INDEX:
            case SORTED_INDEX:
                statHolder = new IoStatisticsHolderIndex(name, subName);

                statisticsHolderKey = new IoStatisticsHolderKey(name, subName);

                break;
            default:
                throw new IgniteException("Gathering IO statistics for " + type + "doesn't support");
        }

        IoStatisticsHolder existedStatisitcHolder = statByType.get(type).putIfAbsent(statisticsHolderKey, statHolder);

        return (existedStatisitcHolder != null) ? existedStatisitcHolder : statHolder;
    }

    /**
     * Remove all holders.
     */
    public void stop() {
        statByType.clear();
    }

    /**
     * Reset statistics
     */
    public void reset() {
        statByType.forEach((t, s) ->
            s.forEach((k, sh) -> sh.resetStatistics())
        );

        statsSince = LocalDateTime.now();
    }

    /**
     * @return When statistics gathering start.
     */
    public LocalDateTime statsSince() {
        return statsSince;
    }

    /**
     * Extract all tracked names for given statistics type.
     *
     * @param statType Type of statistics which tracked names need to extract.
     * @return Set of present names for given statType
     */
    public Set<String> deriveStatNames(IoStatisticsType statType) {
        assert statType != null;

        return statByType.get(statType).keySet().stream()
            .map(v -> v.name()).collect(Collectors.toSet());
    }

    /**
     * Extract all tracked subNames for given statistics type .
     *
     * @param name Name of element of statistics.
     * @param statType Type of statistics which tracked names need to extract.
     * @return Set of present names for given statType
     */
    public Set<String> deriveStatSubNames(IoStatisticsType statType, String name) {
        assert statType != null;

        return statByType.get(statType).keySet().stream()
            .filter(k -> k.name().equalsIgnoreCase(name) && k.subName() != null).map(k -> k.subName()).collect(Collectors.toSet());
    }

    /**
     * @param statType Type of statistics which need to take.
     * @param name name of statistics which need to take, e.g. cache name
     * @param subName subName of statistics which need to take, e.g. index name.
     * @return Tracked physical reads by types since last reset statistics.
     */
    public Map<String, Long> physicalReadsByTypes(IoStatisticsType statType, String name, String subName) {
        IoStatisticsHolder statHolder = statByType.get(statType).get(new IoStatisticsHolderKey(name, subName));

        return (statHolder != null) ? statHolder.physicalReadsMap() : Collections.emptyMap();
    }

    /**
     * @param statType Type of statistics which need to take.
     * @param name name of statistics which need to take, e.g. cache name
     * @param subName subName of statistics which need to take, e.g. index name.
     * @return Number of physical reads since last reset statistics.
     */
    public Long physicalReads(IoStatisticsType statType, String name, String subName) {
        IoStatisticsHolder statHolder = statByType.get(statType).get(new IoStatisticsHolderKey(name, subName));

        return (statHolder != null) ? statHolder.physicalReads() : null;
    }

    /**
     * @param statType Type of statistics which need to take.
     * @param name name of statistics which need to take, e.g. cache name
     * @param subName subName of statistics which need to take, e.g. index name.
     * @return Tracked logical reads by types since last reset statistics.
     */
    public Map<String, Long> logicalReadsMap(IoStatisticsType statType, String name, String subName) {
        IoStatisticsHolder statHolder = statByType.get(statType).get(new IoStatisticsHolderKey(name, subName));

        return (statHolder != null) ? statHolder.logicalReadsMap() : Collections.emptyMap();
    }

    /**
     * @param statType Type of statistics which need to take.
     * @param name name of statistics which need to take, e.g. cache name
     * @param subName subName of statistics which need to take, e.g. index name.
     * @return Number of logical reads since last reset statistics.
     */
    public Long logicalReads(IoStatisticsType statType, String name, String subName) {
        IoStatisticsHolder stat = statByType.get(statType).get(new IoStatisticsHolderKey(name, subName));

        return (stat != null) ? stat.logicalReads() : null;
    }
}
