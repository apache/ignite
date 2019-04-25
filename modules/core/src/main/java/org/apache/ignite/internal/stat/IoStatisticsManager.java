/*
 * Copyright 2019 GridGain Systems, Inc. and Contributors.
 *
 * Licensed under the GridGain Community Edition License (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     https://www.gridgain.com/products/software/community-edition/gridgain-community-edition-license
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.ignite.internal.stat;

import java.time.OffsetDateTime;
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
    private volatile OffsetDateTime startTime;

    /**
     * Constructor.
     */
    public IoStatisticsManager() {
        statByType = new EnumMap<>(IoStatisticsType.class);

        for (IoStatisticsType types : IoStatisticsType.values())
            statByType.put(types, new ConcurrentHashMap<>());

        reset();
    }

    /**
     * Create and register statistics holder for cache group.
     *
     * @param name Name of cache or cache group.
     * @param grpId Cache group id.
     * @return created statistics holder.
     */
    public IoStatisticsHolder registerCacheGroup(String name, int grpId) {
        return register(IoStatisticsType.CACHE_GROUP, name, grpId);
    }

    /**
     * Create and register statistics holder for index.
     *
     * @param type Type of index statistics.
     * @param name Name of cache or cache group.
     * @param idxName Name of index.
     * @return created statistics holder.
     */
    public IoStatisticsHolder registerIndex(IoStatisticsType type, String name, String idxName) {
        assert type == IoStatisticsType.HASH_INDEX || type == IoStatisticsType.SORTED_INDEX : type;

        return register(type, name, idxName);
    }

    /**
     * Create and register statistics holder.
     *
     * @param type Type of statistics.
     * @param name Name of element of statistics.
     * @param param second parameter of statistic's element.
     * @return created statistics holder.
     */
    private IoStatisticsHolder register(IoStatisticsType type, String name, Object param) {
        if (statByType.isEmpty())
            throw new IgniteIllegalStateException("IO Statistics manager has been stopped and can'be used");

        IoStatisticsHolder stat;
        IoStatisticsHolderKey statKey;

        switch (type) {
            case CACHE_GROUP:
                stat = new IoStatisticsHolderCache(name, (Integer)param);
                statKey = new IoStatisticsHolderKey(name);

                break;

            case HASH_INDEX:
            case SORTED_INDEX:
                stat = new IoStatisticsHolderIndex(name, (String)param);
                statKey = new IoStatisticsHolderKey(name, (String)param);

                break;

            default:
                throw new IgniteException("Gathering IO statistics for " + type + "doesn't support");
        }

        IoStatisticsHolder existedStatisitcHolder = statByType.get(type).putIfAbsent(statKey, stat);

        return (existedStatisitcHolder != null) ? existedStatisitcHolder : stat;
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

        startTime = OffsetDateTime.now();
    }

    /**
     * @return When statistics gathering start.
     */
    public OffsetDateTime startTime() {
        return startTime;
    }

    /**
     * Extract all tracked names for given statistics type.
     *
     * @param statType Type of statistics which tracked names need to extract.
     * @return Set of present names for given statType
     */
    public Set<String> deriveStatisticNames(IoStatisticsType statType) {
        assert statType != null;

        return statByType.get(statType).keySet().stream()
            .map(IoStatisticsHolderKey::name)
            .collect(Collectors.toSet());
    }

    /**
     * Extract all tracked subNames for given statistics type .
     *
     * @param name Name of element of statistics.
     * @param statType Type of statistics which tracked names need to extract.
     * @return Set of present names for given statType
     */
    public Set<String> deriveStatisticSubNames(IoStatisticsType statType, String name) {
        assert statType != null;

        return statByType.get(statType).keySet().stream()
            .filter(k -> k.name().equalsIgnoreCase(name) && k.subName() != null)
            .map(IoStatisticsHolderKey::subName)
            .collect(Collectors.toSet());
    }

    /**
     * @param statType Type of statistics which need to take.
     * @param name name of statistics which need to take, e.g. cache name
     * @param subName subName of statistics which need to take, e.g. index name.
     * @return Tracked physical reads by types since last reset statistics.
     */
    public Map<String, Long> physicalReadsMap(IoStatisticsType statType, String name, String subName) {
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

    /**
     * @param statType Type of statistics which need to take.
     * @return All tracked statistics for given type.
     */
    public Map<IoStatisticsHolderKey, IoStatisticsHolder> statistics(IoStatisticsType statType){
        return Collections.unmodifiableMap(statByType.get(statType));
    }
}
