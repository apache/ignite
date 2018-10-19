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
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Collectors;
import org.apache.ignite.IgniteException;
import org.apache.ignite.IgniteIllegalStateException;

public class GridIoStatManager {
    /** No OP statistic handler. */
    public final static StatisticsHolder NO_OP_STATISTIC_HOLDER = new StatisticsHolderNoOp();

    /** All statistic holders */
    private final Map<StatType, Map<StatisticsHolderKey, StatisticsHolder>> statisticsHolders = new EnumMap<>(StatType.class);

    {
        for (StatType types : StatType.values()) {
            statisticsHolders.put(types, new ConcurrentHashMap<>());
        }
    }

    /** Time of since statistics start gathering. */
    private volatile LocalDateTime statsSince = LocalDateTime.now();

    /**
     * Create and register statistics holder.
     *
     * @param type Type of statistics.
     * @param name Name of element of statistics.
     * @return created statistics holder.
     */
    public StatisticsHolder createAndRegisterStatHolder(StatType type, String name) {
        return createAndRegisterStatHolder(type, name, null);
    }
    /**
     * Create and register statistics holder.
     *
     * @param type Type of statistics.
     * @param name Name of element of statistics.
     * @param subName Subname of element of statistics.
     * @return created statistics holder.
     */
    public StatisticsHolder createAndRegisterStatHolder(StatType type, String name, String subName) {
        if (statisticsHolders.isEmpty())
            throw new IgniteIllegalStateException("IO Statistics manager has been stopped and can'be used");

        StatisticsHolder statHolder;

        StatisticsHolderKey statisticsHolderKey;
        switch (type) {
            case CACHE:
                statHolder = new StatisticsHolderCache(name);

                statisticsHolderKey = new StatisticsHolderKey(name);

                break;
            case INDEX:
                statHolder = new StatisticsHolderIndex(name, subName);

                statisticsHolderKey = new StatisticsHolderKey(name, subName);

                break;
            default:
                throw new IgniteException("Gathering IO statistics for " + type + "doesn't support");
        }

        StatisticsHolder old = statisticsHolders.get(type).putIfAbsent(statisticsHolderKey, statHolder);

        assert old == null : old;

        return statHolder;
    }

    /**
     * Remove all holders.
     */
    public void stop() {
        statisticsHolders.clear();
    }

    /**
     * Reset statistics
     */
    public void resetStats() {
        statisticsHolders.forEach((t, s) ->
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
     * Extract all tracked names for given statisitcs type.
     *
     * @param statType Type of statistics which tracked names need to extract.
     * @return Set of present names for given statType
     */
    public Set<String> deriveStatNames(StatType statType) {
        assert statType != null;

        return statisticsHolders.get(statType).keySet().stream()
            .map(StatisticsHolderKey::toString).collect(Collectors.toSet());
    }

    /**
     * Extract all tracked subNames for given statistics type .
     *
     * @param name Name of element of statistics.
     * @param statType Type of statistics which tracked names need to extract.
     * @return Set of present names for given statType
     */
    public Set<String> deriveStatSubNames(StatType statType, String name) {
        assert statType != null;

        return statisticsHolders.get(statType).keySet().stream()
            .filter(k -> k.key1.equalsIgnoreCase(name)).map(k -> k.key2).collect(Collectors.toSet());
    }

    /**
     * @param statType Type of statistics which need to take.
     * @param name name of statistics which need to take, e.g. cache name
     * @param subName subName of statistics which need to take, e.g. index name.
     * @return Tracked physical reads by types since last reset statistics.
     */
    public Map<String, Long> physicalReadsByTypes(StatType statType, String name, String subName) {
        StatisticsHolder statHolder = statisticsHolders.get(statType).get(new StatisticsHolderKey(name, subName));

        return (statHolder != null) ? statHolder.physicalReadsMap() : Collections.emptyMap();
    }

    /**
     * @param statType Type of statistics which need to take.
     * @param name name of statistics which need to take, e.g. cache name
     * @return Number of physical reads since last reset statistics.
     */
    public Long physicalReads(StatType statType, String name) {
        return physicalReads(statType, name, null);
    }

    /**
     * @param statType Type of statistics which need to take.
     * @param name name of statistics which need to take, e.g. cache name
     * @param subName subName of statistics which need to take, e.g. index name.
     * @return Number of physical reads since last reset statistics.
     */
    public Long physicalReads(StatType statType, String name, String subName) {
        StatisticsHolder statHolder = statisticsHolders.get(statType).get(new StatisticsHolderKey(name, subName));

        return (statHolder != null) ? statHolder.physicalReads() : null;
    }

    /**
     * @param statType Type of statistics which need to take.
     * @param name name of statistics which need to take, e.g. cache name
     * @param subName subName of statistics which need to take, e.g. index name.
     * @return Tracked logical reads by types since last reset statistics.
     */
    public Map<String, Long> logicalReadsByTypes(StatType statType, String name, String subName) {
        StatisticsHolder statHolder = statisticsHolders.get(statType).get(new StatisticsHolderKey(name, subName));

        return (statHolder != null) ? statHolder.logicalReadsMap() : Collections.emptyMap();
    }

    /**
     * @param statType Type of statistics which need to take.
     * @param name name of statistics which need to take, e.g. cache name
     * @return Number of logical reads since last reset statistics.
     */
    public Long logicalReads(StatType statType, String name) {
        return logicalReads(statType, name, null);
    }

    /**
     * @param statType Type of statistics which need to take.
     * @param name name of statistics which need to take, e.g. cache name
     * @param subName subName of statistics which need to take, e.g. index name.
     * @return Number of logical reads since last reset statistics.
     */
    public Long logicalReads(StatType statType, String name, String subName) {
        StatisticsHolder statHolder = statisticsHolders.get(statType).get(new StatisticsHolderKey(name, subName));

        return (statHolder != null) ? statHolder.logicalReads() : null;
    }

    /**
     *
     */
    private class StatisticsHolderKey {
        /** */
        private String key1;
        /** */
        private String key2;

        /**
         * @param first First param.
         */
        StatisticsHolderKey(String first) {
            this(first, null);
        }

        /**
         * @param key1 First key.
         * @param key2 Second key.
         */
        StatisticsHolderKey(String key1, String key2) {
            assert key1 != null;

            this.key1 = key1;
            this.key2 = key2;
        }

        /** {@inheritDoc} */
        @Override public boolean equals(Object o) {
            if (this == o)
                return true;
            if (o == null || getClass() != o.getClass())
                return false;
            StatisticsHolderKey key = (StatisticsHolderKey)o;
            return Objects.equals(key1, key.key1) &&
                Objects.equals(key2, key.key2);
        }

        /** {@inheritDoc} */
        @Override public int hashCode() {
            return Objects.hash(key1, key2);
        }

        /** {@inheritDoc} */
        @Override public String toString() {
            return key2 == null ? key1 : key1 + "." + key2;
        }
    }
}
