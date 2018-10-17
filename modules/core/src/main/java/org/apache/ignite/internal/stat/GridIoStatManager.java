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
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Function;

public class GridIoStatManager {
    /** No OP statistic handler. */
    public final static StatisticsHolder NO_OP_STATISTIC_HOLDER = new StatisticsHolderNoOp();

    /** All statistic holders */
    private final Map<StatType, Map<String, StatisticsHolder>> statisticsHolders = new EnumMap<>(StatType.class);

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
        StatisticsHolder statHolder = new StatisticsHolderFineGrained(type, name);

        StatisticsHolder old = statisticsHolders.get(type).put(name, statHolder);
        assert old == null : old;

        return statHolder;
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
     * Extract all tracked subtypes.
     *
     * @param statType Type of statistics which subtypes need to extract.
     * @return Set of present subtypes for given statType
     */
    public Set<String> subTypes(StatType statType) {
        assert statType != null;

        return Collections.unmodifiableSet(statisticsHolders.get(statType).keySet());
    }

    /**
     * @param statType Type of statistics which need to take.
     * @param subType Subtype of statistics which need to take.
     * @return Tracked physical reads by types since last reset statistics.
     */
    public Map<PageType, Long> physicalReads(StatType statType, String subType) {
        StatisticsHolder statHolder = statisticsHolders.get(statType).get(subType);

        return (statHolder != null) ? statHolder.physicalReadsMap() : Collections.emptyMap();
    }

    /**
     * @param statType Type of statistics which need to take.
     * @param subType Subtype of statistics which need to take.
     * @return Tracked logical reads by types since last reset statistics.
     */
    public Map<PageType, Long> logicalReads(StatType statType, String subType) {
        StatisticsHolder statHolder = statisticsHolders.get(statType).get(subType);

        return (statHolder != null) ? statHolder.logicalReadsMap() : Collections.emptyMap();
    }

    /**
     * @param stat Statistics which need to aggregate.
     * @return Aggregated statistics for given statistics
     */
    public Map<AggregatePageType, AtomicLong> aggregate(Map<PageType, ? extends Number> stat) {
        Map<AggregatePageType, AtomicLong> res = new HashMap<>();

        for (AggregatePageType type : AggregatePageType.values())
            res.put(type, new AtomicLong());

        stat.forEach((k, v) -> res.get(AggregatePageType.aggregate(k)).addAndGet(v.longValue()));

        return res;
    }

    /**
     * @return Tracked local node logical reads by types since last reset statistics.
     */
    public Map<PageType, ? extends Number> logicalReadsLocalNode() {
        return readsLocalNode(StatisticsHolder::logicalReadsMap);
    }

    /**
     * @return Tracked local node physical reads by types since last reset statistics.
     */
    public Map<PageType, ? extends Number> physicalReadsLocalNode() {
        return readsLocalNode(StatisticsHolder::physicalReadsMap);
    }

    /**
     * @param deriveStatFunc Function to derive statistics.
     * @return Tracked local node reads by types since last reset statistics.
     */
    private Map<PageType, ? extends Number> readsLocalNode(
        Function<StatisticsHolder, Map<PageType, Long>> deriveStatFunc) {
        Map<PageType, AtomicLong> res = new EnumMap<>(PageType.class);

        statisticsHolders.forEach((t, h) -> h.values().forEach(v -> {
            for (Map.Entry<PageType, Long> entry : deriveStatFunc.apply(v).entrySet()) {
                PageType key = entry.getKey();

                Long val = entry.getValue();

                res.computeIfAbsent(key, (k) -> new AtomicLong()).addAndGet(val);
            }
        }));

        return res;
    }
}
