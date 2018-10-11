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
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.BiFunction;
import org.apache.ignite.mxbean.IoStatMetricsMXBean;
import org.jetbrains.annotations.NotNull;

import static org.apache.ignite.internal.stat.GridIoStatManager.KEY_FOR_LOCAL_NODE_STAT;

/**
 * JMX bean to expose local node IO statistics.
 */
public class IoStatMetricsLocalMXBeanImpl implements IoStatMetricsMXBean {
    /** IO statistic manager. */
    private GridIoStatManager statMgr;

    /**
     * @param statMgr IO statistic manager.
     */
    public IoStatMetricsLocalMXBeanImpl(GridIoStatManager statMgr) {
        this.statMgr = statMgr;
    }

    /** {@inheritDoc} */
    @Override public LocalDateTime getStartGatheringStatistics() {
        return statMgr.statsSince();
    }

    /** {@inheritDoc} */
    @Override public void resetStatistics() {
        statMgr.resetStats();
    }

    /** {@inheritDoc} */
    @Override public Map<String, Long> getPhysicalReads() {
        return convertStat(statMgr.physicalReadsLocalNode());
    }

    /** {@inheritDoc} */
    @Override public Map<String, Long> getPhysicalWrites() {
        return convertStat(statMgr.physicalWritesLocalNode());
    }

    /** {@inheritDoc} */
    @Override public Map<String, Long> getLogicalReads() {
        return convertStat(statMgr.logicalReadsLocalNode());
    }

    /** {@inheritDoc} */
    @Override public Map<String, Long> getAggregatedPhysicalReads() {
        Map<AggregatePageType, AtomicLong> aggregatedStat = statMgr.aggregate(statMgr.physicalReadsLocalNode());

        return convertAggregatedStat(aggregatedStat);
    }

    /** {@inheritDoc} */
    @Override public Map<String, Long> getAggregatedPhysicalWrites() {
        Map<AggregatePageType, AtomicLong> aggregatedStat = statMgr.aggregate(statMgr.physicalWritesLocalNode());

        return convertAggregatedStat(aggregatedStat);
    }

    /** {@inheritDoc} */
    @Override public Map<String, Long> getAggregatedLogicalReads() {
        Map<AggregatePageType, AtomicLong> aggregatedStat = statMgr.aggregate(statMgr.logicalReadsLocalNode());

        return convertAggregatedStat(aggregatedStat);
    }

    /** {@inheritDoc} */
    @Override public Set<String> getLogicalReadsStatIndexesNames() {
        return statMgr.subTypesLogicalReads(StatType.INDEX);
    }

    /** {@inheritDoc} */
    @Override public Set<String> getPhysicalReadsStatIndexesNames() {
        return statMgr.subTypesPhysicalReads(StatType.INDEX);
    }

    /** {@inheritDoc} */
    @Override public Map<String, Long> getPhysicalReadsIndex(String idxName) {
        return convertStat(statMgr.physicalReads(StatType.INDEX, idxName));
    }

    /** {@inheritDoc} */
    @Override public Map<String, Long> getLogicalReadsIndex(String idxName) {
        return convertStat(statMgr.logicalReads(StatType.INDEX, idxName));
    }

    /** {@inheritDoc} */
    @Override public Map<String, Long> getLogicalReadStatistics(String statTypeName, String subTypeFilter,
        boolean aggregate) {
        return getStatistics(statTypeName, subTypeFilter, aggregate, statMgr::logicalReads);

    }

    /** {@inheritDoc} */
    @Override public Map<String, Long> getPhysicalReadStatistics(String statTypeName, String subTypeFilter,
        boolean aggregate) {
        return getStatistics(statTypeName, subTypeFilter, aggregate, statMgr::physicalReads);

    }

    /**
     * @param statTypeName String representation of {@code StatType}.
     * @param subTypeFilter Subtype of statistics to filter results.
     * @param aggregate {@code true} in case statistics should be aggregated.
     * @param statFunction Function to retrieve statistics.
     * @return Requested statistics.
     */
    private Map<String, Long> getStatistics(String statTypeName, String subTypeFilter, boolean aggregate,
        BiFunction<StatType, String, Map<PageType, Long>> statFunction) {
        StatType type = StatType.valueOf(statTypeName);

        String subType;

        if (type == StatType.LOCAL_NODE)
            subType = KEY_FOR_LOCAL_NODE_STAT;
        else
            subType = subTypeFilter;

        Map<PageType, Long> stat = statFunction.apply(type, subType);

        if (aggregate)
            return convertAggregatedStat(statMgr.aggregate(stat));
        else
            return convertStat(stat);
    }

    /**
     * Convert internal object to simple types for given statistics.
     *
     * @param stat Statistics which need to convert.
     * @return Converted statistics from internal.
     */
    @NotNull private Map<String, Long> convertStat(Map<PageType, Long> stat) {
        Map<String, Long> res = new HashMap<>(stat.size());

        stat.forEach((k, v) -> {
            if (v != 0)
                res.put(k.name(), v);
        });

        return res;
    }

    /**
     * Convert internal object to simple types for given statistics.
     *
     * @param stat Statistics which need to convert.
     * @return Converted statistics from internal.
     */
    @NotNull private Map<String, Long> convertAggregatedStat(Map<AggregatePageType, AtomicLong> stat) {
        Map<String, Long> res = new HashMap<>(stat.size());

        stat.forEach((k, v) -> res.put(k.name(), v.longValue()));

        return res;
    }
}
