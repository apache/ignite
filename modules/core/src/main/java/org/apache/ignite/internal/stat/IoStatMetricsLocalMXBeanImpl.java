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

import static org.apache.ignite.internal.stat.GridIoStatManager.KEY_FOR_GLOBAL_STAT;

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
    @Override public Map<String, Long> getPhysicalReadsGlobal() {
        return convertStat(statMgr.physicalReadsGlobal());
    }

    /** {@inheritDoc} */
    @Override public Map<String, Long> getPhysicalWritesGlobal() {
        return convertStat(statMgr.physicalWritesGlobal());
    }

    /** {@inheritDoc} */
    @Override public Map<String, Long> getLogicalReadsGlobal() {
        return convertStat(statMgr.logicalReadsGlobal());
    }

    /** {@inheritDoc} */
    @Override public Map<String, Long> getAggregatedPhysicalReadsGlobal() {
        Map<AggregatePageType, AtomicLong> aggregatedStat = statMgr.aggregate(statMgr.physicalReadsGlobal());

        return convertAggregatedStat(aggregatedStat);
    }

    /** {@inheritDoc} */
    @Override public Map<String, Long> getAggregatedPhysicalWritesGlobal() {
        Map<AggregatePageType, AtomicLong> aggregatedStat = statMgr.aggregate(statMgr.physicalWritesGlobal());

        return convertAggregatedStat(aggregatedStat);
    }

    /** {@inheritDoc} */
    @Override public Map<String, Long> getAggregatedLogicalReadsGlobal() {
        Map<AggregatePageType, AtomicLong> aggregatedStat = statMgr.aggregate(statMgr.logicalReadsGlobal());

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
     * @param subTypeFilter Subtype of statistics. In case value can be parsed as int will be used as Integer.
     * @param aggregate {@code true} in case statistics should be aggregated.
     * @param statFunction Function to retrieve statistics.
     * @return Requested statistics.
     */
    private Map<String, Long> getStatistics(String statTypeName, String subTypeFilter, boolean aggregate,
        BiFunction<StatType, Object, Map<PageType, Long>> statFunction) {
        StatType type = StatType.valueOf(statTypeName);

        Object subType;

        if (type == StatType.GLOBAL)
            subType = KEY_FOR_GLOBAL_STAT;
        else {
            subType = subTypeFilter;
            try {
                subType = Integer.valueOf(subTypeFilter);
            }
            catch (NumberFormatException nfe) {
                //ignore.
            }
        }

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
