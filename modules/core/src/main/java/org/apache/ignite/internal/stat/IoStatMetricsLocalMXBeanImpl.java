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
import java.util.concurrent.atomic.AtomicLong;
import org.apache.ignite.mxbean.IoStatMetricsMXBean;
import org.jetbrains.annotations.NotNull;

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
    @Override public Map<String, Long> getPhysicalReads() {
        return convertStat(statMgr.physicalReads());
    }

    /** {@inheritDoc} */
    @Override public Map<String, Long> getPhysicalWrites() {
        return convertStat(statMgr.physicalWrites());
    }

    /** {@inheritDoc} */
    @Override public Map<String, Long> getLogicalReads() {
        return convertStat(statMgr.logicalReads());
    }

    /** {@inheritDoc} */
    @Override public Map<String, Long> getAggregatedPhysicalReads() {
        Map<AggregatePageType, AtomicLong> aggregatedStat = statMgr.aggregate(statMgr.physicalReads());

        return convertAggregatedStat(aggregatedStat);
    }

    /** {@inheritDoc} */
    @Override public Map<String, Long> getAggregatedPhysicalWrites() {
        Map<AggregatePageType, AtomicLong> aggregatedStat = statMgr.aggregate(statMgr.physicalWrites());

        return convertAggregatedStat(aggregatedStat);
    }

    /** {@inheritDoc} */
    @Override public Map<String, Long> getAggregatedLogicalReads() {
        Map<AggregatePageType, AtomicLong> aggregatedStat = statMgr.aggregate(statMgr.logicalReads());

        return convertAggregatedStat(aggregatedStat);
    }

    /** {@inheritDoc} */
    @Override public void resetStatistics() {
        statMgr.resetStats();
    }

    /**
     * Convert internal object to simple types for given statistics.
     *
     * @param stat Statistics which need to convert.
     * @return Converted statistics from internal.
     */
    @NotNull private Map<String, Long> convertStat(Map<PageType, Long> stat) {
        Map<String, Long> res = new HashMap<>(stat.size());

        stat.forEach((k, v) -> res.put(k.name(), v));

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
