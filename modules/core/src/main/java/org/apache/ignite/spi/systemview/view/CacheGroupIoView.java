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

package org.apache.ignite.spi.systemview.view;

import org.apache.ignite.internal.managers.systemview.walker.Order;
import org.apache.ignite.internal.processors.cache.CacheGroupContext;
import org.apache.ignite.metric.MetricRegistry;
import org.apache.ignite.spi.metric.LongMetric;

import static org.apache.ignite.internal.metric.IoStatisticsHolderCache.LOGICAL_READS;
import static org.apache.ignite.internal.metric.IoStatisticsHolderCache.PHYSICAL_READS;

/**
 * Cache group IO statistics representation for a {@link SystemView}.
 */
public class CacheGroupIoView {
    /** Cache group. */
    private final CacheGroupContext grpCtx;

    /** Metric registry for current cache group IO statistics. */
    private final MetricRegistry mreg;

    /**
     * @param grpCtx Cache group context.
     * @param mreg Metric registry for current cache group IO statistics.
     */
    public CacheGroupIoView(CacheGroupContext grpCtx, MetricRegistry mreg) {
        this.grpCtx = grpCtx;
        this.mreg = mreg;
    }

    /**
     * @return Cache group id.
     */
    @Order
    public int cacheGroupId() {
        return grpCtx.groupId();
    }

    /**
     * @return Cache group name.
     */
    @Order(1)
    public String cacheGroupName() {
        return grpCtx.cacheOrGroupName();
    }

    /**
     * @return Physical reads.
     */
    @Order(2)
    public long physicalReads() {
        LongMetric metric = mreg.findMetric(PHYSICAL_READS);

        return metric != null ? metric.value() : 0;
    }

    /**
     * @return Logical reads.
     */
    @Order(3)
    public long logicalReads() {
        LongMetric metric = mreg.findMetric(LOGICAL_READS);

        return metric != null ? metric.value() : 0;
    }
}
