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

package org.apache.ignite.internal.processors.query.calcite.exec.tracker;

import java.util.UUID;
import org.apache.ignite.internal.metric.IoStatisticsHolder;
import org.apache.ignite.internal.metric.IoStatisticsQueryHelper;
import org.apache.ignite.internal.processors.cache.query.GridCacheQueryType;
import org.apache.ignite.internal.processors.performancestatistics.PerformanceStatisticsProcessor;

/**
 * Performance statistics gathering I/O operations tracker.
 */
public class PerformanceStatisticsIoTracker implements IoTracker {
    /** */
    private final PerformanceStatisticsProcessor performanceStatisticsProc;

    /** */
    private final UUID originatingNodeId;

    /** */
    private final long originatingQryId;

    /** */
    public PerformanceStatisticsIoTracker(
        PerformanceStatisticsProcessor performanceStatisticsProc,
        UUID originatingNodeId,
        long originatingQryId
    ) {
        this.performanceStatisticsProc = performanceStatisticsProc;
        this.originatingNodeId = originatingNodeId;
        this.originatingQryId = originatingQryId;
    }

    /** {@inheritDoc} */
    @Override public void startTracking() {
        IoStatisticsQueryHelper.startGatheringQueryStatistics();
    }

    /** {@inheritDoc} */
    @Override public void stopTracking() {
        IoStatisticsHolder stat = IoStatisticsQueryHelper.finishGatheringQueryStatistics();

        if (stat.logicalReads() > 0 || stat.physicalReads() > 0) {
            performanceStatisticsProc.queryReads(
                GridCacheQueryType.SQL_FIELDS,
                originatingNodeId,
                originatingQryId,
                stat.logicalReads(),
                stat.physicalReads());
        }
    }
}
