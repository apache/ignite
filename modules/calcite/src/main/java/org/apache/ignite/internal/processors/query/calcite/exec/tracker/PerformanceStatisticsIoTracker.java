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

import java.util.List;
import java.util.UUID;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import org.apache.ignite.internal.metric.IoStatisticsHolder;
import org.apache.ignite.internal.metric.IoStatisticsQueryHelper;
import org.apache.ignite.internal.processors.cache.query.GridCacheQueryType;
import org.apache.ignite.internal.processors.performancestatistics.PerformanceStatisticsProcessor;
import org.apache.ignite.internal.util.typedef.T2;

/**
 * Performance statistics gathering I/O operations tracker.
 */
public class PerformanceStatisticsIoTracker implements IoTracker {
    /** */
    private final PerformanceStatisticsProcessor perfStatProc;

    /** */
    private final UUID originatingNodeId;

    /** */
    private final long originatingQryId;

    /** */
    private final AtomicLong logicalReads = new AtomicLong();

    /** */
    private final AtomicLong physicalReads = new AtomicLong();

    /** */
    private final AtomicBoolean started = new AtomicBoolean();

    /** */
    private final List<T2<String, AtomicLong>> cntrs = new CopyOnWriteArrayList<>();

    /** */
    public PerformanceStatisticsIoTracker(
        PerformanceStatisticsProcessor perfStatProc,
        UUID originatingNodeId,
        long originatingQryId
    ) {
        this.perfStatProc = perfStatProc;
        this.originatingNodeId = originatingNodeId;
        this.originatingQryId = originatingQryId;
    }

    /** {@inheritDoc} */
    @Override public boolean startTracking() {
        if (started.compareAndSet(false, true)) {
            IoStatisticsQueryHelper.startGatheringQueryStatistics();
            return true;
        }
        else
            return false;
    }

    /** {@inheritDoc} */
    @Override public void stopTracking() {
        IoStatisticsHolder stat = IoStatisticsQueryHelper.finishGatheringQueryStatistics();

        logicalReads.addAndGet(stat.logicalReads());
        physicalReads.addAndGet(stat.physicalReads());

        started.compareAndSet(true, false);
    }

    /** {@inheritDoc} */
    @Override public AtomicLong processedRowsCounter(String action) {
        AtomicLong cntr = new AtomicLong();

        cntrs.add(new T2<>(action, cntr));

        return cntr;
    }

    /** {@inheritDoc} */
    @Override public void flush() {
        long logicalReads = this.logicalReads.getAndSet(0);
        long physicalReads = this.physicalReads.getAndSet(0);

        if (logicalReads > 0 || physicalReads > 0) {
            perfStatProc.queryReads(
                GridCacheQueryType.SQL_FIELDS,
                originatingNodeId,
                originatingQryId,
                logicalReads,
                physicalReads);
        }

        for (T2<String, AtomicLong> cntr : cntrs) {
            long rowsCnt = cntr.get2().getAndSet(0);

            if (rowsCnt > 0) {
                perfStatProc.queryRowsProcessed(
                    GridCacheQueryType.SQL_FIELDS,
                    originatingNodeId,
                    originatingQryId,
                    cntr.get1(),
                    rowsCnt
                );
            }
        }
    }
}
