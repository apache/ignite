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
package org.apache.ignite.internal.processors.cache.persistence;

import org.apache.ignite.internal.GridKernalContext;
import org.apache.ignite.internal.processors.metric.MetricRegistry;
import org.apache.ignite.internal.processors.metric.sources.DataStorageMetricSource;
import org.apache.ignite.mxbean.DataStorageMetricsMXBean;

/**
 * @deprecated Should be removed for Apache Ignite 3.0 release.
 */
@Deprecated
public class DataStorageMetricsImpl implements DataStorageMetricsMXBean {
    private final DataStorageMetricSource src;

    private final GridKernalContext ctx;

    /**
     * @param src Metric source.
     */
    public DataStorageMetricsImpl(DataStorageMetricSource src, GridKernalContext ctx) {
        this.src = src;
        this.ctx = ctx;
    }

    /** {@inheritDoc} */
    @Override public float getWalLoggingRate() {
        return src.walLoggingRate();
    }

    /** {@inheritDoc} */
    @Override public float getWalWritingRate() {
        return src.walWritingRate();
    }

    /** {@inheritDoc} */
    @Override public int getWalArchiveSegments() {
        return src.walArchiveSegments();
    }

    /** {@inheritDoc} */
    @Override public float getWalFsyncTimeAverage() {
        return src.walFsyncTimeAverage();
    }

    /** {@inheritDoc} */
    @Override public long getWalBuffPollSpinsRate() {
        return src.walBuffPollSpinsRate();
    }

    /** {@inheritDoc} */
    @Override public long getLastCheckpointDuration() {
        return src.lastCheckpointDuration();
    }

    /** {@inheritDoc} */
    @Override public long getLastCheckpointLockWaitDuration() {
        return src.lastCheckpointLockWaitDuration();
    }

    /** {@inheritDoc} */
    @Override public long getLastCheckpointMarkDuration() {
        return src.lastCheckpointMarkDuration();
    }

    /** {@inheritDoc} */
    @Override public long getLastCheckpointPagesWriteDuration() {
        return src.lastCheckpointPagesWriteDuration();
    }

    /** {@inheritDoc} */
    @Override public long getLastCheckpointFsyncDuration() {
        return src.lastCheckpointFsyncDuration();
    }

    /** {@inheritDoc} */
    @Override public long getLastCheckpointTotalPagesNumber() {
        return src.lastCheckpointTotalPagesNumber();
    }

    /** {@inheritDoc} */
    @Override public long getLastCheckpointDataPagesNumber() {
        return src.lastCheckpointDataPagesNumber();
    }

    /** {@inheritDoc} */
    @Override public long getLastCheckpointCopiedOnWritePagesNumber() {
        return src.lastCheckpointCopiedOnWritePagesNumber();
    }

    /** {@inheritDoc} */
    @Override public void enableMetrics() {
        MetricRegistry reg = src.enable();

        ctx.metric().addRegistry(reg);
    }

    /** {@inheritDoc} */
    @Override public void disableMetrics() {
        src.disable();

        //TODO: remove registry
    }

    /** {@inheritDoc} */
    @Override public void rateTimeInterval(long rateTimeInterval) {
        // No-op. Breaks backward compatibility but doesn't break functionality.
    }

    /** {@inheritDoc} */
    @Override public void subIntervals(int subInts) {
        // No-op. Breaks backward compatibility but doesn't break functionality.
    }

    /** {@inheritDoc} */
    @Override public long getWalTotalSize() {
        return src.walTotalSize();
    }

    /** {@inheritDoc} */
    @Override public long getWalLastRollOverTime() {
        return src.walLastRollOverTime();
    }

    /** {@inheritDoc} */
    @Override public long getCheckpointTotalTime() {
        return src.checkpointTotalTime();
    }

    /** {@inheritDoc} */
    @Override public long getDirtyPages() {
        return src.dirtyPages();
    }

    /** {@inheritDoc} */
    @Override public long getPagesRead() {
        return src.pagesRead();
    }

    /** {@inheritDoc} */
    @Override public long getPagesWritten() {
        return src.pagesWritten();
    }

    /** {@inheritDoc} */
    @Override public long getPagesReplaced() {
        return src.pagesReplaced();
    }

    /** {@inheritDoc} */
    @Override public long getOffHeapSize() {
        return src.offHeapSize();
    }

    /** {@inheritDoc} */
    @Override public long getOffheapUsedSize() {
        return src.offheapUsedSize();
    }

    /** {@inheritDoc} */
    @Override public long getTotalAllocatedSize() {
        return src.totalAllocatedSize();
    }

    /** {@inheritDoc} */
    @Override public long getUsedCheckpointBufferPages() {
        return src.usedCheckpointBufferPages();
    }

    /** {@inheritDoc} */
    @Override public long getUsedCheckpointBufferSize() {
        return src.usedCheckpointBufferSize();
    }

    /** {@inheritDoc} */
    @Override public long getCheckpointBufferSize() {
        return src.checkpointBufferSize();
    }

    /** {@inheritDoc} */
    @Override public long getStorageSize() {
        return src.storageSize();
    }

    /** {@inheritDoc} */
    @Override public long getSparseStorageSize() {
        return src.sparseStorageSize();
    }

    /**
     * @param sparseStorageSize Sparse storage size.
     * @param storageSize Storage size.
     */

    //TODO: Move to metric source
    public void onStorageSizeChanged(
            long storageSize,
            long sparseStorageSize
    ) {
/*
        if (metricsEnabled) {
            this.storageSize.value(storageSize);
            this.sparseStorageSize.value(sparseStorageSize);
        }
*/
    }

}
