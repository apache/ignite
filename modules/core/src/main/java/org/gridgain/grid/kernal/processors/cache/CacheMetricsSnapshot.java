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

package org.gridgain.grid.kernal.processors.cache;

import org.gridgain.grid.cache.*;
import org.gridgain.grid.util.typedef.internal.*;

import java.io.*;

/**
 * Metrics snapshot.
 */
class CacheMetricsSnapshot implements CacheMetrics, Externalizable {
    /** */
    private static final long serialVersionUID = 0L;

    /** Number of reads. */
    private long reads = 0;

    /** Number of puts. */
    private long puts = 0;

    /** Number of hits. */
    private long hits = 0;

    /** Number of misses. */
    private long misses = 0;

    /** Number of transaction commits. */
    private long txCommits = 0;

    /** Number of transaction rollbacks. */
    private long txRollbacks = 0;

    /** Number of evictions. */
    private long evicts = 0;

    /** Number of removed entries. */
    private long removes = 0;

    /** Put time taken nanos. */
    private float putAvgTimeNanos = 0;

    /** Get time taken nanos. */
    private float getAvgTimeNanos = 0;

    /** Remove time taken nanos. */
    private float removeAvgTimeNanos = 0;

    /** Commit transaction time taken nanos. */
    private float commitAvgTimeNanos = 0;

    /** Commit transaction time taken nanos. */
    private float rollbackAvgTimeNanos = 0;

    /**
     * Create snapshot for given metrics.
     *
     * @param m Cache metrics.
     */
    public CacheMetricsSnapshot(CacheMetrics m) {
        reads = m.getCacheGets();
        puts = m.getCachePuts();
        hits = m.getCacheHits();
        misses = m.getCacheMisses();
        txCommits = m.getCacheTxCommits();
        txRollbacks = m.getCacheTxRollbacks();
        evicts = m.getCacheEvictions();
        removes = m.getCacheRemovals();

        putAvgTimeNanos = m.getAveragePutTime();
        getAvgTimeNanos = m.getAverageGetTime();
        removeAvgTimeNanos = m.getAverageRemoveTime();
        commitAvgTimeNanos = m.getAverageTxCommitTime();
        rollbackAvgTimeNanos = m.getAverageTxRollbackTime();
    }

    /** {@inheritDoc} */
    @Override public long getCacheHits() {
        return hits;
    }

    /** {@inheritDoc} */
    @Override public float getCacheHitPercentage() {
        if (hits == 0 || reads == 0)
            return 0;

        return (float) hits / reads * 100.0f;
    }

    /** {@inheritDoc} */
    @Override public long getCacheMisses() {
        return misses;
    }

    /** {@inheritDoc} */
    @Override public float getCacheMissPercentage() {
        if (misses == 0 || reads == 0) {
            return 0;
        }

        return (float) misses / reads * 100.0f;
    }

    /** {@inheritDoc} */
    @Override public long getCacheGets() {
        return reads;
    }

    /** {@inheritDoc} */
    @Override public long getCachePuts() {
        return puts;
    }

    /** {@inheritDoc} */
    @Override public long getCacheRemovals() {
        return removes;
    }

    /** {@inheritDoc} */
    @Override public long getCacheEvictions() {
        return evicts;
    }

    /** {@inheritDoc} */
    @Override public float getAverageGetTime() {
        return getAvgTimeNanos;
    }

    /** {@inheritDoc} */
    @Override public float getAveragePutTime() {
        return putAvgTimeNanos;
    }

    /** {@inheritDoc} */
    @Override public float getAverageRemoveTime() {
        return removeAvgTimeNanos;
    }

    /** {@inheritDoc} */
    @Override public float getAverageTxCommitTime() {
        return commitAvgTimeNanos;
    }

    /** {@inheritDoc} */
    @Override public float getAverageTxRollbackTime() {
        return rollbackAvgTimeNanos;
    }

    /** {@inheritDoc} */
    @Override public long getCacheTxCommits() {
        return txCommits;
    }

    /** {@inheritDoc} */
    @Override public long getCacheTxRollbacks() {
        return txRollbacks;
    }

    @Override
    public String name() {
        return null;
    }

    @Override
    public String metricsFormatted() {
        return null;
    }

    @Override
    public long getOverflowSize() {
        return 0;
    }

    @Override
    public long getOffHeapEntriesCount() {
        return 0;
    }

    @Override
    public long getOffHeapAllocatedSize() {
        return 0;
    }

    @Override
    public int getSize() {
        return 0;
    }

    @Override
    public int getKeySize() {
        return 0;
    }

    @Override
    public boolean isEmpty() {
        return false;
    }

    @Override
    public int getDhtEvictQueueCurrentSize() {
        return 0;
    }

    @Override
    public int getTxThreadMapSize() {
        return 0;
    }

    @Override
    public int getTxXidMapSize() {
        return 0;
    }

    @Override
    public int getTxCommitQueueSize() {
        return 0;
    }

    @Override
    public int getTxPrepareQueueSize() {
        return 0;
    }

    @Override
    public int getTxStartVersionCountsSize() {
        return 0;
    }

    @Override
    public int getTxCommittedVersionsSize() {
        return 0;
    }

    @Override
    public int getTxRolledbackVersionsSize() {
        return 0;
    }

    @Override
    public int getTxDhtThreadMapSize() {
        return 0;
    }

    @Override
    public int getTxDhtXidMapSize() {
        return 0;
    }

    @Override
    public int getTxDhtCommitQueueSize() {
        return 0;
    }

    @Override
    public int getTxDhtPrepareQueueSize() {
        return 0;
    }

    @Override
    public int getTxDhtStartVersionCountsSize() {
        return 0;
    }

    @Override
    public int getTxDhtCommittedVersionsSize() {
        return 0;
    }

    @Override
    public int getTxDhtRolledbackVersionsSize() {
        return 0;
    }

    @Override
    public boolean isWriteBehindEnabled() {
        return false;
    }

    @Override
    public int getWriteBehindFlushSize() {
        return 0;
    }

    @Override
    public int getWriteBehindFlushThreadCount() {
        return 0;
    }

    @Override
    public long getWriteBehindFlushFrequency() {
        return 0;
    }

    @Override
    public int getWriteBehindStoreBatchSize() {
        return 0;
    }

    @Override
    public int getWriteBehindTotalCriticalOverflowCount() {
        return 0;
    }

    @Override
    public int getWriteBehindCriticalOverflowCount() {
        return 0;
    }

    @Override
    public int getWriteBehindErrorRetryCount() {
        return 0;
    }

    @Override
    public int getWriteBehindBufferSize() {
        return 0;
    }

    /** {@inheritDoc} */
    @Override public void writeExternal(ObjectOutput out) throws IOException {
        out.writeLong(reads);
        out.writeLong(puts);
        out.writeLong(hits);
        out.writeLong(misses);
        out.writeLong(txCommits);
        out.writeLong(txRollbacks);
        out.writeLong(removes);
        out.writeLong(evicts);

        out.writeFloat(putAvgTimeNanos);
        out.writeFloat(getAvgTimeNanos);
        out.writeFloat(removeAvgTimeNanos);
        out.writeFloat(commitAvgTimeNanos);
        out.writeFloat(rollbackAvgTimeNanos);
    }

    /** {@inheritDoc} */
    @Override public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {
        reads = in.readLong();
        puts = in.readLong();
        hits = in.readLong();
        misses = in.readLong();
        txCommits = in.readLong();
        txRollbacks = in.readLong();
        removes = in.readLong();
        evicts = in.readLong();

        putAvgTimeNanos = in.readFloat();
        getAvgTimeNanos = in.readFloat();
        removeAvgTimeNanos = in.readFloat();
        commitAvgTimeNanos = in.readFloat();
        rollbackAvgTimeNanos = in.readFloat();
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(CacheMetricsSnapshot.class, this);
    }
}
