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
import org.gridgain.grid.util.tostring.*;
import org.gridgain.grid.util.typedef.internal.*;
import org.jetbrains.annotations.*;

import java.io.*;

/**
 * Adapter for cache metrics.
 */
public class GridCacheMetricsAdapter implements GridCacheMetrics, Externalizable {
    /** */
    private static final long NANOS_IN_MICROSECOND = 1000L;

    /** */
    private static final long serialVersionUID = 0L;

    /** Create time. */
    private long createTime = U.currentTimeMillis();

    /** Last read time. */
    private volatile long readTime = createTime;

    /** Last update time. */
    private volatile long writeTime = createTime;

    /** Last commit time. */
    private volatile long commitTime = createTime;

    /** Last rollback time. */
    private volatile long rollbackTime = createTime;

    /** Number of reads. */
    private volatile int reads;

    /** Number of writes. */
    private volatile int writes;

    /** Number of hits. */
    private volatile int hits;

    /** Number of misses. */
    private volatile int misses;

    /** Number of transaction commits. */
    private volatile int txCommits;

    /** Number of transaction rollbacks. */
    private volatile int txRollbacks;

    /** Number of evictions. */
    private volatile long evictCnt;

    /** Number of removed entries. */
    private volatile long rmCnt;

    /** Put time taken nanos. */
    private volatile long putTimeNanos;

    /** Get time taken nanos. */
    private volatile long getTimeNanos;

    /** Remove time taken nanos. */
    private volatile long removeTimeNanos;

    /** Cache metrics. */
    @GridToStringExclude
    private transient GridCacheMetricsAdapter delegate;

    /**
     * No-args constructor.
     */
    public GridCacheMetricsAdapter() {
        delegate = null;
    }

    /**
     * @param m Metrics to copy from.
     */
    public GridCacheMetricsAdapter(GridCacheMetricsAdapter m) {
        createTime = m.createTime;
        readTime = m.readTime;
        writeTime = m.writeTime;
        commitTime = m.commitTime;
        rollbackTime = m.rollbackTime;
        reads = m.reads;
        writes = m.writes;
        hits = m.hits;
        misses = m.misses;
        txCommits = m.txCommits;
        txRollbacks = m.txRollbacks;
        rmCnt = m.rmCnt;
        evictCnt = m.evictCnt;
        getTimeNanos = m.getTimeNanos;
        putTimeNanos = m.putTimeNanos;
        removeTimeNanos = m.removeTimeNanos;
    }

    /**
     * @param delegate Metrics to delegate to.
     */
    public void delegate(GridCacheMetricsAdapter delegate) {
        this.delegate = delegate;
    }

    /** {@inheritDoc} */
    @Override public long createTime() {
        return createTime;
    }

    /** {@inheritDoc} */
    @Override public long writeTime() {
        return writeTime;
    }

    /** {@inheritDoc} */
    @Override public long readTime() {
        return readTime;
    }

    /** {@inheritDoc} */
    @Override public long commitTime() {
        return commitTime;
    }

    /** {@inheritDoc} */
    @Override public long rollbackTime() {
        return rollbackTime;
    }

    /** {@inheritDoc} */
    @Override public int reads() {
        return reads;
    }

    /** {@inheritDoc} */
    @Override public int writes() {
        return (int)(writes + rmCnt);
    }

    /** {@inheritDoc} */
    @Override public int hits() {
        return hits;
    }

    /** {@inheritDoc} */
    @Override public int misses() {
        return misses;
    }

    /** {@inheritDoc} */
    @Override public int txCommits() {
        return txCommits;
    }

    /** {@inheritDoc} */
    @Override public int txRollbacks() {
        return txRollbacks;
    }

    /** {@inheritDoc} */
    @Override public void clear() {
        createTime = U.currentTimeMillis();
        readTime = createTime;
        writeTime = createTime;
        commitTime = createTime;
        rollbackTime = createTime;
        reads = 0;
        writes = 0;
        hits = 0;
        misses = 0;
        txCommits = 0;
        txRollbacks = 0;
    }

    /** {@inheritDoc} */
    @Override public long getCacheHits() {
        return hits;
    }

    /** {@inheritDoc} */
    @Override public float getCacheHitPercentage() {
        long hits0 = hits;

        if (hits0 == 0)
            return 0;

        return (float) hits0 / getCacheGets() * 100.0f;
    }

    /** {@inheritDoc} */
    @Override public long getCacheMisses() {
        return misses;
    }

    /** {@inheritDoc} */
    @Override public float getCacheMissPercentage() {
        long misses0 = misses;

        if (misses0 == 0)
            return 0;

        return (float) misses0 / getCacheGets() * 100.0f;
    }

    /** {@inheritDoc} */
    @Override public long getCacheGets() {
        return reads;
    }

    /** {@inheritDoc} */
    @Override public long getCachePuts() {
        return writes;
    }

    /** {@inheritDoc} */
    @Override public long getCacheRemovals() {
        return rmCnt;
    }

    /** {@inheritDoc} */
    @Override public long getCacheEvictions() {
        return evictCnt;
    }

    /**
     * Increments the get time accumulator.
     *
     * @param duration the time taken in nanoseconds.
     */
    public void addGetTimeNanos(long duration) {
        getTimeNanos += duration;

        if (delegate != null)
            delegate.addGetTimeNanos(duration);
    }

    /**
     * Increments the put time accumulator.
     *
     * @param duration the time taken in nanoseconds.
     */
    public void addPutTimeNanos(long duration) {
        putTimeNanos += duration;

        if (delegate != null)
            delegate.addPutTimeNanos(duration);
    }

    /**
     * Increments the remove time accumulator.
     *
     * @param duration the time taken in nanoseconds.
     */
    public void addRemoveTimeNanos(long duration) {
        removeTimeNanos += duration;

        if (delegate != null)
            delegate.addRemoveTimeNanos(duration);
    }

    /**
     * Increments remove and get time accumulators.
     *
     * @param duration the time taken in nanoseconds.
     */
    public void addRemoveAndGetTimeNanos(long duration) {
        removeTimeNanos += duration;
        getTimeNanos += duration;

        if (delegate != null)
            delegate.addRemoveAndGetTimeNanos(duration);
    }

    /**
     * Increments put and get time accumulators.
     *
     * @param duration the time taken in nanoseconds.
     */
    public void addPutAndGetTimeNanos(long duration) {
        putTimeNanos += duration;
        getTimeNanos += duration;

        if (delegate != null)
            delegate.addPutAndGetTimeNanos(duration);
    }

    /** {@inheritDoc} */
    @Override public float getAverageGetTime() {
        long timeNanos = getTimeNanos;
        long readsCnt = reads;

        if (timeNanos == 0 || readsCnt == 0)
            return 0;

        return ((1f * timeNanos) / readsCnt) / NANOS_IN_MICROSECOND;
    }

    /** {@inheritDoc} */
    @Override public float getAveragePutTime() {
        long timeNanos = putTimeNanos;
        long putsCnt = writes;

        if (timeNanos == 0 || putsCnt == 0)
            return 0;

        return ((1f * timeNanos) / putsCnt) / NANOS_IN_MICROSECOND;
    }

    /** {@inheritDoc} */
    @Override public float getAverageRemoveTime() {
        long timeNanos = removeTimeNanos;
        long removesCnt = rmCnt;

        if (timeNanos == 0 || removesCnt == 0)
            return 0;

        return ((1f * timeNanos) / removesCnt) / NANOS_IN_MICROSECOND;
    }

    /**
     * Cache read callback.
     * @param isHit Hit or miss flag.
     */
    public void onRead(boolean isHit) {
        readTime = U.currentTimeMillis();

        reads++;

        if (isHit)
            hits++;
        else
            misses++;

        if (delegate != null)
            delegate.onRead(isHit);
    }

    /**
     * Cache write callback.
     */
    public void onWrite() {
        writeTime = U.currentTimeMillis();

        writes++;

        if (delegate != null)
            delegate.onWrite();
    }

    /**
     * Cache remove callback.
     */
    public void onRemove(){
        writeTime = U.currentTimeMillis();

        rmCnt++;

        if (delegate != null)
            delegate.onRemove();
    }

    /**
     * Cache remove callback.
     */
    public void onEvict() {
        evictCnt++;

        if (delegate != null)
            delegate.onEvict();
    }

    /**
     * Transaction commit callback.
     */
    public void onTxCommit() {
        commitTime = U.currentTimeMillis();

        txCommits++;

        if (delegate != null)
            delegate.onTxCommit();
    }

    /**
     * Transaction rollback callback.
     */
    public void onTxRollback() {
        rollbackTime = U.currentTimeMillis();

        txRollbacks++;

        if (delegate != null)
            delegate.onTxRollback();
    }

    /**
     * Gets remove time.
     *
     * @return Remove time taken nanos.
     */
    public long removeTimeNanos() {
        return removeTimeNanos;
    }

    /**
     * Gets get time.
     *
     * @return Get time taken nanos.
     */
    public long getTimeNanos() {
        return getTimeNanos;
    }

    /**
     * Gets put time.
     *
     * @return Get time taken nanos.
     */
    public long putTimeNanos() {
        return putTimeNanos;
    }

    /**
     * Create a copy of given metrics object.
     *
     * @param m Metrics to copy from.
     * @return Copy of given metrics.
     */
    @Nullable public static GridCacheMetricsAdapter copyOf(@Nullable GridCacheMetricsAdapter m) {
        if (m == null)
            return null;

        return new GridCacheMetricsAdapter(m);
    }

    /** {@inheritDoc} */
    @Override public void writeExternal(ObjectOutput out) throws IOException {
        out.writeLong(createTime);
        out.writeLong(readTime);
        out.writeLong(writeTime);
        out.writeLong(commitTime);
        out.writeLong(rollbackTime);

        out.writeInt(reads);
        out.writeInt(writes);
        out.writeInt(hits);
        out.writeInt(misses);
        out.writeInt(txCommits);
        out.writeInt(txRollbacks);
        out.writeLong(rmCnt);
        out.writeLong(evictCnt);
    }

    /** {@inheritDoc} */
    @Override public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {
        createTime = in.readLong();
        readTime = in.readLong();
        writeTime = in.readLong();
        commitTime = in.readLong();
        rollbackTime = in.readLong();

        reads = in.readInt();
        writes = in.readInt();
        hits = in.readInt();
        misses = in.readInt();
        txCommits = in.readInt();
        txRollbacks = in.readInt();
        rmCnt = in.readLong();
        evictCnt = in.readLong();
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(GridCacheMetricsAdapter.class, this);
    }
}
