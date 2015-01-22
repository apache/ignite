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

import javax.cache.management.CacheStatisticsMXBean;
import java.io.*;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Adapter for cache metrics.
 */
public class CacheMetricsMxBeanImpl implements CacheMetricsMxBean, CacheStatisticsMXBean, Externalizable {
    /** */
    private static final long NANOS_IN_MICROSECOND = 1000L;

    /** */
    private static final long serialVersionUID = 0L;

    /** Number of reads. */
    private AtomicLong reads = new AtomicLong();

    /** Number of writes. */
    private AtomicLong writes = new AtomicLong();

    /** Number of hits. */
    private AtomicLong hits = new AtomicLong();

    /** Number of misses. */
    private AtomicLong misses = new AtomicLong();

    /** Number of transaction commits. */
    private AtomicLong txCommits = new AtomicLong();

    /** Number of transaction rollbacks. */
    private AtomicLong txRollbacks = new AtomicLong();

    /** Number of evictions. */
    private AtomicLong evictCnt = new AtomicLong();

    /** Number of removed entries. */
    private AtomicLong rmCnt = new AtomicLong();

    /** Put time taken nanos. */
    private AtomicLong putTimeNanos = new AtomicLong();

    /** Get time taken nanos. */
    private AtomicLong getTimeNanos = new AtomicLong();

    /** Remove time taken nanos. */
    private AtomicLong removeTimeNanos = new AtomicLong();

    /** Commit transaction time taken nanos. */
    private AtomicLong commitTimeNanos = new AtomicLong();

    /** Commit transaction time taken nanos. */
    private AtomicLong rollbackTimeNanos = new AtomicLong();

    /** Cache metrics. */
    @GridToStringExclude
    private transient CacheMetricsMxBeanImpl delegate;

    /**
     * No-args constructor.
     */
    public CacheMetricsMxBeanImpl() {
        delegate = null;
    }

    /**
     * @param delegate Metrics to delegate to.
     */
    public void delegate(CacheMetricsMxBeanImpl delegate) {
        this.delegate = delegate;
    }

    /** {@inheritDoc} */
    @Override public float getAverageTxCommitTime() {
        long timeNanos = commitTimeNanos.get();
        long commitsCnt = txCommits.get();

        if (timeNanos == 0 || commitsCnt == 0)
            return 0;

        return ((1f * timeNanos) / commitsCnt) / NANOS_IN_MICROSECOND;
    }

    /** {@inheritDoc} */
    @Override public float getAverageTxRollbackTime() {
        long timeNanos = rollbackTimeNanos.get();
        long rollbacksCnt = txRollbacks.get();

        if (timeNanos == 0 || rollbacksCnt == 0)
            return 0;

        return ((1f * timeNanos) / rollbacksCnt) / NANOS_IN_MICROSECOND;
    }

    /** {@inheritDoc} */
    @Override public long getCacheTxCommits() {
        return txCommits.get();
    }

    /** {@inheritDoc} */
    @Override public long getCacheTxRollbacks() {
        return txRollbacks.get();
    }

    /** {@inheritDoc} */
    @Override public void clear() {
        reads.set(0);
        writes.set(0);
        rmCnt.set(0);
        hits.set(0);
        misses.set(0);
        evictCnt.set(0);
        txCommits.set(0);
        txRollbacks.set(0);
        putTimeNanos.set(0);
        removeTimeNanos.set(0);
        getTimeNanos.set(0);
        commitTimeNanos.set(0);
        rollbackTimeNanos.set(0);

        if (delegate != null)
            delegate.clear();
    }

    /** {@inheritDoc} */
    @Override public long getCacheHits() {
        return hits.get();
    }

    /** {@inheritDoc} */
    @Override public float getCacheHitPercentage() {
        long hits0 = hits.get();
        long gets0 = reads.get();

        if (hits0 == 0)
            return 0;

        return (float) hits0 / gets0 * 100.0f;
    }

    /** {@inheritDoc} */
    @Override public long getCacheMisses() {
        return misses.get();
    }

    /** {@inheritDoc} */
    @Override public float getCacheMissPercentage() {
        long misses0 = misses.get();
        long reads0 = reads.get();

        if (misses0 == 0) {
            return 0;
        }

        return (float) misses0 / reads0 * 100.0f;
    }

    /** {@inheritDoc} */
    @Override public long getCacheGets() {
        return reads.get();
    }

    /** {@inheritDoc} */
    @Override public long getCachePuts() {
        return writes.get();
    }

    /** {@inheritDoc} */
    @Override public long getCacheRemovals() {
        return rmCnt.get();
    }

    /** {@inheritDoc} */
    @Override public long getCacheEvictions() {
        return evictCnt.get();
    }

    /** {@inheritDoc} */
    @Override public float getAverageGetTime() {
        long timeNanos = getTimeNanos.get();
        long readsCnt = reads.get();

        if (timeNanos == 0 || readsCnt == 0)
            return 0;

        return ((1f * timeNanos) / readsCnt) / NANOS_IN_MICROSECOND;
    }

    /** {@inheritDoc} */
    @Override public float getAveragePutTime() {
        long timeNanos = putTimeNanos.get();
        long putsCnt = writes.get();

        if (timeNanos == 0 || putsCnt == 0)
            return 0;

        return ((1f * timeNanos) / putsCnt) / NANOS_IN_MICROSECOND;
    }

    /** {@inheritDoc} */
    @Override public float getAverageRemoveTime() {
        long timeNanos = removeTimeNanos.get();
        long removesCnt = rmCnt.get();

        if (timeNanos == 0 || removesCnt == 0)
            return 0;

        return ((1f * timeNanos) / removesCnt) / NANOS_IN_MICROSECOND;
    }

    /**
     * Cache read callback.
     * @param isHit Hit or miss flag.
     */
    public void onRead(boolean isHit) {
        reads.incrementAndGet();

        if (isHit)
            hits.incrementAndGet();
        else
            misses.incrementAndGet();

        if (delegate != null)
            delegate.onRead(isHit);
    }

    /**
     * Cache write callback.
     */
    public void onWrite() {
        writes.incrementAndGet();

        if (delegate != null)
            delegate.onWrite();
    }

    /**
     * Cache remove callback.
     */
    public void onRemove(){
        rmCnt.incrementAndGet();

        if (delegate != null)
            delegate.onRemove();
    }

    /**
     * Cache remove callback.
     */
    public void onEvict() {
        evictCnt.incrementAndGet();

        if (delegate != null)
            delegate.onEvict();
    }

    /**
     * Transaction commit callback.
     */
    public void onTxCommit(long duration) {
        txCommits.incrementAndGet();
        commitTimeNanos.addAndGet(duration);

        if (delegate != null) {
            delegate.onTxCommit(duration);
        }
    }

    /**
     * Transaction rollback callback.
     */
    public void onTxRollback(long duration) {
        txRollbacks.incrementAndGet();
        rollbackTimeNanos.addAndGet(duration);

        if (delegate != null)
            delegate.onTxRollback(duration);
    }


    /**
     * Increments the get time accumulator.
     *
     * @param duration the time taken in nanoseconds.
     */
    public void addGetTimeNanos(long duration) {
        getTimeNanos.addAndGet(duration);

        if (delegate != null)
            delegate.addGetTimeNanos(duration);
    }

    /**
     * Increments the put time accumulator.
     *
     * @param duration the time taken in nanoseconds.
     */
    public void addPutTimeNanos(long duration) {
        putTimeNanos.addAndGet(duration);

        if (delegate != null)
            delegate.addPutTimeNanos(duration);
    }

    /**
     * Increments the remove time accumulator.
     *
     * @param duration the time taken in nanoseconds.
     */
    public void addRemoveTimeNanos(long duration) {
        removeTimeNanos.addAndGet(duration);

        if (delegate != null)
            delegate.addRemoveTimeNanos(duration);
    }

    /**
     * Increments remove and get time accumulators.
     *
     * @param duration the time taken in nanoseconds.
     */
    public void addRemoveAndGetTimeNanos(long duration) {
        removeTimeNanos.addAndGet(duration);
        getTimeNanos.addAndGet(duration);

        if (delegate != null)
            delegate.addRemoveAndGetTimeNanos(duration);
    }

    /**
     * Increments put and get time accumulators.
     *
     * @param duration the time taken in nanoseconds.
     */
    public void addPutAndGetTimeNanos(long duration) {
        putTimeNanos.addAndGet(duration);
        getTimeNanos.addAndGet(duration);

        if (delegate != null)
            delegate.addPutAndGetTimeNanos(duration);
    }

    /** {@inheritDoc} */
    @Override public void writeExternal(ObjectOutput out) throws IOException {
        out.writeLong(reads.get());
        out.writeLong(writes.get());
        out.writeLong(hits.get());
        out.writeLong(misses.get());
        out.writeLong(txCommits.get());
        out.writeLong(txRollbacks.get());
        out.writeLong(rmCnt.get());
        out.writeLong(evictCnt.get());

        out.writeLong(putTimeNanos.get());
        out.writeLong(getTimeNanos.get());
        out.writeLong(removeTimeNanos.get());
        out.writeLong(commitTimeNanos.get());
        out.writeLong(rollbackTimeNanos.get());
    }

    /** {@inheritDoc} */
    @Override public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {
        reads = new AtomicLong(in.readLong());
        writes = new AtomicLong(in.readLong());
        hits = new AtomicLong(in.readLong());
        misses = new AtomicLong(in.readLong());
        txCommits = new AtomicLong(in.readLong());
        txRollbacks = new AtomicLong(in.readLong());
        rmCnt = new AtomicLong(in.readLong());
        evictCnt = new AtomicLong(in.readLong());

        putTimeNanos = new AtomicLong(in.readLong());
        getTimeNanos = new AtomicLong(in.readLong());
        removeTimeNanos = new AtomicLong(in.readLong());
        commitTimeNanos = new AtomicLong(in.readLong());
        rollbackTimeNanos = new AtomicLong(in.readLong());
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(CacheMetricsMxBeanImpl.class, this);
    }
}
