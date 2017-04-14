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

package org.apache.ignite.internal.visor.cache;

import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.cache.CacheMetrics;
import org.apache.ignite.cache.CacheMode;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.internal.processors.cache.GridCacheAdapter;
import org.apache.ignite.internal.processors.cache.GridCacheProcessor;
import org.apache.ignite.internal.util.typedef.internal.S;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.internal.visor.VisorDataTransferObject;
import org.apache.ignite.internal.visor.query.VisorQueryMetrics;

/**
 * Data transfer object for {@link CacheMetrics}.
 */
public class VisorCacheMetrics extends VisorDataTransferObject {
    /** */
    private static final long serialVersionUID = 0L;

    /** */
    private static final float MICROSECONDS_IN_SECOND = 1_000_000;

    /** Cache name. */
    private String name;

    /** Cache mode. */
    private CacheMode mode;

    /** Cache system state. */
    private boolean sys;

    /** Number of non-{@code null} values in the cache. */
    private int size;

    /** Gets number of keys in the cache, possibly with {@code null} values. */
    private int keySize;

    /** Total number of reads of the owning entity (either cache or entry). */
    private long reads;

    /** The mean time to execute gets. */
    private float avgReadTime;

    /** Total number of writes of the owning entity (either cache or entry). */
    private long writes;

    /** Total number of hits for the owning entity (either cache or entry). */
    private long hits;

    /** Total number of misses for the owning entity (either cache or entry). */
    private long misses;

    /** Total number of transaction commits. */
    private long txCommits;

    /** The mean time to execute tx commit. */
    private float avgTxCommitTime;

    /** Total number of transaction rollbacks. */
    private long txRollbacks;

    /** The mean time to execute tx rollbacks. */
    private float avgTxRollbackTime;

    /** The total number of puts to the cache. */
    private long puts;

    /** The mean time to execute puts. */
    private float avgPutTime;

    /** The total number of removals from the cache. */
    private long removals;

    /** The mean time to execute removes. */
    private float avgRemovalTime;

    /** The total number of evictions from the cache. */
    private long evictions;

    /** Reads per second. */
    private int readsPerSec;

    /** Puts per second. */
    private int putsPerSec;

    /** Removes per second. */
    private int removalsPerSec;

    /** Commits per second. */
    private int commitsPerSec;

    /** Rollbacks per second. */
    private int rollbacksPerSec;

    /** Gets query metrics for cache. */
    private VisorQueryMetrics qryMetrics;

    /** Current size of evict queue used to batch up evictions. */
    private int dhtEvictQueueCurrSize;

    /** Gets transaction per-thread map size. */
    private int txThreadMapSize;

    /** Transaction per-Xid map size. */
    private int txXidMapSize;

    /** Committed transaction queue size. */
    private int txCommitQueueSize;

    /** Prepared transaction queue size. */
    private int txPrepareQueueSize;

    /** Start version counts map size. */
    private int txStartVerCountsSize;

    /** Number of cached committed transaction IDs. */
    private int txCommittedVersionsSize;

    /** Number of cached rolled back transaction IDs. */
    private int txRolledbackVersionsSize;

    /** DHT thread map size */
    private int txDhtThreadMapSize;

    /** Transaction DHT per-Xid map size. */
    private int txDhtXidMapSize;

    /** Committed DHT transaction queue size. */
    private int txDhtCommitQueueSize;

    /** Prepared DHT transaction queue size. */
    private int txDhtPrepareQueueSize;

    /** DHT start version counts map size. */
    private int txDhtStartVerCountsSize;

    /** Number of cached committed DHT transaction IDs. */
    private int txDhtCommittedVersionsSize;

    /** Number of cached rolled back DHT transaction IDs. */
    private int txDhtRolledbackVersionsSize;

    /** Memory size allocated in off-heap. */
    private long offHeapAllocatedSize;

    /** Number of cache entries stored in off-heap memory. */
    private long offHeapEntriesCnt;

    /**
     * Calculate rate of metric per second.
     *
     * @param meanTime Metric mean time.
     * @return Metric per second.
     */
    private static int perSecond(float meanTime) {
        return (meanTime > 0) ? (int)(MICROSECONDS_IN_SECOND / meanTime) : 0;
    }

    /**
     * Default constructor.
     */
    public VisorCacheMetrics() {
        // No-op.
    }

    /**
     * Create data transfer object for given cache metrics.
     *
     * @param ignite Ignite.
     * @param cacheName Cache name.
     */
    public VisorCacheMetrics(IgniteEx ignite, String cacheName) {
        GridCacheProcessor cacheProcessor = ignite.context().cache();

        IgniteCache<Object, Object> c = cacheProcessor.jcache(cacheName);

        name = cacheName;
        mode = cacheProcessor.cacheMode(cacheName);
        sys = cacheProcessor.systemCache(cacheName);

        CacheMetrics m = c.localMetrics();

        size = m.getSize();
        keySize = m.getKeySize();

        reads = m.getCacheGets();
        writes = m.getCachePuts() + m.getCacheRemovals();
        hits = m.getCacheHits();
        misses = m.getCacheMisses();

        txCommits = m.getCacheTxCommits();
        txRollbacks = m.getCacheTxRollbacks();

        avgTxCommitTime = m.getAverageTxCommitTime();
        avgTxRollbackTime = m.getAverageTxRollbackTime();

        puts = m.getCachePuts();
        removals = m.getCacheRemovals();
        evictions = m.getCacheEvictions();

        avgReadTime = m.getAverageGetTime();
        avgPutTime = m.getAveragePutTime();
        avgRemovalTime = m.getAverageRemoveTime();

        readsPerSec = perSecond(m.getAverageGetTime());
        putsPerSec = perSecond(m.getAveragePutTime());
        removalsPerSec = perSecond(m.getAverageRemoveTime());
        commitsPerSec = perSecond(m.getAverageTxCommitTime());
        rollbacksPerSec = perSecond(m.getAverageTxRollbackTime());

        qryMetrics = new VisorQueryMetrics(c.queryMetrics());

        dhtEvictQueueCurrSize = m.getDhtEvictQueueCurrentSize();
        txThreadMapSize = m.getTxThreadMapSize();
        txXidMapSize = m.getTxXidMapSize();
        txCommitQueueSize = m.getTxCommitQueueSize();
        txPrepareQueueSize = m.getTxPrepareQueueSize();
        txStartVerCountsSize = m.getTxStartVersionCountsSize();
        txCommittedVersionsSize = m.getTxCommittedVersionsSize();
        txRolledbackVersionsSize = m.getTxRolledbackVersionsSize();
        txDhtThreadMapSize = m.getTxDhtThreadMapSize();
        txDhtXidMapSize = m.getTxDhtXidMapSize();
        txDhtCommitQueueSize = m.getTxDhtCommitQueueSize();
        txDhtPrepareQueueSize = m.getTxDhtPrepareQueueSize();
        txDhtStartVerCountsSize = m.getTxDhtStartVersionCountsSize();
        txDhtCommittedVersionsSize = m.getTxDhtCommittedVersionsSize();
        txDhtRolledbackVersionsSize = m.getTxDhtRolledbackVersionsSize();

        GridCacheAdapter<Object, Object> ca = cacheProcessor.internalCache(cacheName);

        offHeapAllocatedSize = ca.offHeapAllocatedSize();
        offHeapEntriesCnt = ca.offHeapEntriesCount();
    }

    /**
     * @return Cache name.
     */
    public String getName() {
        return name;
    }

    /**
     * Sets cache name.
     *
     * @param name New value for cache name.
     */
    public void setName(String name) {
        this.name = name;
    }

    /**
     * @return Cache mode.
     */
    public CacheMode getMode() {
        return mode;
    }

    /**
     * @return Cache system state.
     */
    public boolean isSystem() {
        return sys;
    }

    /**
     * @return Total number of reads of the owning entity (either cache or entry).
     */
    public long getReads() {
        return reads;
    }

    /**
     * @return The mean time to execute gets
     */
    public float getAvgReadTime() {
        return avgReadTime;
    }

    /**
     * @return Total number of writes of the owning entity (either cache or entry).
     */
    public long getWrites() {
        return writes;
    }

    /**
     * @return Total number of hits for the owning entity (either cache or entry).
     */
    public long getHits() {
        return hits;
    }

    /**
     * @return Total number of misses for the owning entity (either cache or entry).
     */
    public long getMisses() {
        return misses;
    }

    /**
     * @return Total number of transaction commits.
     */
    public long getTxCommits() {
        return txCommits;
    }

    /**
     * @return avgTxCommitTime
     */
    public float getAvgTxCommitTime() {
        return avgTxCommitTime;
    }

    /**
     * @return The mean time to execute tx rollbacks.
     */
    public float getAvgTxRollbackTime() {
        return avgTxRollbackTime;
    }

    /**
     * @return The total number of puts to the cache.
     */
    public long getPuts() {
        return puts;
    }

    /**
     * @return The mean time to execute puts.
     */
    public float getAvgPutTime() {
        return avgPutTime;
    }

    /**
     * @return The total number of removals from the cache.
     */
    public long getRemovals() {
        return removals;
    }

    /**
     * @return The mean time to execute removes.
     */
    public float getAvgRemovalTime() {
        return avgRemovalTime;
    }

    /**
     * @return The total number of evictions from the cache.
     */
    public long getEvictions() {
        return evictions;
    }

    /**
     * @return Total number of transaction rollbacks.
     */
    public long getTxRollbacks() {
        return txRollbacks;
    }

    /**
     * @return Reads per second.
     */
    public int getReadsPerSecond() {
        return readsPerSec;
    }

    /**
     * @return Puts per second.
     */
    public int getPutsPerSecond() {
        return putsPerSec;
    }

    /**
     * @return Removes per second.
     */
    public int getRemovalsPerSecond() {
        return removalsPerSec;
    }

    /**
     * @return Commits per second.
     */
    public int getCommitsPerSecond() {
        return commitsPerSec;
    }

    /**
     * @return Rollbacks per second.
     */
    public int getRollbacksPerSecond() {
        return rollbacksPerSec;
    }

    /**
     * @return Number of non-{@code null} values in the cache.
     */
    public int getSize() {
        return size;
    }

    /**
     * @return Gets number of keys in the cache, possibly with {@code null} values.
     */
    public int getKeySize() {
        return keySize;
    }

    /**
     * @return Gets query metrics for cache.
     */
    public VisorQueryMetrics getQueryMetrics() {
        return qryMetrics;
    }

    /**
     * @return Current size of evict queue used to batch up evictions.
     */
    public int getDhtEvictQueueCurrentSize() {
        return dhtEvictQueueCurrSize;
    }

    /**
     * @return Gets transaction per-thread map size.
     */
    public int getTxThreadMapSize() {
        return txThreadMapSize;
    }

    /**
     * @return Transaction per-Xid map size.
     */
    public int getTxXidMapSize() {
        return txXidMapSize;
    }

    /**
     * @return Committed transaction queue size.
     */
    public int getTxCommitQueueSize() {
        return txCommitQueueSize;
    }

    /**
     * @return Prepared transaction queue size.
     */
    public int getTxPrepareQueueSize() {
        return txPrepareQueueSize;
    }

    /**
     * @return Start version counts map size.
     */
    public int getTxStartVersionCountsSize() {
        return txStartVerCountsSize;
    }

    /**
     * @return Number of cached committed transaction IDs.
     */
    public int getTxCommittedVersionsSize() {
        return txCommittedVersionsSize;
    }

    /**
     * @return Number of cached rolled back transaction IDs.
     */
    public int getTxRolledbackVersionsSize() {
        return txRolledbackVersionsSize;
    }

    /**
     * @return DHT thread map size
     */
    public int getTxDhtThreadMapSize() {
        return txDhtThreadMapSize;
    }

    /**
     * @return Transaction DHT per-Xid map size.
     */
    public int getTxDhtXidMapSize() {
        return txDhtXidMapSize;
    }

    /**
     * @return Committed DHT transaction queue size.
     */
    public int getTxDhtCommitQueueSize() {
        return txDhtCommitQueueSize;
    }

    /**
     * @return Prepared DHT transaction queue size.
     */
    public int getTxDhtPrepareQueueSize() {
        return txDhtPrepareQueueSize;
    }

    /**
     * @return DHT start version counts map size.
     */
    public int getTxDhtStartVersionCountsSize() {
        return txDhtStartVerCountsSize;
    }

    /**
     * @return Number of cached committed DHT transaction IDs.
     */
    public int getTxDhtCommittedVersionsSize() {
        return txDhtCommittedVersionsSize;
    }

    /**
     * @return Number of cached rolled back DHT transaction IDs.
     */
    public int getTxDhtRolledbackVersionsSize() {
        return txDhtRolledbackVersionsSize;
    }

    /**
     * @return Memory size allocated in off-heap.
     */
    public long getOffHeapAllocatedSize() {
        return offHeapAllocatedSize;
    }

    /**
     * @return Number of cache entries stored in off-heap memory.
     */
    public long offHeapEntriesCount() {
        return offHeapEntriesCnt;
    }

    /** {@inheritDoc} */
    @Override protected void writeExternalData(ObjectOutput out) throws IOException {
        U.writeString(out, name);
        U.writeEnum(out, mode);
        out.writeBoolean(sys);
        out.writeInt(size);
        out.writeInt(keySize);
        out.writeLong(reads);
        out.writeFloat(avgReadTime);
        out.writeLong(writes);
        out.writeLong(hits);
        out.writeLong(misses);
        out.writeLong(txCommits);
        out.writeFloat(avgTxCommitTime);
        out.writeLong(txRollbacks);
        out.writeFloat(avgTxRollbackTime);
        out.writeLong(puts);
        out.writeFloat(avgPutTime);
        out.writeLong(removals);
        out.writeFloat(avgRemovalTime);
        out.writeLong(evictions);
        out.writeInt(readsPerSec);
        out.writeInt(putsPerSec);
        out.writeInt(removalsPerSec);
        out.writeInt(commitsPerSec);
        out.writeInt(rollbacksPerSec);
        out.writeInt(dhtEvictQueueCurrSize);
        out.writeInt(txThreadMapSize);
        out.writeInt(txXidMapSize);
        out.writeInt(txCommitQueueSize);
        out.writeInt(txPrepareQueueSize);
        out.writeInt(txStartVerCountsSize);
        out.writeInt(txCommittedVersionsSize);
        out.writeInt(txRolledbackVersionsSize);
        out.writeInt(txDhtThreadMapSize);
        out.writeInt(txDhtXidMapSize);
        out.writeInt(txDhtCommitQueueSize);
        out.writeInt(txDhtPrepareQueueSize);
        out.writeInt(txDhtStartVerCountsSize);
        out.writeInt(txDhtCommittedVersionsSize);
        out.writeInt(txDhtRolledbackVersionsSize);
        out.writeLong(offHeapAllocatedSize);
        out.writeLong(offHeapEntriesCnt);
        out.writeObject(qryMetrics);
    }

    /** {@inheritDoc} */
    @Override protected void readExternalData(byte protoVer, ObjectInput in) throws IOException, ClassNotFoundException {
        name = U.readString(in);
        mode = CacheMode.fromOrdinal(in.readByte());
        sys = in.readBoolean();
        size = in.readInt();
        keySize = in.readInt();
        reads = in.readLong();
        avgReadTime = in.readFloat();
        writes = in.readLong();
        hits = in.readLong();
        misses = in.readLong();
        txCommits = in.readLong();
        avgTxCommitTime = in.readFloat();
        txRollbacks = in.readLong();
        avgTxRollbackTime = in.readFloat();
        puts = in.readLong();
        avgPutTime = in.readFloat();
        removals = in.readLong();
        avgRemovalTime = in.readFloat();
        evictions = in.readLong();
        readsPerSec = in.readInt();
        putsPerSec = in.readInt();
        removalsPerSec = in.readInt();
        commitsPerSec = in.readInt();
        rollbacksPerSec = in.readInt();
        dhtEvictQueueCurrSize = in.readInt();
        txThreadMapSize = in.readInt();
        txXidMapSize = in.readInt();
        txCommitQueueSize = in.readInt();
        txPrepareQueueSize = in.readInt();
        txStartVerCountsSize = in.readInt();
        txCommittedVersionsSize = in.readInt();
        txRolledbackVersionsSize = in.readInt();
        txDhtThreadMapSize = in.readInt();
        txDhtXidMapSize = in.readInt();
        txDhtCommitQueueSize = in.readInt();
        txDhtPrepareQueueSize = in.readInt();
        txDhtStartVerCountsSize = in.readInt();
        txDhtCommittedVersionsSize = in.readInt();
        txDhtRolledbackVersionsSize = in.readInt();
        offHeapAllocatedSize = in.readLong();
        offHeapEntriesCnt = in.readLong();
        qryMetrics = (VisorQueryMetrics)in.readObject();
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(VisorCacheMetrics.class, this);
    }
}
