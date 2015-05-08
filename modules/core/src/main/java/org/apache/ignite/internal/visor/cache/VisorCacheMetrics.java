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

import org.apache.ignite.cache.*;
import org.apache.ignite.internal.processors.cache.*;
import org.apache.ignite.internal.util.typedef.internal.*;

import java.io.*;

/**
 * Data transfer object for {@link org.apache.ignite.cache.CacheMetrics}.
 */
public class VisorCacheMetrics implements Serializable {
    /** */
    private static final int MICROSECONDS_IN_SECOND = 1_000_000;

    /** */
    private static final long serialVersionUID = 0L;

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

    /** Writes per second. */
    private int writesPerSec;

    /** Hits per second. */
    private int hitsPerSec;

    /** Misses per second. */
    private int missesPerSec;

    /** Commits per second. */
    private int commitsPerSec;

    /** Rollbacks per second. */
    private int rollbacksPerSec;

    /** Gets query metrics for cache. */
    private VisorCacheQueryMetrics qryMetrics;

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

    /** Calculate rate of metric per second. */
    private static int perSecond(int metric, long time, long createTime) {
        long seconds = (time - createTime) / 1000;

        return (seconds > 0) ? (int)(metric / seconds) : 0;
    }

    /**
     * @param c Cache.
     * @return Data transfer object for given cache metrics.
     */
    public static VisorCacheMetrics from(GridCache c) {
        VisorCacheMetrics cm = new VisorCacheMetrics();

        CacheMetrics m = c.metrics();

        cm.size = m.getSize();
        cm.keySize = m.getKeySize();

        cm.reads = m.getCacheGets();
        cm.writes = m.getCachePuts() + m.getCacheRemovals();
        cm.hits = m.getCacheHits();
        cm.misses = m.getCacheMisses();

        cm.txCommits = m.getCacheTxCommits();
        cm.txRollbacks = m.getCacheTxRollbacks();

        cm.avgTxCommitTime = m.getAverageTxCommitTime();
        cm.avgTxRollbackTime = m.getAverageTxRollbackTime();

        cm.puts = m.getCachePuts();
        cm.removals = m.getCacheRemovals();
        cm.evictions = m.getCacheEvictions();

        cm.avgReadTime = m.getAverageGetTime();
        cm.avgPutTime = m.getAveragePutTime();
        cm.avgRemovalTime = m.getAverageRemoveTime();

        cm.readsPerSec = (int)(MICROSECONDS_IN_SECOND * 1.f / m.getAverageGetTime());
        cm.writesPerSec = (int)(MICROSECONDS_IN_SECOND * 1.f / m.getAveragePutTime());
        cm.hitsPerSec = -1;
        cm.missesPerSec = (int)(MICROSECONDS_IN_SECOND * 1.f / m.getAverageRemoveTime());
        cm.commitsPerSec = (int)(MICROSECONDS_IN_SECOND * 1.f / m.getAverageTxCommitTime());
        cm.rollbacksPerSec = (int)(MICROSECONDS_IN_SECOND * 1.f / m.getAverageTxRollbackTime());

        cm.qryMetrics = VisorCacheQueryMetrics.from(c.queries().metrics());

        cm.dhtEvictQueueCurrSize = m.getDhtEvictQueueCurrentSize();
        cm.txThreadMapSize = m.getTxThreadMapSize();
        cm.txXidMapSize = m.getTxXidMapSize();
        cm.txCommitQueueSize = m.getTxCommitQueueSize();
        cm.txPrepareQueueSize = m.getTxPrepareQueueSize();
        cm.txStartVerCountsSize = m.getTxStartVersionCountsSize();
        cm.txCommittedVersionsSize = m.getTxCommittedVersionsSize();
        cm.txRolledbackVersionsSize = m.getTxRolledbackVersionsSize();
        cm.txDhtThreadMapSize = m.getTxDhtThreadMapSize();
        cm.txDhtXidMapSize = m.getTxDhtXidMapSize();
        cm.txDhtCommitQueueSize = m.getTxDhtCommitQueueSize();
        cm.txDhtPrepareQueueSize = m.getTxDhtPrepareQueueSize();
        cm.txDhtStartVerCountsSize = m.getTxDhtStartVersionCountsSize();
        cm.txDhtCommittedVersionsSize = m.getTxDhtCommittedVersionsSize();
        cm.txDhtRolledbackVersionsSize = m.getTxDhtRolledbackVersionsSize();

        return cm;
    }

    /**
     * @return Total number of reads of the owning entity (either cache or entry).
     */
    public long reads() {
        return reads;
    }

    /**
     * @return The mean time to execute gets
     */
    public float avgReadTime() {
        return avgReadTime;
    }

    /**
     * @return Total number of writes of the owning entity (either cache or entry).
     */
    public long writes() {
        return writes;
    }

    /**
     * @return Total number of hits for the owning entity (either cache or entry).
     */
    public long hits() {
        return hits;
    }

    /**
     * @return Total number of misses for the owning entity (either cache or entry).
     */
    public long misses() {
        return misses;
    }

    /**
     * @return Total number of transaction commits.
     */
    public long txCommits() {
        return txCommits;
    }

    /**
     * @return avgTxCommitTime
     */
    public float avgTxCommitTime() {
        return avgTxCommitTime;
    }

    /**
     * @return The mean time to execute tx rollbacks.
     */
    public float avgTxRollbackTime() {
        return avgTxRollbackTime;
    }

    /**
     * @return The total number of puts to the cache.
     */
    public long puts() {
        return puts;
    }

    /**
     * @return The mean time to execute puts.
     */
    public float avgPutTime() {
        return avgPutTime;
    }

    /**
     * @return The total number of removals from the cache.
     */
    public long removals() {
        return removals;
    }

    /**
     * @return The mean time to execute removes.
     */
    public float avgRemovalTime() {
        return avgRemovalTime;
    }

    /**
     * @return The total number of evictions from the cache.
     */
    public long evictions() {
        return evictions;
    }

    /**
     * @return Total number of transaction rollbacks.
     */
    public long txRollbacks() {
        return txRollbacks;
    }

    /**
     * @return Reads per second.
     */
    public int readsPerSecond() {
        return readsPerSec;
    }

    /**
     * @return Writes per second.
     */
    public int writesPerSecond() {
        return writesPerSec;
    }

    /**
     * @return Hits per second.
     */
    public int hitsPerSecond() {
        return hitsPerSec;
    }

    /**
     * @return Misses per second.
     */
    public int missesPerSecond() {
        return missesPerSec;
    }

    /**
     * @return Commits per second.
     */
    public int commitsPerSecond() {
        return commitsPerSec;
    }

    /**
     * @return Rollbacks per second.
     */
    public int rollbacksPerSecond() {
        return rollbacksPerSec;
    }

    /**
     * @return Number of non-{@code null} values in the cache.
     */
    public int size() {
        return size;
    }

    /**
     * @return Gets number of keys in the cache, possibly with {@code null} values.
     */
    public int keySize() {
        return keySize;
    }

    /**
     * @return Gets query metrics for cache.
     */
    public VisorCacheQueryMetrics queryMetrics() {
        return qryMetrics;
    }

    /**
     * @return Current size of evict queue used to batch up evictions.
     */
    public int dhtEvictQueueCurrentSize() {
        return dhtEvictQueueCurrSize;
    }

    /**
     * @return Gets transaction per-thread map size.
     */
    public int txThreadMapSize() {
        return txThreadMapSize;
    }

    /**
     * @return Transaction per-Xid map size.
     */
    public int txXidMapSize() {
        return txXidMapSize;
    }

    /**
     * Committed transaction queue size.
     */
    public int txCommitQueueSize() {
        return txCommitQueueSize;
    }

    /**
     * @return Prepared transaction queue size.
     */
    public int txPrepareQueueSize() {
        return txPrepareQueueSize;
    }

    /**
     * @return Start version counts map size.
     */
    public int txStartVersionCountsSize() {
        return txStartVerCountsSize;
    }

    /**
     * @return Number of cached committed transaction IDs.
     */
    public int txCommittedVersionsSize() {
        return txCommittedVersionsSize;
    }

    /**
     * @return Number of cached rolled back transaction IDs.
     */
    public int txRolledbackVersionsSize() {
        return txRolledbackVersionsSize;
    }

    /**
     * @return DHT thread map size
     */
    public int txDhtThreadMapSize() {
        return txDhtThreadMapSize;
    }

    /**
     * @return Transaction DHT per-Xid map size.
     */
    public int txDhtXidMapSize() {
        return txDhtXidMapSize;
    }

    /**
     * @return Committed DHT transaction queue size.
     */
    public int txDhtCommitQueueSize() {
        return txDhtCommitQueueSize;
    }

    /**
     * @return Prepared DHT transaction queue size.
     */
    public int txDhtPrepareQueueSize() {
        return txDhtPrepareQueueSize;
    }

    /**
     * @return DHT start version counts map size.
     */
    public int txDhtStartVersionCountsSize() {
        return txDhtStartVerCountsSize;
    }

    /**
     * @return Number of cached committed DHT transaction IDs.
     */
    public int txDhtCommittedVersionsSize() {
        return txDhtCommittedVersionsSize;
    }

    /**
     * @return Number of cached rolled back DHT transaction IDs.
     */
    public int txDhtRolledbackVersionsSize() {
        return txDhtRolledbackVersionsSize;
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(VisorCacheMetrics.class, this);
    }
}
