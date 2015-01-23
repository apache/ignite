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

package org.gridgain.grid.kernal.visor.cache;

import org.gridgain.grid.cache.*;
import org.gridgain.grid.util.typedef.internal.*;

import java.io.*;

/**
 * Data transfer object for {@link org.gridgain.grid.cache.CacheMetrics}.
 */
public class VisorCacheMetrics implements Serializable {
    /** */
    private static final int MICROSECONDS_IN_SECOND = 1_000_000;

    /** */
    private static final long serialVersionUID = 0L;

    /** Gets the number of all entries cached on this node. */
    private int size;

    /** Create time of the owning entity (either cache or entry). */
    private long createTm;

    /** Last write time of the owning entity (either cache or entry). */
    private long writeTm;

    /** Last read time of the owning entity (either cache or entry). */
    private long readTm;

    /** Last time transaction was committed. */
    private long commitTm;

    /** Last time transaction was rollback. */
    private long rollbackTm;

    /** Total number of reads of the owning entity (either cache or entry). */
    private int reads;

    /** Total number of writes of the owning entity (either cache or entry). */
    private int writes;

    /** Total number of hits for the owning entity (either cache or entry). */
    private int hits;

    /** Total number of misses for the owning entity (either cache or entry). */
    private int misses;

    /** Total number of transaction commits. */
    private int txCommits;

    /** Total number of transaction rollbacks. */
    private int txRollbacks;

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

        cm.size = c.size();

        cm.reads = (int)m.getCacheGets();
        cm.writes = (int)(m.getCachePuts() + m.getCacheRemovals());
        cm.hits = (int)m.getCacheHits();
        cm.misses = (int)m.getCacheMisses();

        cm.txCommits = (int)m.getCacheTxCommits();
        cm.txRollbacks = (int)m.getCacheTxRollbacks();

        cm.readsPerSec = (int)(MICROSECONDS_IN_SECOND * 1.f / m.getAverageGetTime());
        cm.writesPerSec = (int)(MICROSECONDS_IN_SECOND * 1.f / m.getAveragePutTime());
        cm.hitsPerSec = -1;
        cm.missesPerSec = (int)(MICROSECONDS_IN_SECOND * 1.f / m.getAverageRemoveTime());
        cm.commitsPerSec = (int)(MICROSECONDS_IN_SECOND * 1.f / m.getAverageTxCommitTime());
        cm.rollbacksPerSec = (int)(MICROSECONDS_IN_SECOND * 1.f / m.getAverageTxRollbackTime());

        cm.qryMetrics = VisorCacheQueryMetrics.from(c.queries().metrics());

        return cm;
    }

    /**
     * @return Create time of the owning entity (either cache or entry).
     */
    public long createTime() {
        return createTm;
    }

    /**
     * @return Last write time of the owning entity (either cache or entry).
     */
    public long writeTime() {
        return writeTm;
    }

    /**
     * @return Last read time of the owning entity (either cache or entry).
     */
    public long readTime() {
        return readTm;
    }

    /**
     * @return Last time transaction was committed.
     */
    public long commitTime() {
        return commitTm;
    }

    /**
     * @return Last time transaction was rollback.
     */
    public long rollbackTime() {
        return rollbackTm;
    }

    /**
     * @return Total number of reads of the owning entity (either cache or entry).
     */
    public int reads() {
        return reads;
    }

    /**
     * @return Total number of writes of the owning entity (either cache or entry).
     */
    public int writes() {
        return writes;
    }

    /**
     * @return Total number of hits for the owning entity (either cache or entry).
     */
    public int hits() {
        return hits;
    }

    /**
     * @return Total number of misses for the owning entity (either cache or entry).
     */
    public int misses() {
        return misses;
    }

    /**
     * @return Total number of transaction commits.
     */
    public int txCommits() {
        return txCommits;
    }

    /**
     * @return Total number of transaction rollbacks.
     */
    public int txRollbacks() {
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
     * @return Gets the number of all entries cached on this node.
     */
    public int size() {
        return size;
    }

    /**
     * @return Gets query metrics for cache.
     */
    public VisorCacheQueryMetrics queryMetrics() {
        return qryMetrics;
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(VisorCacheMetrics.class, this);
    }
}
