/* @java.file.header */

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.gridgain.grid.kernal.visor.cmd.dto.cache;

import org.jetbrains.annotations.*;

import java.io.*;

/**
 * Data transfer object for eviction configuration properties.
 */
public class VisorEvictionConfig implements Serializable {
    /** */
    private static final long serialVersionUID = 0L;

    /** Eviction policy. */
    private final String plc;

    /** Cache eviction policy max size. */
    private final Integer plcMaxSize;

    /** Eviction filter to specify which entries should not be evicted. */
    private final String filter;

    /** Synchronous eviction concurrency level. */
    private final int syncConcurrencyLvl;

    /** Synchronous eviction timeout. */
    private final long syncTimeout;

    /** Synchronized key buffer size. */
    private final int syncKeyBufSize;

    /** Synchronous evicts flag. */
    private final boolean evictSynchronized;

    /** Synchronous near evicts flag. */
    private final boolean nearSynchronized;

    /** Eviction max overflow ratio. */
    private final float maxOverflowRatio;

    public VisorEvictionConfig(
        @Nullable String plc,
        @Nullable Integer plcMaxSize,
        @Nullable String filter,
        int syncConcurrencyLvl,
        long syncTimeout,
        int syncKeyBufSize,
        boolean evictSynchronized,
        boolean nearSynchronized,
        float maxOverflowRatio) {
        this.plc = plc;
        this.plcMaxSize = plcMaxSize;
        this.filter = filter;
        this.syncConcurrencyLvl = syncConcurrencyLvl;
        this.syncTimeout = syncTimeout;
        this.syncKeyBufSize = syncKeyBufSize;
        this.evictSynchronized = evictSynchronized;
        this.nearSynchronized = nearSynchronized;
        this.maxOverflowRatio = maxOverflowRatio;
    }

    /**
     * @return Eviction policy.
     */
    @Nullable public String policy() {
        return plc;
    }

    /**
     * @return Cache eviction policy max size.
     */
    @Nullable public Integer policyMaxSize() {
        return plcMaxSize;
    }

    /**
     * @return Eviction filter to specify which entries should not be evicted.
     */
    @Nullable public String filter() {
        return filter;
    }

    /**
     * @return Synchronous eviction timeout.
     */
    public long synchronizedTimeout() {
        return syncTimeout;
    }

    /**
     * @return Synchronized key buffer size.
     */
    public int synchronizedKeyBufferSize() {
        return syncKeyBufSize;
    }

    /**
     * @return Synchronous evicts flag.
     */
    public boolean evictSynchronized() {
        return evictSynchronized;
    }

    /**
     * @return Synchronous near evicts flag.
     */
    public boolean nearSynchronized() {
        return nearSynchronized;
    }

    /**
     * @return Eviction max overflow ratio.
     */
    public float maxOverflowRatio() {
        return maxOverflowRatio;
    }

    /**
     * @return Synchronous eviction concurrency level.
     */
    public int syncConcurrencyLvl() {
        return syncConcurrencyLvl;
    }
}
