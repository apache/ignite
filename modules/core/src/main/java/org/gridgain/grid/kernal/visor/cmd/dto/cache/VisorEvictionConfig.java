/* @java.file.header */

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.gridgain.grid.kernal.visor.cmd.dto.cache;

import java.io.*;

/**
 * Eviction configuration data.
 */
public class VisorEvictionConfig implements Serializable {
    /** */
    private static final long serialVersionUID = 0L;

    private final String plc;
    private final Integer plcMaxSize;
    private final String filter;
    private final int syncConcurrencyLvl;
    private final long syncTimeout;
    private final int syncKeyBufSize;
    private final boolean evictSynchronized;
    private final boolean nearSynchronized;
    private final float maxOverflowRatio;

    public VisorEvictionConfig(String plc, Integer plcMaxSize, String filter, int syncConcurrencyLvl,
        long syncTimeout,
        int syncKeyBufSize, boolean evictSynchronized, boolean nearSynchronized, float maxOverflowRatio) {
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
     * @return Policy.
     */
    public String policy() {
        return plc;
    }

    /**
     * @return Policy max size.
     */
    public Integer policyMaxSize() {
        return plcMaxSize;
    }

    /**
     * @return Filter.
     */
    public String filter() {
        return filter;
    }

    /**
     * @return Synchronized Timeout.
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
     * @return Evict synchronized.
     */
    public boolean evictSynchronized() {
        return evictSynchronized;
    }

    /**
     * @return Near synchronized.
     */
    public boolean nearSynchronized() {
        return nearSynchronized;
    }

    /**
     * @return Max overflow ratio.
     */
    public float maxOverflowRatio() {
        return maxOverflowRatio;
    }

    /**
     * @return Sync concurrency lvl.
     */
    public int syncConcurrencyLvl() {
        return syncConcurrencyLvl;
    }
}
