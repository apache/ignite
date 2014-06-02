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
 * Write-behind cache configuration data.
 */
public class VisorWriteBehindConfig implements Serializable {
    /** */
    private static final long serialVersionUID = 0L;

    /** Flag indicating whether write-behind behaviour should be used for the cache store. */
    private final boolean enabled;

    /** Maximum batch size for write-behind cache store operations. */
    private final int batchSize;

    /** Frequency with which write-behind cache is flushed to the cache store in milliseconds. */
    private final long flushFrequency;

    /** Maximum object count in write-behind cache. */
    private final int flushSize;

    /** Number of threads that will perform cache flushing. */
    private final int flushThreadCnt;

    public VisorWriteBehindConfig(boolean enabled, int batchSize, long flushFrequency, int flushSize, int flushThreadCnt) {
        this.enabled = enabled;
        this.batchSize = batchSize;
        this.flushFrequency = flushFrequency;
        this.flushSize = flushSize;
        this.flushThreadCnt = flushThreadCnt;
    }

    /**
     * @return Flag indicating whether write-behind behaviour should be used for the cache store.
     */
    public boolean enabled() {
        return enabled;
    }

    /**
     * @return Maximum batch size for write-behind cache store operations.
     */
    public int batchSize() {
        return batchSize;
    }

    /**
     * @return Frequency with which write-behind cache is flushed to the cache store in milliseconds.
     */
    public long flushFrequency() {
        return flushFrequency;
    }

    /**
     * @return Maximum object count in write-behind cache.
     */
    public int flushSize() {
        return flushSize;
    }

    /**
     * @return Number of threads that will perform cache flushing.
     */
    public int flushThreadCount() {
        return flushThreadCnt;
    }
}
