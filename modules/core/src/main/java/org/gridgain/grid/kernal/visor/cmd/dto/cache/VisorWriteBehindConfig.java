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

    private final boolean enabled;
    private final int batchSize;
    private final long flushFrequency;
    private final int flushSize;
    private final int flushThreadCnt;

    public VisorWriteBehindConfig(boolean enabled, int batchSize, long flushFrequency, int flushSize,
        int flushThreadCnt) {
        this.enabled = enabled;
        this.batchSize = batchSize;
        this.flushFrequency = flushFrequency;
        this.flushSize = flushSize;
        this.flushThreadCnt = flushThreadCnt;
    }

    /**
     * @return Enabled.
     */
    public boolean enabled() {
        return enabled;
    }

    /**
     * @return Batch size.
     */
    public int batchSize() {
        return batchSize;
    }

    /**
     * @return Flush frequency.
     */
    public long flushFrequency() {
        return flushFrequency;
    }

    /**
     * @return Flush size.
     */
    public int flushSize() {
        return flushSize;
    }

    /**
     * @return Flush thread count.
     */
    public int flushThreadCount() {
        return flushThreadCnt;
    }
}
