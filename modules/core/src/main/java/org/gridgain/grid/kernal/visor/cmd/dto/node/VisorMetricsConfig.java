/* @java.file.header */

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.gridgain.grid.kernal.visor.cmd.dto.node;

import java.io.*;

/**
 * Metrics configuration data.
 */
public class VisorMetricsConfig implements Serializable {
    /** */
    private static final long serialVersionUID = 0L;

    /** Metrics expire time. */
    private final long expTime;

    /** Number of node metrics stored in memory. */
    private final int historySize;

    /** Frequency of metrics log printout. */
    private final long logFreq;

    public VisorMetricsConfig(long expTime, int historySize, long logFreq) {
        this.expTime = expTime;
        this.historySize = historySize;
        this.logFreq = logFreq;
    }

    /**
     * @return Metrics expire time.
     */
    public long expireTime() {
        return expTime;
    }

    /**
     * @return Number of node metrics stored in memory.
     */
    public int historySize() {
        return historySize;
    }

    /**
     * @return Frequency of metrics log printout.
     */
    public long loggerFrequency() {
        return logFreq;
    }
}
