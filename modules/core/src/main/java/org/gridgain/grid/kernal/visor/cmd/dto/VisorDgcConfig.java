/* @java.file.header */

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.gridgain.grid.kernal.visor.cmd.dto;

import java.io.*;

/**
 * DGC configuration data.
 */
public class VisorDgcConfig implements Serializable {
    /** */
    private static final long serialVersionUID = 0L;

    private final long freq;
    private final boolean rmvLocks;
    private final long suspectLockTimeout;

    public VisorDgcConfig(long freq, boolean rmvLocks, long suspectLockTimeout) {
        this.freq = freq;
        this.rmvLocks = rmvLocks;
        this.suspectLockTimeout = suspectLockTimeout;
    }

    /**
     * @return Frequency.
     */
    public long frequency() {
        return freq;
    }

    /**
     * @return Removed locks.
     */
    public boolean removedLocks() {
        return rmvLocks;
    }

    /**
     * @return Suspect lock timeout.
     */
    public long suspectLockTimeout() {
        return suspectLockTimeout;
    }
}
