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
 * Data transfer object for DGC configuration properties.
 */
public class VisorDgcConfig implements Serializable {
    /** */
    private static final long serialVersionUID = 0L;

    /** DGC check frequency. */
    private final long freq;

    /** DGC remove locks flag. */
    private final boolean rmvLocks;

    /** Timeout for considering lock to be suspicious. */
    private final long suspectLockTimeout;

    /** Create data transfer object with given parameters.  */
    public VisorDgcConfig(long freq, boolean rmvLocks, long suspectLockTimeout) {
        this.freq = freq;
        this.rmvLocks = rmvLocks;
        this.suspectLockTimeout = suspectLockTimeout;
    }

    /**
     * @return DGC check frequency.
     */
    public long frequency() {
        return freq;
    }

    /**
     * @return DGC remove locks flag.
     */
    public boolean removedLocks() {
        return rmvLocks;
    }

    /**
     * @return Timeout for considering lock to be suspicious.
     */
    public long suspectLockTimeout() {
        return suspectLockTimeout;
    }
}
