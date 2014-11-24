/* @java.file.header */

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.gridgain.grid.kernal.visor.dto.cache;

import org.gridgain.grid.cache.*;
import org.gridgain.grid.util.typedef.internal.*;

import java.io.*;

/**
 * Data transfer object for DGC configuration properties.
 */
public class VisorCacheDgcConfig implements Serializable {
    /** */
    private static final long serialVersionUID = 0L;

    /** DGC check frequency. */
    private long freq;

    /** DGC remove locks flag. */
    private boolean rmvLocks;

    /** Timeout for considering lock to be suspicious. */
    private long suspectLockTimeout;

    /**
     * @param ccfg Cache configuration.
     * @return Data transfer object for DGC configuration properties.
     */
    public static VisorCacheDgcConfig from(GridCacheConfiguration ccfg) {
        VisorCacheDgcConfig cfg = new VisorCacheDgcConfig();

        cfg.frequency(ccfg.getDgcFrequency());
        cfg.removedLocks(ccfg.isDgcRemoveLocks());
        cfg.suspectLockTimeout(ccfg.getDgcSuspectLockTimeout());

        return cfg;
    }

    /**
     * @return DGC check frequency.
     */
    public long frequency() {
        return freq;
    }

    /**
     * @param freq New dGC check frequency.
     */
    public void frequency(long freq) {
        this.freq = freq;
    }

    /**
     * @return DGC remove locks flag.
     */
    public boolean removedLocks() {
        return rmvLocks;
    }

    /**
     * @param rmvLocks New dGC remove locks flag.
     */
    public void removedLocks(boolean rmvLocks) {
        this.rmvLocks = rmvLocks;
    }

    /**
     * @return Timeout for considering lock to be suspicious.
     */
    public long suspectLockTimeout() {
        return suspectLockTimeout;
    }

    /**
     * @param suspectLockTimeout New timeout for considering lock to be suspicious.
     */
    public void suspectLockTimeout(long suspectLockTimeout) {
        this.suspectLockTimeout = suspectLockTimeout;
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(VisorCacheDgcConfig.class, this);
    }
}
