/* @java.file.header */

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.gridgain.grid.kernal.visor.cmd.dto;

import org.gridgain.grid.cache.*;

import java.io.*;

/**
 * Default cache configuration data.
 */
public class VisorDefaultCacheConfig implements Serializable {
    /** */
    private static final long serialVersionUID = 0L;

    private final GridCacheTxIsolation dfltIsolation;
    private final GridCacheTxConcurrency dfltConcurrency;
    private final long dfltTxTimeout;
    private final long dfltLockTimeout;

    public VisorDefaultCacheConfig(GridCacheTxIsolation dfltIsolation,
        GridCacheTxConcurrency dfltConcurrency, long dfltTxTimeout, long dfltLockTimeout) {
        this.dfltIsolation = dfltIsolation;
        this.dfltConcurrency = dfltConcurrency;
        this.dfltTxTimeout = dfltTxTimeout;
        this.dfltLockTimeout = dfltLockTimeout;
    }

    /**
     * @return Default isolation.
     */
    public GridCacheTxIsolation defaultIsolation() {
        return dfltIsolation;
    }

    /**
     * @return Default concurrency.
     */
    public GridCacheTxConcurrency defaultConcurrency() {
        return dfltConcurrency;
    }

    /**
     * @return Default tx timeout.
     */
    public long defaultTxTimeout() {
        return dfltTxTimeout;
    }

    /**
     * @return Default lock timeout.
     */
    public long defaultLockTimeout() {
        return dfltLockTimeout;
    }
}
