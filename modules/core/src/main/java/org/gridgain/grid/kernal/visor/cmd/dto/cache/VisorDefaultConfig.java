/* @java.file.header */

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.gridgain.grid.kernal.visor.cmd.dto.cache;

import org.gridgain.grid.cache.*;

import java.io.*;

/**
 * Default cache configuration data.
 */
public class VisorDefaultConfig implements Serializable {
    /** */
    private static final long serialVersionUID = 0L;

    private final GridCacheTxIsolation txIsolation;
    private final GridCacheTxConcurrency txConcurrency;
    private final long txTimeout;
    private final long txLockTimeout;
    private final long queryTimeout;

    public VisorDefaultConfig(GridCacheTxIsolation txIsolation,
        GridCacheTxConcurrency txConcurrency, long txTimeout, long txLockTimeout, long queryTimeout) {
        this.txIsolation = txIsolation;
        this.txConcurrency = txConcurrency;
        this.txTimeout = txTimeout;
        this.txLockTimeout = txLockTimeout;
        this.queryTimeout = queryTimeout;
    }

    /**
     * @return Default isolation.
     */
    public GridCacheTxIsolation txIsolation() {
        return txIsolation;
    }

    /**
     * @return Default concurrency.
     */
    public GridCacheTxConcurrency txConcurrency() {
        return txConcurrency;
    }

    /**
     * @return Default tx timeout.
     */
    public long txTimeout() {
        return txTimeout;
    }

    /**
     * @return Default lock timeout.
     */
    public long txLockTimeout() {
        return txLockTimeout;
    }

    /**
     * @return Query timeout.
     */
    public long queryTimeout() {
        return queryTimeout;
    }
}
