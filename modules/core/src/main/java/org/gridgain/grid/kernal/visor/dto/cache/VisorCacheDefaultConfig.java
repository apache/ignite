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
 * Data transfer object for default cache configuration properties.
 */
public class VisorCacheDefaultConfig implements Serializable {
    /** */
    private static final long serialVersionUID = 0L;

    /** Default transaction isolation. */
    private GridCacheTxIsolation txIsolation;

    /** Default transaction concurrency. */
    private GridCacheTxConcurrency txConcurrency;

    /** TTL value. */
    private long ttl;

    /** Default transaction concurrency. */
    private long txTimeout;

    /** Default transaction timeout. */
    private long txLockTimeout;

    /** Default query timeout. */
    private long queryTimeout;

    /**
     * @param ccfg Cache configuration.
     * @return Data transfer object for default cache configuration properties.
     */
    public static VisorCacheDefaultConfig from(GridCacheConfiguration ccfg) {
        VisorCacheDefaultConfig cfg = new VisorCacheDefaultConfig();

        cfg.txIsolation(ccfg.getDefaultTxIsolation());
        cfg.txConcurrency(ccfg.getDefaultTxConcurrency());
        cfg.timeToLive(ccfg.getDefaultTimeToLive());
        cfg.txTimeout(ccfg.getDefaultTxTimeout());
        cfg.txLockTimeout(ccfg.getDefaultLockTimeout());
        cfg.queryTimeout(ccfg.getDefaultQueryTimeout());

        return cfg;
    }

    /**
     * @return Default transaction isolation.
     */
    public GridCacheTxIsolation txIsolation() {
        return txIsolation;
    }

    /**
     * @param txIsolation New default transaction isolation.
     */
    public void txIsolation(GridCacheTxIsolation txIsolation) {
        this.txIsolation = txIsolation;
    }

    /**
     * @return Default transaction concurrency.
     */
    public GridCacheTxConcurrency txConcurrency() {
        return txConcurrency;
    }

    /**
     * @param txConcurrency New default transaction concurrency.
     */
    public void txConcurrency(GridCacheTxConcurrency txConcurrency) {
        this.txConcurrency = txConcurrency;
    }

    /**
     * @return TTL value.
     */
    public long timeToLive() {
        return ttl;
    }

    /**
     * @param ttl New tTL value.
     */
    public void timeToLive(long ttl) {
        this.ttl = ttl;
    }

    /**
     * @return Default transaction concurrency.
     */
    public long txTimeout() {
        return txTimeout;
    }

    /**
     * @param txTimeout New default transaction concurrency.
     */
    public void txTimeout(long txTimeout) {
        this.txTimeout = txTimeout;
    }

    /**
     * @return Default transaction timeout.
     */
    public long txLockTimeout() {
        return txLockTimeout;
    }

    /**
     * @param txLockTimeout New default transaction timeout.
     */
    public void txLockTimeout(long txLockTimeout) {
        this.txLockTimeout = txLockTimeout;
    }

    /**
     * @return Default query timeout.
     */
    public long queryTimeout() {
        return queryTimeout;
    }

    /**
     * @param qryTimeout New default query timeout.
     */
    public void queryTimeout(long qryTimeout) {
        queryTimeout = qryTimeout;
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(VisorCacheDefaultConfig.class, this);
    }
}
