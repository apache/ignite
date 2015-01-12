// @java.file.header

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.apache.ignite.cache;

import org.gridgain.grid.cache.GridCacheConfiguration;

import javax.cache.management.*;

/**
 * Implementation of {@link CacheMXBean}.
 *
 * It's a simple wrapper around {@link GridCacheConfiguration} for readonly
 * access to cache configuration.
 */
public class CacheConfigurationMXBean implements CacheMXBean {
    /**
     *
     */
    private final GridCacheConfiguration cacheCfg;

    /**
     * Constructor.
     *
     * @param cacheCfg The cache configuration.
     */
    public CacheConfigurationMXBean(GridCacheConfiguration cacheCfg) {
        this.cacheCfg = cacheCfg;
    }

    /** {@inheritDoc} */
    @Override public String getKeyType() {
        return cacheCfg.getKeyType().getName();
    }

    /** {@inheritDoc} */
    @Override public String getValueType() {
        return cacheCfg.getValueType().getName();
    }

    /** {@inheritDoc} */
    @Override public boolean isReadThrough() {
        return cacheCfg.isReadThrough();
    }

    /** {@inheritDoc} */
    @Override public boolean isWriteThrough() {
        return cacheCfg.isWriteThrough();
    }

    /** {@inheritDoc} */
    @Override public boolean isStoreByValue() {
        return cacheCfg.isStoreByValue();
    }

    /** {@inheritDoc} */
    @Override public boolean isStatisticsEnabled() {
        return cacheCfg.isStatisticsEnabled();
    }

    /** {@inheritDoc} */
    @Override public boolean isManagementEnabled() {
        return cacheCfg.isManagementEnabled();
    }
}
