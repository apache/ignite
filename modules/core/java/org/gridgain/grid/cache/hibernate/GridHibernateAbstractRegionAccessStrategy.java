/* @java.file.header */

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.gridgain.grid.cache.hibernate;

import org.hibernate.cache.*;
import org.hibernate.cache.spi.access.*;
import org.jetbrains.annotations.*;

/**
 * Implementation of L2 cache access strategy delegating to {@link GridHibernateAccessStrategyAdapter}.
 */
public abstract class GridHibernateAbstractRegionAccessStrategy implements RegionAccessStrategy {
    /** */
    protected final GridHibernateAccessStrategyAdapter stgy;

    /**
     * @param stgy Access strategy implementation.
     */
    protected GridHibernateAbstractRegionAccessStrategy(GridHibernateAccessStrategyAdapter stgy) {
        this.stgy = stgy;
    }

    /** {@inheritDoc} */
    @Nullable @Override public Object get(Object key, long txTs) throws CacheException {
        return stgy.get(key);
    }

    /** {@inheritDoc} */
    @Override public boolean putFromLoad(Object key, Object val, long txTs, Object ver) throws CacheException {
        stgy.putFromLoad(key, val);

        return true;
    }

    /** {@inheritDoc} */
    @Override public boolean putFromLoad(Object key, Object val, long txTs, Object ver, boolean minimalPutOverride)
        throws CacheException {
        stgy.putFromLoad(key, val, minimalPutOverride);

        return true;
    }

    /** {@inheritDoc} */
    @Nullable @Override public SoftLock lockItem(Object key, Object ver) throws CacheException {
        return stgy.lock(key);
    }

    /** {@inheritDoc} */
    @Nullable @Override public SoftLock lockRegion() throws CacheException {
        return stgy.lockRegion();
    }

    /** {@inheritDoc} */
    @Override public void unlockRegion(SoftLock lock) throws CacheException {
        stgy.unlockRegion(lock);
    }

    /** {@inheritDoc} */
    @Override public void unlockItem(Object key, SoftLock lock) throws CacheException {
        stgy.unlock(key, lock);
    }

    /** {@inheritDoc} */
    @Override public void remove(Object key) throws CacheException {
        stgy.remove(key);
    }

    /** {@inheritDoc} */
    @Override public void removeAll() throws CacheException {
        stgy.removeAll();
    }

    /** {@inheritDoc} */
    @Override public void evict(Object key) throws CacheException {
        stgy.evict(key);
    }

    /** {@inheritDoc} */
    @Override public void evictAll() throws CacheException {
        stgy.evictAll();
    }
}
