/* @java.file.header */

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.gridgain.grid.cache.hibernate;

import org.apache.ignite.*;
import org.gridgain.grid.*;
import org.gridgain.grid.cache.*;
import org.hibernate.cache.*;
import org.hibernate.cache.spi.*;
import org.jetbrains.annotations.*;

/**
 * Implementation of {@link GeneralDataRegion}. This interface defines common contract for {@link QueryResultsRegion}
 * and {@link TimestampsRegion}.
 */
public class GridHibernateGeneralDataRegion extends GridHibernateRegion implements GeneralDataRegion {
    /**
     * @param factory Region factory.
     * @param name Region name.
     * @param ignite Grid.
     * @param cache Region cache.
     */
    public GridHibernateGeneralDataRegion(GridHibernateRegionFactory factory, String name,
        Ignite ignite, GridCache<Object, Object> cache) {
        super(factory, name, ignite, cache);
    }

    /** {@inheritDoc} */
    @Nullable @Override public Object get(Object key) throws CacheException {
        try {
            return cache.get(key);
        } catch (GridException e) {
            throw new CacheException(e);
        }
    }

    /** {@inheritDoc} */
    @Override public void put(Object key, Object val) throws CacheException {
        try {
            cache.putx(key, val);
        } catch (GridException e) {
            throw new CacheException(e);
        }
    }

    /** {@inheritDoc} */
    @Override public void evict(Object key) throws CacheException {
        GridHibernateAccessStrategyAdapter.evict(ignite, cache, key);
    }

    /** {@inheritDoc} */
    @Override public void evictAll() throws CacheException {
        GridHibernateAccessStrategyAdapter.evictAll(cache);
    }
}
