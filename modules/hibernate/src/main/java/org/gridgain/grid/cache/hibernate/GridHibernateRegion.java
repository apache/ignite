/* @java.file.header */

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.gridgain.grid.cache.hibernate;

import org.gridgain.grid.*;
import org.gridgain.grid.cache.*;
import org.hibernate.cache.*;
import org.hibernate.cache.spi.*;

import java.util.*;

/**
 * Implementation of {@link Region}. This interface defines base contract for all L2 cache regions.
 */
public class GridHibernateRegion implements Region {
    /** */
    protected final GridHibernateRegionFactory factory;

    /** */
    private final String name;

    /** Cache instance. */
    protected final GridCache<Object, Object> cache;

    /** Grid instance. */
    protected Ignite ignite;

    /**
     * @param factory Region factory.
     * @param name Region name.
     * @param ignite Grid.
     * @param cache Region cache.
     */
    public GridHibernateRegion(GridHibernateRegionFactory factory, String name, Ignite ignite,
        GridCache<Object, Object> cache) {
        this.factory = factory;
        this.name = name;
        this.ignite = ignite;
        this.cache = cache;
    }

    /** {@inheritDoc} */
    @Override public String getName() {
        return name;
    }

    /** {@inheritDoc} */
    @Override public void destroy() throws CacheException {
        // No-op.
    }

    /** {@inheritDoc} */
    @Override public boolean contains(Object key) {
        return cache.containsKey(key);
    }

    /** {@inheritDoc} */
    @Override public long getSizeInMemory() {
        return -1;
    }

    /** {@inheritDoc} */
    @Override public long getElementCountInMemory() {
        return cache.size();
    }

    /** {@inheritDoc} */
    @Override public long getElementCountOnDisk() {
        return -1;
    }

    /** {@inheritDoc} */
    @Override public Map toMap() {
        return cache.toMap();
    }

    /** {@inheritDoc} */
    @Override public long nextTimestamp() {
        return System.currentTimeMillis();
    }

    /** {@inheritDoc} */
    @Override public int getTimeout() {
        return 0;
    }
}
