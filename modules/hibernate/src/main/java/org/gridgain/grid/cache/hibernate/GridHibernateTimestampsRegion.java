/* @java.file.header */

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.gridgain.grid.cache.hibernate;

import org.apache.ignite.*;
import org.gridgain.grid.cache.*;
import org.hibernate.cache.spi.*;

/**
 * Implementation of {@link TimestampsRegion}. This region is automatically created when query
 * caching is enabled and it holds most recent updates timestamps to queryable tables.
 * Name of timestamps region is {@code "org.hibernate.cache.spi.UpdateTimestampsCache"}.
 */
public class GridHibernateTimestampsRegion extends GridHibernateGeneralDataRegion implements TimestampsRegion {
    /**
     * @param factory Region factory.
     * @param name Region name.
     * @param ignite Grid.
     * @param cache Region cache.
     */
    public GridHibernateTimestampsRegion(GridHibernateRegionFactory factory, String name,
        Ignite ignite, GridCache<Object, Object> cache) {
        super(factory, name, ignite, cache);
    }
}
