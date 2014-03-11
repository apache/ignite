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
import org.hibernate.*;
import org.hibernate.cache.spi.*;

/**
 * Implementation of {@link QueryResultsRegion}. This region is used to store query results.
 * <p>
 * Query results caching can be enabled in the Hibernate configuration file:
 * <pre name="code" class="xml">
 * &lt;hibernate-configuration&gt;
 *     &lt;!-- Enable L2 cache. --&gt;
 *     &lt;property name="cache.use_second_level_cache"&gt;true&lt;/property&gt;
 *
 *     &lt;!-- Enable query cache. --&gt;
 *     &lt;property name="cache.use_second_level_cache"&gt;true&lt;/property&gt;

 *     &lt;!-- Use GridGain as L2 cache provider. --&gt;
 *     &lt;property name="cache.region.factory_class"&gt;org.gridgain.grid.cache.hibernate.GridHibernateRegionFactory&lt;/property&gt;
 *
 *     &lt;!-- Specify entity. --&gt;
 *     &lt;mapping class="com.example.Entity"/&gt;
 *
 *     &lt;!-- Enable L2 cache with nonstrict-read-write access strategy for entity. --&gt;
 *     &lt;class-cache class="com.example.Entity" usage="nonstrict-read-write"/&gt;
 * &lt;/hibernate-configuration&gt;
 * </pre>
 * By default queries are not cached even after enabling query caching, to enable results caching for a particular
 * query, call {@link Query#setCacheable(boolean)}:
 * <pre name="code" class="java">
 *     Session ses = getSession();
 *
 *     Query qry = ses.createQuery("...");
 *
 *     qry.setCacheable(true); // Enable L2 cache for query.
 * </pre>
 * Note: the query cache does not cache the state of the actual entities in the cache, it caches only identifier
 * values. For this reason, the query cache should always be used in conjunction with
 * the second-level cache for those entities expected to be cached as part of a query result cache
 */
public class GridHibernateQueryResultsRegion extends GridHibernateGeneralDataRegion implements QueryResultsRegion {
    /**
     * @param factory Region factory.
     * @param name Region name.
     * @param grid Grid.
     * @param cache Region cache.
     */
    public GridHibernateQueryResultsRegion(GridHibernateRegionFactory factory, String name,
        Grid grid, GridCache<Object, Object> cache) {
        super(factory, name, grid, cache);
    }
}
