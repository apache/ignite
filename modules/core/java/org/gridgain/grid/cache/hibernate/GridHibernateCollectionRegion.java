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
import org.hibernate.cache.spi.access.*;

/**
 * Implementation of {@link CollectionRegion}. This region is used to store collection data.
 * <p>
 * L2 cache for collection can be enabled in the Hibernate configuration file:
 * <pre name="code" class="xml">
 * &lt;hibernate-configuration&gt;
 *     &lt;!-- Enable L2 cache. --&gt;
 *     &lt;property name="cache.use_second_level_cache"&gt;true&lt;/property&gt;
 *
 *     &lt;!-- Use GridGain as L2 cache provider. --&gt;
 *     &lt;property name="cache.region.factory_class"&gt;org.gridgain.grid.cache.hibernate.GridHibernateRegionFactory&lt;/property&gt;
 *
 *     &lt;!-- Specify entities. --&gt;
 *     &lt;mapping class="com.example.Entity"/&gt;
 *     &lt;mapping class="com.example.ChildEntity"/&gt;
 *
 *     &lt;!-- Enable L2 cache with nonstrict-read-write access strategy for entities and collection. --&gt;
 *     &lt;collection-cache collection="com.example.Entity" usage="nonstrict-read-write"/&gt;
 *     &lt;collection-cache collection="com.example.ChildEntity" usage="nonstrict-read-write"/&gt;
 *     &lt;collection-cache collection="com.example.Entity.children" usage="nonstrict-read-write"/&gt;
 * &lt;/hibernate-configuration&gt;
 * </pre>
 * Also cache for collection can be enabled using annotations:
 * <pre name="code" class="java">
 * &#064;javax.persistence.Entity
 * public class Entity {
 *    ...
 *
 *    &#064;javax.persistence.OneToMany(cascade=CascadeType.ALL, fetch=FetchType.EAGER)
 *    &#064;javax.persistence.JoinColumn(name="PARENT_ID")
 *    &#064;org.hibernate.annotations.Cache(usage = CacheConcurrencyStrategy.NONSTRICT_READ_WRITE)
 *    public List&lt;ChildEntity&gt; getChildren() {...}
 * }
 * </pre>
 * Note: the collection cache does not cache the state of the actual entities in the cache, it caches only identifier
 * values. For this reason, the collection cache should always be used in conjunction with
 * the second-level cache for those entities expected to be cached as part of a collection cache.
 */
public class GridHibernateCollectionRegion extends GridHibernateTransactionalDataRegion implements CollectionRegion {
    /**
     * @param factory Region factory.
     * @param name Region name.
     * @param grid Grid.
     * @param cache Region cache.
     * @param dataDesc Region data description.
     */
    public GridHibernateCollectionRegion(GridHibernateRegionFactory factory, String name,
        Grid grid, GridCache<Object, Object> cache, CacheDataDescription dataDesc) {
        super(factory, name, grid, cache, dataDesc);
    }

    /** {@inheritDoc} */
    @Override public CollectionRegionAccessStrategy buildAccessStrategy(AccessType accessType) throws CacheException {
        return new AccessStrategy(createAccessStrategy(accessType));
    }

    /**
     * Collection region access strategy.
     */
    private class AccessStrategy extends GridHibernateAbstractRegionAccessStrategy
        implements CollectionRegionAccessStrategy {
        /**
         * @param stgy Access strategy implementation.
         */
        private AccessStrategy(GridHibernateAccessStrategyAdapter stgy) {
            super(stgy);
        }

        /** {@inheritDoc} */
        @Override public CollectionRegion getRegion() {
            return GridHibernateCollectionRegion.this;
        }
    }
}
