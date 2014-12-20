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
import org.gridgain.grid.util.typedef.*;
import org.hibernate.cache.*;
import org.hibernate.cache.spi.*;
import org.hibernate.cache.spi.RegionFactory;
import org.hibernate.cache.spi.access.AccessType;
import org.hibernate.cfg.*;

import java.util.*;

import static org.hibernate.cache.spi.access.AccessType.*;

/**
 * Hibernate L2 cache region factory.
 * <p>
 * Following Hibernate settings should be specified to enable second level cache and to use this
 * region factory for caching:
 * <pre name="code" class="brush: xml; gutter: false;">
 * hibernate.cache.use_second_level_cache=true
 * hibernate.cache.region.factory_class=org.gridgain.grid.cache.hibernate.GridHibernateRegionFactory
 * </pre>
 * Note that before region factory is started you need to start properly configured GridGain node in the same JVM.
 * For example to start GridGain node one of loader provided in {@code org.gridgain.grid.startup} package can be used.
 * <p>
 * Name of grid to be used for region factory must be specified as following Hibernate property:
 * <pre name="code" class="brush: xml; gutter: false;">
 * org.gridgain.hibernate.grid_name=&lt;grid name&gt;
 * </pre>
 * Each Hibernate cache region must be associated with some {@link GridCache}, by default it is assumed that
 * for each cache region there is a {@link GridCache} with the same name. Also it is possible to define
 * region to cache mapping using properties with prefix {@code org.gridgain.hibernate.region_cache}.
 * For example if for region with name "region1" cache with name "cache1" should be used then following
 * Hibernate property should be specified:
 * <pre name="code" class="brush: xml; gutter: false;">
 * org.gridgain.hibernate.region_cache.region1=cache1
 * </pre>
 */
public class GridHibernateRegionFactory implements RegionFactory {
    /** */
    private static final long serialVersionUID = 0L;

    /** Hibernate L2 cache grid name property name. */
    public static final String GRID_NAME_PROPERTY = "org.gridgain.hibernate.grid_name";

    /** Default cache property name. */
    public static final String DFLT_CACHE_NAME_PROPERTY = "org.gridgain.hibernate.default_cache";

    /** Property prefix used to specify region name to cache name mapping. */
    public static final String REGION_CACHE_PROPERTY = "org.gridgain.hibernate.region_cache.";

    /** */
    public static final String DFLT_ACCESS_TYPE_PROPERTY = "org.gridgain.hibernate.default_access_type";

    /** */
    public static final String GRID_CONFIG_PROPERTY = "org.gridgain.hibernate.grid_config";

    /** Grid providing caches. */
    private Ignite ignite;

    /** Default cache. */
    private GridCache<Object, Object> dfltCache;

    /** Default region access type. */
    private AccessType dfltAccessType;

    /** Region name to cache name mapping. */
    private final Map<String, String> regionCaches = new HashMap<>();

    /** Map needed to provide the same transaction context for different regions. */
    private final ThreadLocal threadLoc = new ThreadLocal();

    /** {@inheritDoc} */
    @Override public void start(Settings settings, Properties props) throws CacheException {
        String gridName = props.getProperty(GRID_NAME_PROPERTY);

        if (gridName != null)
            ignite = G.ignite(gridName);
        else {
            String gridCfg = props.getProperty(GRID_CONFIG_PROPERTY);

            if (gridCfg == null)
                throw new CacheException("Either grid name or path to grid configuration must be specified.");

            try {
                ignite = G.start(gridCfg);
            }
            catch (IgniteCheckedException e) {
                throw new CacheException(e);
            }
        }

        if (ignite == null)
            throw new CacheException("Grid '" + gridName + "' for hibernate L2 cache is not started.");

        String accessType = props.getProperty(DFLT_ACCESS_TYPE_PROPERTY, NONSTRICT_READ_WRITE.name());

        dfltAccessType = AccessType.valueOf(accessType);

        for (Map.Entry<Object, Object> prop : props.entrySet()) {
            String key = prop.getKey().toString();

            if (key.startsWith(REGION_CACHE_PROPERTY)) {
                String regionName = key.substring(REGION_CACHE_PROPERTY.length());

                String cacheName = prop.getValue().toString();

                if (ignite.cache(cacheName) == null)
                    throw new CacheException("Cache '" + cacheName + "' specified for region '" + regionName + "' " +
                        "is not configured.");

                regionCaches.put(regionName, cacheName);
            }
        }

        String dfltCacheName = props.getProperty(DFLT_CACHE_NAME_PROPERTY);

        if (dfltCacheName != null) {
            dfltCache = ignite.cache(dfltCacheName);

            if (dfltCache == null)
                throw new CacheException("Cache specified as default is not configured: " + dfltCacheName);
        }

        IgniteLogger log = ignite.log().getLogger(GridHibernateRegionFactory.class);

        if (log.isDebugEnabled())
            log.debug("GridHibernateRegionFactory started [grid=" + gridName + ']');
    }

    /** {@inheritDoc} */
    @Override public void stop() {
    }

    /** {@inheritDoc} */
    @Override public boolean isMinimalPutsEnabledByDefault() {
        return false;
    }

    /** {@inheritDoc} */
    @Override public AccessType getDefaultAccessType() {
        return dfltAccessType;
    }

    /** {@inheritDoc} */
    @Override public long nextTimestamp() {
        return System.currentTimeMillis();
    }

    /** {@inheritDoc} */
    @Override public EntityRegion buildEntityRegion(String regionName, Properties props, CacheDataDescription metadata)
        throws CacheException {
        return new GridHibernateEntityRegion(this, regionName, ignite, regionCache(regionName), metadata);
    }

    /** {@inheritDoc} */
    @Override public NaturalIdRegion buildNaturalIdRegion(String regionName, Properties props,
        CacheDataDescription metadata) throws CacheException {
        return new GridHibernateNaturalIdRegion(this, regionName, ignite, regionCache(regionName), metadata);
    }

    /** {@inheritDoc} */
    @Override public CollectionRegion buildCollectionRegion(String regionName, Properties props,
        CacheDataDescription metadata) throws CacheException {
        return new GridHibernateCollectionRegion(this, regionName, ignite, regionCache(regionName), metadata);
    }

    /** {@inheritDoc} */
    @Override public QueryResultsRegion buildQueryResultsRegion(String regionName, Properties props)
        throws CacheException {
        return new GridHibernateQueryResultsRegion(this, regionName, ignite, regionCache(regionName));
    }

    /** {@inheritDoc} */
    @Override public TimestampsRegion buildTimestampsRegion(String regionName, Properties props) throws CacheException {
        return new GridHibernateTimestampsRegion(this, regionName, ignite, regionCache(regionName));
    }

    /**
     * Reuse same thread local for the same cache across different regions.
     *
     * @param cacheName Cache name.
     * @return Thread local instance used to track updates done during one Hibernate transaction.
     */
    ThreadLocal threadLocalForCache(String cacheName) {
        return threadLoc;
    }

    /**
     * @param regionName L2 cache region name.
     * @return Cache for given region.
     * @throws CacheException If cache for given region is not configured.
     */
    private GridCache<Object, Object> regionCache(String regionName) throws CacheException {
        String cacheName = regionCaches.get(regionName);

        if (cacheName == null) {
            if (dfltCache != null)
                return dfltCache;

            cacheName = regionName;
        }

        GridCache<Object, Object> cache = ignite.cache(cacheName);

        if (cache == null)
            throw new CacheException("Cache '" + cacheName + "' for region '" + regionName + "' is not configured.");

        return cache;
    }
}
