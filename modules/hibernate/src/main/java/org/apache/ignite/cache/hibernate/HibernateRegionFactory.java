/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.ignite.cache.hibernate;

import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteException;
import org.apache.ignite.IgniteLogger;
import org.apache.ignite.Ignition;
import org.apache.ignite.internal.IgniteKernal;
import org.apache.ignite.internal.processors.cache.IgniteInternalCache;
import org.apache.ignite.internal.util.typedef.G;
import org.hibernate.cache.CacheException;
import org.hibernate.cache.spi.CacheDataDescription;
import org.hibernate.cache.spi.CollectionRegion;
import org.hibernate.cache.spi.EntityRegion;
import org.hibernate.cache.spi.NaturalIdRegion;
import org.hibernate.cache.spi.QueryResultsRegion;
import org.hibernate.cache.spi.RegionFactory;
import org.hibernate.cache.spi.TimestampsRegion;
import org.hibernate.cache.spi.access.AccessType;
import org.hibernate.cfg.Settings;

import static org.hibernate.cache.spi.access.AccessType.NONSTRICT_READ_WRITE;

/**
 * Hibernate L2 cache region factory.
 * <p>
 * Following Hibernate settings should be specified to enable second level cache and to use this
 * region factory for caching:
 * <pre name="code" class="brush: xml; gutter: false;">
 * hibernate.cache.use_second_level_cache=true
 * hibernate.cache.region.factory_class=org.apache.ignite.cache.hibernate.HibernateRegionFactory
 * </pre>
 * Note that before region factory is started you need to start properly configured Ignite node in the same JVM.
 * For example to start Ignite node one of loader provided in {@code org.apache.ignite.grid.startup} package can be used.
 * <p>
 * Name of grid to be used for region factory must be specified as following Hibernate property:
 * <pre name="code" class="brush: xml; gutter: false;">
 * org.apache.ignite.hibernate.grid_name=&lt;grid name&gt;
 * </pre>
 * Each Hibernate cache region must be associated with some {@link IgniteInternalCache}, by default it is assumed that
 * for each cache region there is a {@link IgniteInternalCache} with the same name. Also it is possible to define
 * region to cache mapping using properties with prefix {@code org.apache.ignite.hibernate.region_cache}.
 * For example if for region with name "region1" cache with name "cache1" should be used then following
 * Hibernate property should be specified:
 * <pre name="code" class="brush: xml; gutter: false;">
 * org.apache.ignite.hibernate.region_cache.region1=cache1
 * </pre>
 */
public class HibernateRegionFactory implements RegionFactory {
    /** */
    private static final long serialVersionUID = 0L;

    /** Hibernate L2 cache grid name property name. */
    public static final String GRID_NAME_PROPERTY = "org.apache.ignite.hibernate.grid_name";

    /** Default cache property name. */
    public static final String DFLT_CACHE_NAME_PROPERTY = "org.apache.ignite.hibernate.default_cache";

    /** Property prefix used to specify region name to cache name mapping. */
    public static final String REGION_CACHE_PROPERTY = "org.apache.ignite.hibernate.region_cache.";

    /** */
    public static final String DFLT_ACCESS_TYPE_PROPERTY = "org.apache.ignite.hibernate.default_access_type";

    /** */
    public static final String GRID_CONFIG_PROPERTY = "org.apache.ignite.hibernate.grid_config";

    /** Grid providing caches. */
    private Ignite ignite;

    /** Default cache. */
    private IgniteInternalCache<Object, Object> dfltCache;

    /** Default region access type. */
    private AccessType dfltAccessType;

    /** Region name to cache name mapping. */
    private final Map<String, String> regionCaches = new HashMap<>();

    /** Map needed to provide the same transaction context for different regions. */
    private final ThreadLocal threadLoc = new ThreadLocal();

    /** {@inheritDoc} */
    @Override public void start(Settings settings, Properties props) throws CacheException {
        String gridCfg = props.getProperty(GRID_CONFIG_PROPERTY);
        String gridName = props.getProperty(GRID_NAME_PROPERTY);

        if (gridCfg != null) {
            try {
                ignite = G.start(gridCfg);
            }
            catch (IgniteException e) {
                throw new CacheException(e);
            }
        }
        else
            ignite = Ignition.ignite(gridName);

        String accessType = props.getProperty(DFLT_ACCESS_TYPE_PROPERTY, NONSTRICT_READ_WRITE.name());

        dfltAccessType = AccessType.valueOf(accessType);

        for (Map.Entry<Object, Object> prop : props.entrySet()) {
            String key = prop.getKey().toString();

            if (key.startsWith(REGION_CACHE_PROPERTY)) {
                String regionName = key.substring(REGION_CACHE_PROPERTY.length());

                String cacheName = prop.getValue().toString();

                if (((IgniteKernal)ignite).getCache(cacheName) == null)
                    throw new CacheException("Cache '" + cacheName + "' specified for region '" + regionName + "' " +
                        "is not configured.");

                regionCaches.put(regionName, cacheName);
            }
        }

        String dfltCacheName = props.getProperty(DFLT_CACHE_NAME_PROPERTY);

        if (dfltCacheName != null) {
            dfltCache = ((IgniteKernal)ignite).getCache(dfltCacheName);

            if (dfltCache == null)
                throw new CacheException("Cache specified as default is not configured: " + dfltCacheName);
        }

        IgniteLogger log = ignite.log().getLogger(HibernateRegionFactory.class);

        if (log.isDebugEnabled())
            log.debug("HibernateRegionFactory started [grid=" + gridName + ']');
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
        return new HibernateEntityRegion(this, regionName, ignite, regionCache(regionName), metadata);
    }

    /** {@inheritDoc} */
    @Override public NaturalIdRegion buildNaturalIdRegion(String regionName, Properties props,
        CacheDataDescription metadata) throws CacheException {
        return new HibernateNaturalIdRegion(this, regionName, ignite, regionCache(regionName), metadata);
    }

    /** {@inheritDoc} */
    @Override public CollectionRegion buildCollectionRegion(String regionName, Properties props,
        CacheDataDescription metadata) throws CacheException {
        return new HibernateCollectionRegion(this, regionName, ignite, regionCache(regionName), metadata);
    }

    /** {@inheritDoc} */
    @Override public QueryResultsRegion buildQueryResultsRegion(String regionName, Properties props)
        throws CacheException {
        return new HibernateQueryResultsRegion(this, regionName, ignite, regionCache(regionName));
    }

    /** {@inheritDoc} */
    @Override public TimestampsRegion buildTimestampsRegion(String regionName, Properties props) throws CacheException {
        return new HibernateTimestampsRegion(this, regionName, ignite, regionCache(regionName));
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
    private IgniteInternalCache<Object, Object> regionCache(String regionName) throws CacheException {
        String cacheName = regionCaches.get(regionName);

        if (cacheName == null) {
            if (dfltCache != null)
                return dfltCache;

            cacheName = regionName;
        }

        IgniteInternalCache<Object, Object> cache = ((IgniteKernal)ignite).getCache(cacheName);

        if (cache == null)
            throw new CacheException("Cache '" + cacheName + "' for region '" + regionName + "' is not configured.");

        return cache;
    }
}