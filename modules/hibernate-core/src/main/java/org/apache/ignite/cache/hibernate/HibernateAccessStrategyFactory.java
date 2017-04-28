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
import java.util.concurrent.ConcurrentHashMap;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteException;
import org.apache.ignite.IgniteLogger;
import org.apache.ignite.Ignition;
import org.apache.ignite.configuration.TransactionConfiguration;
import org.apache.ignite.internal.IgniteKernal;
import org.apache.ignite.internal.processors.cache.IgniteInternalCache;
import org.apache.ignite.internal.util.typedef.G;

import static org.apache.ignite.cache.CacheAtomicityMode.TRANSACTIONAL;

/**
 * Access strategy factory.
 */
public class HibernateAccessStrategyFactory {
    /** */
    private final HibernateKeyTransformer keyTransformer;

    /** */
    private final HibernateExceptionConverter eConverter;

    /**
     * Hibernate L2 cache grid name property name.
     *
     * @deprecated Use {@link #IGNITE_INSTANCE_NAME_PROPERTY}.
     *      If {@link #IGNITE_INSTANCE_NAME_PROPERTY} is specified it takes precedence.
     */
    @Deprecated
    public static final String GRID_NAME_PROPERTY = "org.apache.ignite.hibernate.grid_name";

    /** Hibernate L2 cache Ignite instance name property name. */
    public static final String IGNITE_INSTANCE_NAME_PROPERTY = "org.apache.ignite.hibernate.ignite_instance_name";

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
    private HibernateCacheProxy dfltCache;

    /** Region name to cache name mapping. */
    private final Map<String, String> regionCaches = new HashMap<>();

    /** */
    private final ThreadLocal threadLoc = new ThreadLocal();

    /** */
    private final ConcurrentHashMap<String, ThreadLocal> threadLocMap = new ConcurrentHashMap<>();

    /**
     * @param keyTransformer Key transformer.
     * @param eConverter Exception converter.
     */
    HibernateAccessStrategyFactory(HibernateKeyTransformer keyTransformer, HibernateExceptionConverter eConverter) {
        this.keyTransformer = keyTransformer;
        this.eConverter = eConverter;
    }

    /**
     * @param props Properties.
     */
    public void start(Properties props)  {
        String gridCfg = props.getProperty(GRID_CONFIG_PROPERTY);
        String igniteInstanceName = props.getProperty(IGNITE_INSTANCE_NAME_PROPERTY);

        if (igniteInstanceName == null)
            igniteInstanceName = props.getProperty(GRID_NAME_PROPERTY);

        if (gridCfg != null) {
            try {
                ignite = G.start(gridCfg);
            }
            catch (IgniteException e) {
                throw eConverter.convert(e);
            }
        }
        else
            ignite = Ignition.ignite(igniteInstanceName);

        for (Map.Entry<Object, Object> prop : props.entrySet()) {
            String key = prop.getKey().toString();

            if (key.startsWith(REGION_CACHE_PROPERTY)) {
                String regionName = key.substring(REGION_CACHE_PROPERTY.length());

                String cacheName = prop.getValue().toString();

                if (((IgniteKernal)ignite).getCache(cacheName) == null)
                    throw new IllegalArgumentException("Cache '" + cacheName + "' specified for region '" + regionName + "' " +
                        "is not configured.");

                regionCaches.put(regionName, cacheName);
            }
        }

        String dfltCacheName = props.getProperty(DFLT_CACHE_NAME_PROPERTY);

        if (dfltCacheName != null) {
            IgniteInternalCache<Object, Object> dfltCache = ((IgniteKernal)ignite).getCache(dfltCacheName);

            if (dfltCache == null)
                throw new IllegalArgumentException("Cache specified as default is not configured: " + dfltCacheName);

            this.dfltCache = new HibernateCacheProxy(dfltCache, keyTransformer);
        }

        IgniteLogger log = ignite.log().getLogger(getClass());

        if (log.isDebugEnabled())
            log.debug("HibernateRegionFactory started [igniteInstanceName=" + igniteInstanceName + ']');
    }

    /**
     * @return Ignite node.
     */
    public Ignite node() {
        return ignite;
    }

    /**
     * @param regionName L2 cache region name.
     * @return Cache for given region.
     */
    HibernateCacheProxy regionCache(String regionName) {
        String cacheName = regionCaches.get(regionName);

        if (cacheName == null) {
            if (dfltCache != null)
                return dfltCache;

            cacheName = regionName;
        }

        IgniteInternalCache<Object, Object> cache = ((IgniteKernal)ignite).getCache(cacheName);

        if (cache == null)
            throw new IllegalArgumentException("Cache '" + cacheName + "' for region '" + regionName + "' is not configured.");

        return new HibernateCacheProxy(cache, keyTransformer);
    }

    /**
     * @param cache Cache.
     * @return Access strategy implementation.
     */
    HibernateAccessStrategyAdapter createReadOnlyStrategy(HibernateCacheProxy cache) {
        return new HibernateReadOnlyAccessStrategy(ignite, cache, eConverter);
    }

    /**
     * @param cache Cache.
     * @return Access strategy implementation.
     */
    HibernateAccessStrategyAdapter createNonStrictReadWriteStrategy(HibernateCacheProxy cache) {
        ThreadLocal threadLoc = threadLocMap.get(cache.name());

        if (threadLoc == null) {
            ThreadLocal old = threadLocMap.putIfAbsent(cache.name(), threadLoc = new ThreadLocal());

            if (old != null)
                threadLoc = old;
        }

        return new HibernateNonStrictAccessStrategy(ignite, cache, threadLoc, eConverter);
    }

    /**
     * @param cache Cache.
     * @return Access strategy implementation.
     */
    HibernateAccessStrategyAdapter createReadWriteStrategy(HibernateCacheProxy cache) {
        if (cache.configuration().getAtomicityMode() != TRANSACTIONAL)
            throw new IllegalArgumentException("Hibernate READ-WRITE access strategy must have Ignite cache with " +
                "'TRANSACTIONAL' atomicity mode: " + cache.name());

        return new HibernateReadWriteAccessStrategy(ignite, cache, threadLoc, eConverter);
    }

    /**
     * @param cache Cache.
     * @return Access strategy implementation.
     */
    HibernateAccessStrategyAdapter createTransactionalStrategy(HibernateCacheProxy cache) {
        if (cache.configuration().getAtomicityMode() != TRANSACTIONAL)
            throw new IllegalArgumentException("Hibernate TRANSACTIONAL access strategy must have Ignite cache with " +
                "'TRANSACTIONAL' atomicity mode: " + cache.name());

        TransactionConfiguration txCfg = ignite.configuration().getTransactionConfiguration();

        if (txCfg == null ||
            (txCfg.getTxManagerFactory() == null
                && txCfg.getTxManagerLookupClassName() == null
                && cache.configuration().getTransactionManagerLookupClassName() == null)) {
            throw new IllegalArgumentException("Hibernate TRANSACTIONAL access strategy must have Ignite with " +
                "Factory<TransactionManager> configured (see IgniteConfiguration." +
                "getTransactionConfiguration().setTxManagerFactory()): " + cache.name());
        }

        return new HibernateTransactionalAccessStrategy(ignite, cache, eConverter);
    }
}
