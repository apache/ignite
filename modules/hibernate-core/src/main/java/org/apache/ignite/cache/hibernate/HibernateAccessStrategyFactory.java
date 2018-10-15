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

import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteException;
import org.apache.ignite.IgniteLogger;
import org.apache.ignite.Ignition;
import org.apache.ignite.configuration.TransactionConfiguration;
import org.apache.ignite.internal.IgniteKernal;
import org.apache.ignite.internal.processors.cache.IgniteInternalCache;
import org.apache.ignite.internal.util.typedef.G;

import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Supplier;

import static java.lang.String.format;
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

    /** Property prefix used to specify region name to cache name mapping. */
    public static final String REGION_CACHE_PROPERTY = "org.apache.ignite.hibernate.region_cache.";

    /** */
    public static final String DFLT_ACCESS_TYPE_PROPERTY = "org.apache.ignite.hibernate.default_access_type";

    /** */
    public static final String GRID_CONFIG_PROPERTY = "org.apache.ignite.hibernate.grid_config";

    /** */
    public static final String VERIFY_ATOMICITY = "org.apache.ignite.hibernate.verify_atomicity";

    /** when set, all cache names in ignite will be fetched using the specified prefix */
    public static final String CACHE_PREFIX = "org.apache.ignite.hibernate.cache_prefix";

    /** Grid providing caches. */
    private Ignite ignite;

    /** Region name to cache name mapping. */
    private final Map<String, String> regionCaches = new HashMap<>();

    /** */
    private final ThreadLocal threadLoc = new ThreadLocal();

    /** */
    private final ConcurrentHashMap<String, ThreadLocal> threadLocMap = new ConcurrentHashMap<>();
    private String cachePrefix;
    private boolean verifyAtomicity = true;

    /**
     * @param keyTransformer Key transformer.
     * @param eConverter Exception converter.
     */
    HibernateAccessStrategyFactory(HibernateKeyTransformer keyTransformer, HibernateExceptionConverter eConverter) {
        this.keyTransformer = keyTransformer;
        this.eConverter = eConverter;
    }

    /**
     * @param configValues {@link Map} of config values.
     */
    public void start(Map configValues)  {
        cachePrefix = configValues.getOrDefault(CACHE_PREFIX, "").toString();

        verifyAtomicity = Boolean.valueOf(configValues.getOrDefault(VERIFY_ATOMICITY, "true").toString());

        Object gridCfg = configValues.get(GRID_CONFIG_PROPERTY);

        Object igniteInstanceName = configValues.get(IGNITE_INSTANCE_NAME_PROPERTY);

        if (gridCfg != null) {
            try {
                ignite = G.start(gridCfg.toString());
            } catch (IgniteException e) {
                throw eConverter.convert(e);
            }
        } else {
            ignite = Ignition.ignite(igniteInstanceName.toString());
        }

        for (Iterator<Map.Entry> it=configValues.entrySet().iterator(); it.hasNext(); ) {
            Map.Entry entry = it.next();

            String key = entry.getKey().toString();

            String cacheName = entry.getValue().toString();

            if (key.startsWith(REGION_CACHE_PROPERTY)) {
                String regionName = key.substring(REGION_CACHE_PROPERTY.length());
                if (((IgniteKernal) ignite).getCache(cachePrefix + cacheName) == null)
                    throw new IllegalArgumentException(
                        format("Cache '%s' specified for region '%s' is not configured.", cacheName, regionName));

                regionCaches.put(regionName, cacheName);
            }
        };

        IgniteLogger log = ignite.log().getLogger(getClass());

        if (log.isDebugEnabled())
            log.debug(format("HibernateRegionFactory started [igniteInstanceName=%s]", igniteInstanceName));

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

        if (cacheName == null)
            cacheName = regionName;

        cacheName = cachePrefix + cacheName;

        Supplier<IgniteInternalCache<Object, Object>> lazyCache = getLazyCache(cacheName, regionName);

        return new HibernateCacheProxy(cacheName, lazyCache, keyTransformer);
    }

    private Supplier<IgniteInternalCache<Object,Object>> getLazyCache(String cacheName, String regionName) {
        AtomicReference<IgniteInternalCache<Object, Object>> reference = new AtomicReference<>();
        return () -> {
            reference.compareAndSet(null, ((IgniteKernal) ignite).getCache(cacheName));

            IgniteInternalCache<Object, Object> cache = reference.get();

            if (cache == null)
                throw new IllegalArgumentException(
                    format("Cache '%s' for region '%s' is not configured.", cacheName, regionName));

            return cache;
        };
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
     * NOTE: the original ignite hibernate integration checked whether the cache configuration was set with
     * AtomicityMode = TRANSACTIONAL and would error if the cache didn't exist or the mode was set correctly.
     * This was removed since we are creating caching dynamicly at startup via an ApplicationListener
     *
     * @param cache Cache.
     * @return Access strategy implementation.
     */
    HibernateAccessStrategyAdapter createReadWriteStrategy(HibernateCacheProxy cache) {
        if (verifyAtomicity)
            if (cache.configuration().getAtomicityMode() != TRANSACTIONAL)
                throw new IllegalArgumentException("Hibernate READ-WRITE access strategy must have Ignite cache with " +
                    "'TRANSACTIONAL' atomicity mode: " + cache.name());

        return new HibernateReadWriteAccessStrategy(ignite, cache, threadLoc, eConverter);
    }

    /**
     * NOTE: the original ignite hibernate integration checked whether the cache configuration was set with
     * AtomicityMode = TRANSACTIONAL and would error if the cache didn't exist or the mode was set correctly.
     * This was removed since we are creating caching dynamicly at startup via an ApplicationListener
     *
     * @param cache Cache.
     * @return Access strategy implementation.
     */
    HibernateAccessStrategyAdapter createTransactionalStrategy(HibernateCacheProxy cache) {
        if (verifyAtomicity) {
            if (cache.configuration().getAtomicityMode() != TRANSACTIONAL)
                throw new IllegalArgumentException("Hibernate TRANSACTIONAL access strategy must have Ignite cache with " +
                    "'TRANSACTIONAL' atomicity mode: " + cache.name());

            TransactionConfiguration txCfg = ignite.configuration().getTransactionConfiguration();

            if (txCfg == null ||
                (txCfg.getTxManagerFactory() == null
                    && txCfg.getTxManagerLookupClassName() == null
                    && cache.configuration().getTransactionManagerLookupClassName() == null))
                throw new IllegalArgumentException("Hibernate TRANSACTIONAL access strategy must have Ignite with " +
                    "Factory<TransactionManager> configured (see IgniteConfiguration." +
                    "getTransactionConfiguration().setTxManagerFactory()): " + cache.name());

        }
        return new HibernateTransactionalAccessStrategy(ignite, cache, eConverter);
    }
}
