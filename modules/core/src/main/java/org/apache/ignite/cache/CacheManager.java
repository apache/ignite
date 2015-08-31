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

package org.apache.ignite.cache;

import java.net.MalformedURLException;
import java.net.URI;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;
import javax.cache.Cache;
import javax.cache.CacheException;
import javax.cache.configuration.CompleteConfiguration;
import javax.cache.configuration.Configuration;
import javax.cache.management.CacheMXBean;
import javax.cache.management.CacheStatisticsMXBean;
import javax.management.MBeanServer;
import javax.management.MalformedObjectNameException;
import javax.management.ObjectName;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.GridKernalGateway;
import org.apache.ignite.internal.GridKernalState;
import org.apache.ignite.internal.IgniteKernal;
import org.apache.ignite.internal.IgnitionEx;
import org.apache.ignite.internal.mxbean.IgniteStandardMXBean;
import org.apache.ignite.internal.processors.cache.IgniteCacheProxy;
import org.apache.ignite.internal.util.typedef.internal.CU;
import org.apache.ignite.spi.discovery.tcp.TcpDiscoverySpi;
import org.apache.ignite.spi.discovery.tcp.ipfinder.vm.TcpDiscoveryVmIpFinder;
import org.jetbrains.annotations.Nullable;

import static org.apache.ignite.IgniteSystemProperties.IGNITE_JCACHE_DEFAULT_ISOLATED;
import static org.apache.ignite.IgniteSystemProperties.getBoolean;

/**
 * Implementation of JSR-107 {@link CacheManager}.
 */
public class CacheManager implements javax.cache.CacheManager {
    /** */
    private static final String CACHE_STATISTICS = "CacheStatistics";

    /** */
    private static final String CACHE_CONFIGURATION = "CacheConfiguration";

    /** */
    private static final AtomicInteger igniteCnt = new AtomicInteger();

    /** */
    private final URI uri;

    /** */
    private final CachingProvider cachingProvider;

    /** */
    private final ClassLoader clsLdr;

    /** */
    private Properties props = new Properties();

    /** */
    private final IgniteKernal ignite;

    /** */
    private final GridKernalGateway kernalGateway;

    /**
     * @param uri Uri.
     * @param cachingProvider Caching provider.
     * @param clsLdr Class loader.
     * @param props Properties.
     */
    public CacheManager(URI uri, CachingProvider cachingProvider, ClassLoader clsLdr, Properties props) {
        this.uri = uri;
        this.cachingProvider = cachingProvider;
        this.clsLdr = clsLdr;
        this.props = props == null ? new Properties() : props;

        try {
            if (uri.equals(cachingProvider.getDefaultURI())) {
                IgniteConfiguration cfg = new IgniteConfiguration();

                if (getBoolean(IGNITE_JCACHE_DEFAULT_ISOLATED, true)) {
                    TcpDiscoverySpi discoSpi = new TcpDiscoverySpi();

                    discoSpi.setIpFinder(new TcpDiscoveryVmIpFinder(true));

                    cfg.setDiscoverySpi(discoSpi);
                }

                cfg.setGridName("CacheManager_" + igniteCnt.getAndIncrement());

                ignite = (IgniteKernal)IgnitionEx.start(cfg);
            }
            else
                ignite = (IgniteKernal)IgnitionEx.start(uri.toURL());

            kernalGateway = ignite.context().gateway();
        }
        catch (IgniteCheckedException e) {
            throw CU.convertToCacheException(e);
        }
        catch (MalformedURLException e) {
            throw new CacheException(e);
        }
    }

    /** {@inheritDoc} */
    @Override public CachingProvider getCachingProvider() {
        return cachingProvider;
    }

    /** {@inheritDoc} */
    @Override public URI getURI() {
        return uri;
    }

    /** {@inheritDoc} */
    @Override public ClassLoader getClassLoader() {
        return clsLdr;
    }

    /** {@inheritDoc} */
    @Override public Properties getProperties() {
        return props;
    }

    /** {@inheritDoc} */
    @SuppressWarnings("unchecked")
    @Override public <K, V, C extends Configuration<K, V>> Cache<K, V> createCache(String cacheName, C cacheCfg)
        throws IllegalArgumentException {
        kernalGateway.readLock();

        try {
            if (cacheCfg == null)
                throw new NullPointerException();

            if (cacheName == null)
                throw new NullPointerException();

            CacheConfiguration<K, V> igniteCacheCfg;

            if (cacheCfg instanceof CompleteConfiguration)
                igniteCacheCfg = new CacheConfiguration<>((CompleteConfiguration<K, V>)cacheCfg);
            else {
                igniteCacheCfg = new CacheConfiguration<>();

                igniteCacheCfg.setTypes(cacheCfg.getKeyType(), cacheCfg.getValueType());
            }

            igniteCacheCfg.setName(cacheName);

            IgniteCache<K, V> res = ignite.createCache(igniteCacheCfg);

            if (res == null)
                throw new CacheException();

            ((IgniteCacheProxy<K, V>)res).setCacheManager(this);

            if (igniteCacheCfg.isManagementEnabled())
                enableManagement(cacheName, true);

            if (igniteCacheCfg.isStatisticsEnabled())
                enableStatistics(cacheName, true);

            return res;
        }
        finally {
            kernalGateway.readUnlock();
        }
    }

    /** {@inheritDoc} */
    @Override public <K, V> Cache<K, V> getCache(String cacheName, Class<K> keyType, Class<V> valType) {
        kernalGateway.readLock();

        try {
            Cache<K, V> cache = getCache0(cacheName);

            if (cache != null) {
                if(!keyType.isAssignableFrom(cache.getConfiguration(Configuration.class).getKeyType()))
                    throw new ClassCastException();

                if(!valType.isAssignableFrom(cache.getConfiguration(Configuration.class).getValueType()))
                    throw new ClassCastException();
            }

            return cache;
        }
        finally {
            kernalGateway.readUnlock();
        }
    }

    /** {@inheritDoc} */
    @Override public <K, V> Cache<K, V> getCache(String cacheName) {
        kernalGateway.readLock();

        try {
            IgniteCache<K, V> cache = getCache0(cacheName);

            if (cache != null) {
                if(cache.getConfiguration(Configuration.class).getKeyType() != Object.class)
                    throw new IllegalArgumentException();

                if(cache.getConfiguration(Configuration.class).getValueType() != Object.class)
                    throw new IllegalArgumentException();
            }

            return cache;
        }
        finally {
            kernalGateway.readUnlock();
        }
    }

    /**
     * @param cacheName Cache name.
     * @return Cache.
     */
    @Nullable private <K, V> IgniteCache<K, V> getCache0(String cacheName) {
        if (cacheName == null)
            throw new NullPointerException();

        try {
            return ignite.cache(cacheName);
        }
        catch (IllegalArgumentException ignored) {
            return null;
        }
    }

    /** {@inheritDoc} */
    @Override public Iterable<String> getCacheNames() {
        kernalGateway.readLockAnyway();

        try {
            if (kernalGateway.getState() != GridKernalState.STARTED)
                return Collections.emptySet(); // javadoc of #getCacheNames() says that IllegalStateException should be
                                               // thrown but CacheManagerTest.close_cachesEmpty() require empty collection.

            Collection<String> res = new ArrayList<>();

            for (IgniteCache<?, ?> cache : ignite.context().cache().publicCaches())
                res.add(cache.getName());

            return Collections.unmodifiableCollection(res);
        }
        finally {
            kernalGateway.readUnlock();
        }
    }

    /** {@inheritDoc} */
    @Override public void destroyCache(String cacheName) {
        kernalGateway.readLock();

        IgniteCache<?, ?> cache;

        try {
            cache = getCache0(cacheName);

            if (cache != null) {
                unregisterCacheObject(cacheName, CACHE_CONFIGURATION);
                unregisterCacheObject(cacheName, CACHE_STATISTICS);
            }
        }
        finally {
            kernalGateway.readUnlock();
        }

        if (cache != null)
            cache.destroy();
    }

    /**
     * @param cacheName Cache name.
     * @param objName Object name.
     * @return Object name instance.
     */
    private ObjectName getObjectName(String cacheName, String objName) {
        String mBeanName = "javax.cache:type=" + objName + ",CacheManager="
            + uri.toString().replaceAll(",|:|=|\n", ".")
            + ",Cache=" + cacheName.replaceAll(",|:|=|\n", ".");

        try {
            return new ObjectName(mBeanName);
        }
        catch (MalformedObjectNameException e) {
            throw new CacheException("Failed to create MBean name: " + mBeanName, e);
        }
    }

    /** {@inheritDoc} */
    @Override public void enableManagement(String cacheName, boolean enabled) {
        kernalGateway.readLock();

        try {
            IgniteCache<?, ?> cache = getCache0(cacheName);

            if (cache == null)
                throw new CacheException("Cache not found: " + cacheName);

            if (enabled)
                registerCacheObject(cache.mxBean(), cacheName, CACHE_CONFIGURATION);
            else
                unregisterCacheObject(cacheName, CACHE_CONFIGURATION);

            cache.getConfiguration(CacheConfiguration.class).setManagementEnabled(enabled);
        }
        finally {
            kernalGateway.readUnlock();
        }
    }

    /** {@inheritDoc} */
    @Override public void enableStatistics(String cacheName, boolean enabled) {
        kernalGateway.readLock();

        try {
            IgniteCache<?, ?> cache = getCache0(cacheName);

            if (cache == null)
                throw new CacheException("Cache not found: " + cacheName);

            CacheConfiguration cfg = cache.getConfiguration(CacheConfiguration.class);

            if (enabled)
                registerCacheObject(cache.mxBean(), cacheName, CACHE_STATISTICS);
            else
                unregisterCacheObject(cacheName, CACHE_STATISTICS);

            cfg.setStatisticsEnabled(enabled);
        }
        finally {
            kernalGateway.readUnlock();
        }
    }

    /**
     * @param mxbean MXBean.
     * @param name Cache name.
     * @param beanType Bean type.
     */
    private void registerCacheObject(Object mxbean, String name, String beanType) {
        MBeanServer mBeanSrv = ignite.configuration().getMBeanServer();

        ObjectName registeredObjName = getObjectName(name, beanType);

        try {
            if (mBeanSrv.queryNames(registeredObjName, null).isEmpty()) {
                IgniteStandardMXBean bean = beanType.equals(CACHE_CONFIGURATION)
                    ? new IgniteStandardMXBean((CacheMXBean)mxbean, CacheMXBean.class)
                    : new IgniteStandardMXBean((CacheStatisticsMXBean)mxbean, CacheStatisticsMXBean.class);

                mBeanSrv.registerMBean(bean, registeredObjName);
            }
        }
        catch (Exception e) {
            throw new CacheException("Failed to register MBean: " + registeredObjName, e);
        }
    }

    /**
     * UnRegisters the mxbean if registered already.
     *
     * @param name Cache name.
     * @param beanType Mxbean name.
     */
    private void unregisterCacheObject(String name, String beanType) {
        MBeanServer mBeanSrv = ignite.configuration().getMBeanServer();

        Set<ObjectName> registeredObjNames = mBeanSrv.queryNames(getObjectName(name, beanType), null);

        //should just be one
        for (ObjectName registeredObjectName : registeredObjNames) {
            try {
                mBeanSrv.unregisterMBean(registeredObjectName);
            }
            catch (Exception e) {
                throw new CacheException("Error unregistering object instance " + registeredObjectName
                    + " . Error was " + e.getMessage(), e);
            }
        }
    }

    /** {@inheritDoc} */
    @Override public void close() {
        try {
            ignite.close();
        }
        catch (Exception ignored) {
            // Ignore any exceptions according to javadoc of javax.cache.CacheManager#close()
        }
        finally {
            cachingProvider.removeClosedManager(this);
        }
    }

    /** {@inheritDoc} */
    @Override public boolean isClosed() {
        kernalGateway.readLockAnyway();

        try {
            return kernalGateway.getState() != GridKernalState.STARTED;
        }
        finally {
            kernalGateway.readUnlock();
        }
    }

    /** {@inheritDoc} */
    @Override public <T> T unwrap(Class<T> clazz) {
        if(clazz.isAssignableFrom(getClass()))
            return clazz.cast(this);

        if(clazz.isAssignableFrom(ignite.getClass()))
            return clazz.cast(ignite);

        throw new IllegalArgumentException();
    }
}