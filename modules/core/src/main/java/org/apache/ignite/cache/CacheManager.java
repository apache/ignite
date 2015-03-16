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

import org.apache.ignite.*;
import org.apache.ignite.configuration.*;
import org.apache.ignite.internal.*;
import org.apache.ignite.internal.mxbean.*;
import org.apache.ignite.internal.processors.cache.*;
import org.apache.ignite.internal.util.typedef.internal.*;

import javax.cache.*;
import javax.cache.configuration.*;
import javax.cache.management.*;
import javax.management.*;
import java.net.*;
import java.util.*;
import java.util.concurrent.atomic.*;

/**
 * Implementation of JSR-107 {@link CacheManager}.
 */
public class CacheManager implements javax.cache.CacheManager {
    /** */
    private static final String CACHE_STATISTICS = "CacheStatistics";

    /** */
    private static final String CACHE_CONFIGURATION = "CacheConfiguration";

    /** */
    private final URI uri;

    /** */
    private final CachingProvider cachingProvider;

    /** */
    private final ClassLoader clsLdr;

    /** */
    private final Properties props;

    /** */
    private final AtomicBoolean closed = new AtomicBoolean();

    /** */
    private final IgniteKernal ignite;

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
        this.props = props;

        try {
            if (uri.equals(cachingProvider.getDefaultURI()))
                ignite = (IgniteKernal)IgnitionEx.start();
            else
                ignite = (IgniteKernal)IgnitionEx.start(uri.toURL());
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
    @Override public <K, V, C extends Configuration<K, V>> Cache<K, V> createCache(String cacheName, C cacheCfg)
        throws IllegalArgumentException {
        ensureNotClosed();

        if (cacheCfg == null)
            throw new NullPointerException();

        if (cacheName == null)
            throw new NullPointerException();

        if (getCache0(cacheName) != null)
            throw new CacheException("Cache already exists [cacheName=" + cacheName + ", manager=" + uri + ']');

        CacheConfiguration<K, V> igniteCacheCfg;

        if (cacheCfg instanceof CompleteConfiguration)
            igniteCacheCfg = new CacheConfiguration<>((CompleteConfiguration<K, V>)cacheCfg);
        else {
            igniteCacheCfg = new CacheConfiguration<>();

            igniteCacheCfg.setTypes(cacheCfg.getKeyType(), cacheCfg.getValueType());
        }

        igniteCacheCfg.setName(cacheName);

        IgniteCache<K, V> res = ignite.createCache(igniteCacheCfg);

        ((IgniteCacheProxy<K, V>)res).setCacheManager(this);

        if (res == null)
            throw new CacheException();

        if (igniteCacheCfg.isManagementEnabled())
            enableManagement(cacheName, true);

        if (igniteCacheCfg.isStatisticsEnabled())
            enableStatistics(cacheName, true);

        return res;
    }

    /** {@inheritDoc} */
    @Override public <K, V> Cache<K, V> getCache(String cacheName, Class<K> keyType, Class<V> valType) {
        ensureNotClosed();

        Cache<K, V> cache = getCache0(cacheName);

        if (cache != null) {
            if(!keyType.isAssignableFrom(cache.getConfiguration(Configuration.class).getKeyType()))
                throw new ClassCastException();

            if(!valType.isAssignableFrom(cache.getConfiguration(Configuration.class).getValueType()))
                throw new ClassCastException();
        }

        return cache;
    }

    /** {@inheritDoc} */
    @Override public <K, V> Cache<K, V> getCache(String cacheName) {
        ensureNotClosed();

        IgniteCache<K, V> cache = getCache0(cacheName);

        if (cache != null) {
            if(cache.getConfiguration(Configuration.class).getKeyType() != Object.class)
                throw new IllegalArgumentException();

            if(cache.getConfiguration(Configuration.class).getValueType() != Object.class)
                throw new IllegalArgumentException();
        }

        return cache;
    }

    /**
     * @param cacheName Cache name.
     */
    private <K, V> IgniteCache<K, V> getCache0(String cacheName) {
        try {
            return ignite.jcache(cacheName);
        }
        catch (IllegalArgumentException ignored) {
            return null;
        }
    }

    /** {@inheritDoc} */
    @Override public Iterable<String> getCacheNames() {
        if (isClosed())
            return Collections.emptySet(); // javadoc of #getCacheNames() says that IllegalStateException should be
                                           // thrown but CacheManagerTest.close_cachesEmpty() require empty collection.

        Collection<String> res = new ArrayList<>();

        for (GridCache<?, ?> cache : ignite.caches())
            res.add(cache.name());

        return Collections.unmodifiableCollection(res);
    }

    /** {@inheritDoc} */
    @Override public void destroyCache(String cacheName) {
        ensureNotClosed();

        if (cacheName == null)
            throw new NullPointerException();

        IgniteCache<?, ?> cache = getCache0(cacheName);

        if (cache != null)
            cache.close();
    }

    /**
     * @param cacheName Cache name.
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
        ensureNotClosed();

        if (cacheName == null)
            throw new NullPointerException();

        IgniteCache<?, ?> cache = getCache0(cacheName);

        if (cache == null)
            throw new CacheException("Cache not found: " + cacheName);

        if (enabled)
            registerCacheObject(cache.mxBean(), cacheName, CACHE_CONFIGURATION);
        else
            unregisterCacheObject(cacheName, CACHE_CONFIGURATION);

        cache.getConfiguration(CacheConfiguration.class).setManagementEnabled(enabled);
    }

    /** {@inheritDoc} */
    @Override public void enableStatistics(String cacheName, boolean enabled) {
        ensureNotClosed();

        if (cacheName == null)
            throw new NullPointerException();

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

    /**
     * @param mxbean MXBean.
     * @param name cache name.
     */
    public void registerCacheObject(Object mxbean, String name, String beanType) {
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

    /**
     *
     */
    private void ensureNotClosed() throws IllegalStateException {
        if (closed.get())
            throw new IllegalStateException("Cache manager are closed [uri=" + uri + ", classLoader=" + clsLdr + ']');
    }

    /** {@inheritDoc} */
    @Override public void close() {
        if (closed.compareAndSet(false, true)) {
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
    }

    /** {@inheritDoc} */
    @Override public boolean isClosed() {
        return closed.get();
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
