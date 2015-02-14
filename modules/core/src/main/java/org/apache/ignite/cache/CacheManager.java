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
import org.apache.ignite.internal.mxbean.*;

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
    private final Map<String, Ignite> igniteMap = new HashMap<>();

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
    private final AtomicInteger mgrIdx = new AtomicInteger();

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

        CacheConfiguration igniteCacheCfg;

        if (cacheCfg instanceof CompleteConfiguration)
            igniteCacheCfg = new CacheConfiguration((CompleteConfiguration)cacheCfg);
        else {
            igniteCacheCfg = new CacheConfiguration();

            igniteCacheCfg.setTypes(cacheCfg.getKeyType(), cacheCfg.getValueType());
            igniteCacheCfg.setStoreValueBytes(cacheCfg.isStoreByValue());
        }

        igniteCacheCfg.setName(cacheName);

        IgniteCache<K, V> res;

        synchronized (igniteMap) {
            if (igniteMap.containsKey(cacheName))
                throw new CacheException("Cache already exists [cacheName=" + cacheName + ", manager=" + uri + ']');

            Ignite ignite;

            if (uri.equals(cachingProvider.getDefaultURI())) {
                IgniteConfiguration cfg = new IgniteConfiguration();
                cfg.setGridName(mgrIdx.incrementAndGet() + "-grid-for-" + cacheName);

                cfg.setCacheConfiguration(igniteCacheCfg);

                try {
                    ignite = Ignition.start(cfg);
                }
                catch (IgniteException e) {
                    throw new CacheException(e);
                }
            }
            else
                throw new UnsupportedOperationException();

            res = ignite.jcache(cacheName);

            igniteMap.put(cacheName, ignite);
        }

        if (igniteCacheCfg.isManagementEnabled())
            enableManagement(cacheName, true);

        if (igniteCacheCfg.isStatisticsEnabled())
            enableStatistics(cacheName, true);

        return res;
    }

    /**
     * @param cacheName Cache name.
     */
    private <K, V> IgniteCache<K, V> findCache(String cacheName) {
        Ignite ignite;

        synchronized (igniteMap) {
            ignite = igniteMap.get(cacheName);
        }

        if (ignite == null)
            return null;

        return ignite.jcache(cacheName);
    }

    /** {@inheritDoc} */
    @Override public <K, V> Cache<K, V> getCache(String cacheName, Class<K> keyType, Class<V> valType) {
        ensureNotClosed();

        Cache<K, V> cache = findCache(cacheName);

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

        IgniteCache<K, V> cache = findCache(cacheName);

        if (cache != null) {
            if(cache.getConfiguration(Configuration.class).getKeyType() != Object.class)
                throw new IllegalArgumentException();

            if(cache.getConfiguration(Configuration.class).getValueType() != Object.class)
                throw new IllegalArgumentException();
        }

        return cache;
    }

    /** {@inheritDoc} */
    @Override public Iterable<String> getCacheNames() {
        if (isClosed())
            return Collections.emptySet(); // javadoc of #getCacheNames() says that IllegalStateException should be
                                           // thrown but CacheManagerTest.close_cachesEmpty() require empty collection.

        String[] resArr;

        synchronized (igniteMap) {
            resArr = igniteMap.keySet().toArray(new String[igniteMap.keySet().size()]);
        }

        return Collections.unmodifiableCollection(Arrays.asList(resArr));
    }

    /**
     * @param ignite Ignite.
     */
    public boolean isManagedIgnite(Ignite ignite) {
        synchronized (igniteMap) {
            for (Ignite instance : igniteMap.values()) {
                if (ignite.equals(instance))
                    return true;
            }
        }

        return false;
    }

    /** {@inheritDoc} */
    @Override public void destroyCache(String cacheName) {
        ensureNotClosed();

        if (cacheName == null)
            throw new NullPointerException();

        Ignite ignite;

        synchronized (igniteMap) {
            ignite = igniteMap.remove(cacheName);
        }

        if (ignite != null) {
            try {
                ignite.close();
            }
            catch (Exception ignored) {
                // No-op.
            }

            MBeanServer mBeanSrv = ignite.configuration().getMBeanServer();

            unregisterCacheObject(mBeanSrv, cacheName, CACHE_STATISTICS);

            unregisterCacheObject(mBeanSrv, cacheName, CACHE_CONFIGURATION);
        }
    }

    /**
     * @param cacheName Cache name.
     */
    private ObjectName getObjectName(String cacheName, String objectName) {
        String mBeanName = "javax.cache:type=" + objectName + ",CacheManager="
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

        Ignite ignite;

        synchronized (igniteMap) {
            ignite = igniteMap.get(cacheName);
        }

        MBeanServer mBeanSrv = ignite.configuration().getMBeanServer();

        if (enabled) {
            registerCacheObject(mBeanSrv, ignite.jcache(cacheName).mxBean(), cacheName, CACHE_CONFIGURATION);

            ignite.jcache(cacheName).getConfiguration(CacheConfiguration.class).setManagementEnabled(true);
        }
        else {
            unregisterCacheObject(mBeanSrv, cacheName, CACHE_CONFIGURATION);

            ignite.jcache(cacheName).getConfiguration(CacheConfiguration.class).setManagementEnabled(false);
        }
    }


    /** {@inheritDoc} */
    @Override public void enableStatistics(String cacheName, boolean enabled) {
        ensureNotClosed();

        if (cacheName == null)
            throw new NullPointerException();

        Ignite ignite;

        synchronized (igniteMap) {
            ignite = igniteMap.get(cacheName);
        }

        IgniteCache<Object, Object> cache = ignite.jcache(cacheName);

        CacheConfiguration cfg = cache.getConfiguration(CacheConfiguration.class);

        MBeanServer mBeanSrv = ignite.configuration().getMBeanServer();

        if (enabled) {
            registerCacheObject(mBeanSrv, cache.mxBean(), cacheName, CACHE_STATISTICS);

            cfg.setStatisticsEnabled(true);
        }
        else {
            unregisterCacheObject(mBeanSrv, cacheName, CACHE_STATISTICS);

            cfg.setStatisticsEnabled(false);
        }
    }

    /**
     * @param mxbean MXBean.
     * @param name cache name.
     */
    public void registerCacheObject(MBeanServer mBeanServer, Object mxbean, String name, String objectName) {
        ObjectName registeredObjName = getObjectName(name, objectName);

        try {
            if (!isRegistered(mBeanServer, registeredObjName))
                if (objectName.equals(CACHE_CONFIGURATION))
                    mBeanServer.registerMBean(new IgniteStandardMXBean((CacheMXBean)mxbean, CacheMXBean.class),
                        registeredObjName);
                else
                    mBeanServer.registerMBean(
                        new IgniteStandardMXBean((CacheStatisticsMXBean)mxbean, CacheStatisticsMXBean.class),
                        registeredObjName);
        }
        catch (Exception e) {
            throw new CacheException("Failed to register MBean: " + registeredObjName, e);
        }
    }

    /**
     * @return {@code True} if MBean registered.
     */
    private static boolean isRegistered(MBeanServer mBeanServer, ObjectName objectName) {
        return !mBeanServer.queryNames(objectName, null).isEmpty();
    }

    /**
     * UnRegisters the mxbean if registered already.
     *
     * @param mBeanSrv MBean server
     * @param name Cache name.
     * @param objectName Mxbean name.
     */
    public void unregisterCacheObject(MBeanServer mBeanSrv, String name, String objectName) {
        Set<ObjectName> registeredObjectNames;

        ObjectName objName = getObjectName(name, objectName);

        registeredObjectNames = mBeanSrv.queryNames(objName, null);

        //should just be one
        for (ObjectName registeredObjectName : registeredObjectNames) {
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
            Ignite[] ignites;

            synchronized (igniteMap) {
                ignites = igniteMap.values().toArray(new Ignite[igniteMap.values().size()]);

                igniteMap.clear();
            }

            for (Ignite ignite : ignites) {
                try {
                    ignite.close();
                }
                catch (Exception ignored) {
                    // Ignore any exceptions according to javadoc of javax.cache.CacheManager#close()
                }
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

//        if(clazz.isAssignableFrom(ignite.getClass()))
//            return clazz.cast(ignite);

        throw new IllegalArgumentException();
    }
}
