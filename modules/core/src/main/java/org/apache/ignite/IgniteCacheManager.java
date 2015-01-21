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

package org.apache.ignite;

import org.apache.ignite.cache.*;
import org.apache.ignite.configuration.*;
import org.apache.ignite.lang.*;
import org.gridgain.grid.cache.*;
import org.gridgain.grid.util.typedef.*;

import javax.cache.*;
import javax.cache.configuration.*;
import javax.cache.spi.*;
import javax.management.*;
import java.net.*;
import java.util.*;
import java.util.concurrent.atomic.*;

/**
 *
 */
public class IgniteCacheManager implements CacheManager {
    /** */
    private final Map<String, IgniteBiTuple<Ignite, IgniteCacheMXBean>> igniteMap = new HashMap<>();

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

    /**
     * @param uri Uri.
     * @param cachingProvider Caching provider.
     * @param clsLdr Class loader.
     * @param props Properties.
     */
    public IgniteCacheManager(URI uri, CachingProvider cachingProvider, ClassLoader clsLdr, Properties props) {
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

        if (!(cacheCfg instanceof CompleteConfiguration))
            throw new UnsupportedOperationException("Configuration is not supported: " + cacheCfg);

        if (cacheCfg instanceof GridCacheConfiguration) {
            String cfgCacheName = ((GridCacheConfiguration)cacheCfg).getName();

            if (cfgCacheName != null) {
                if (!cacheName.equals(cfgCacheName))
                    throw new IllegalArgumentException();
            }
            else {
                cacheCfg = (C)new GridCacheConfiguration((CompleteConfiguration)cacheCfg);

                ((GridCacheConfiguration)cacheCfg).setName(cacheName);
            }
        }

        IgniteCache<K, V> res;

        synchronized (igniteMap) {
            if (igniteMap.containsKey(cacheName))
                throw new CacheException("Cache already exists [cacheName=" + cacheName + ", manager=" + uri + ']');

            Ignite ignite;

            if (uri.equals(cachingProvider.getDefaultURI())) {
                IgniteConfiguration cfg = new IgniteConfiguration();
                cfg.setGridName("grid-for-" + cacheName);

                cfg.setCacheConfiguration(new GridCacheConfiguration((CompleteConfiguration)cacheCfg));

                cfg.getCacheConfiguration()[0].setName(cacheName);

                try {
                    ignite = Ignition.start(cfg);
                }
                catch (IgniteCheckedException e) {
                    throw new CacheException(e);
                }
            }
            else
                throw new UnsupportedOperationException();

            res = ignite.jcache(cacheName);

            igniteMap.put(cacheName, new T2<>(ignite, new IgniteCacheMXBean(res)));
        }

        if (((CompleteConfiguration)cacheCfg).isManagementEnabled())
            enableManagement(cacheName, true);

        if (((CompleteConfiguration)cacheCfg).isStatisticsEnabled())
            enableStatistics(cacheName, true);

        return res;
    }

    /**
     * @param cacheName Cache name.
     */
    private <K, V> IgniteCache<K, V> findCache(String cacheName) {
        IgniteBiTuple<Ignite, IgniteCacheMXBean> tuple;

        synchronized (igniteMap) {
            tuple = igniteMap.get(cacheName);
        }

        if (tuple == null)
            return null;

        return tuple.get1().jcache(cacheName);
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
            for (IgniteBiTuple<Ignite, IgniteCacheMXBean> tuple : igniteMap.values()) {
                if (ignite.equals(tuple.get1()))
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

        IgniteBiTuple<Ignite, IgniteCacheMXBean> tuple;

        synchronized (igniteMap) {
            tuple = igniteMap.remove(cacheName);
        }

        if (tuple != null) {
            try {
                tuple.get1().close();
            }
            catch (Exception ignored) {

            }

            MBeanServer mBeanSrv = tuple.get1().configuration().getMBeanServer();

            unregisterCacheObject(mBeanSrv, cacheName, true);

            unregisterCacheObject(mBeanSrv, cacheName, false);
        }
    }

    /**
     * @param cacheName Cache name.
     */
    private ObjectName getObjectName(String cacheName, boolean isStatistic) {
        String mbType = isStatistic ? "CacheStatistics" : "CacheConfiguration";

        String mBeanName = "javax.cache:type=" + mbType + ",CacheManager="
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

        IgniteBiTuple<Ignite, IgniteCacheMXBean> tuple;

        synchronized (igniteMap) {
            tuple = igniteMap.get(cacheName);
        }

        MBeanServer mBeanSrv = tuple.get1().configuration().getMBeanServer();

        if (enabled) {
            registerCacheObject(mBeanSrv, tuple.get2(), cacheName, false);

            tuple.get1().cache(cacheName).configuration().setManagementEnabled(true);
        }
        else {
            unregisterCacheObject(mBeanSrv, cacheName, false);

            tuple.get1().cache(cacheName).configuration().setManagementEnabled(false);
        }
    }


    /** {@inheritDoc} */
    @Override public void enableStatistics(String cacheName, boolean enabled) {
        ensureNotClosed();

        if (cacheName == null)
            throw new NullPointerException();

        IgniteBiTuple<Ignite, IgniteCacheMXBean> tuple;

        synchronized (igniteMap) {
            tuple = igniteMap.get(cacheName);
        }

        Ignite ignite = tuple.get1();

        GridCache cache = ignite.cache(cacheName);

        GridCacheConfiguration configuration = ignite.jcache(cacheName).getConfiguration(GridCacheConfiguration.class);

        MBeanServer mBeanSrv = ignite.configuration().getMBeanServer();

        if (enabled) {
            CacheMetricsMXBean mxBean = new CacheMetricsMXBean(cache);

            registerCacheObject(mBeanSrv, mxBean, cacheName, true);

            configuration.setStatisticsEnabled(true);
        }
        else {
            unregisterCacheObject(mBeanSrv, cacheName, true);

            configuration.setStatisticsEnabled(false);
        }
    }

    /**
     * @param mxbean MXBean.
     * @param name cache name.
     */
    public void registerCacheObject(MBeanServer mBeanServer, Object mxbean, String name, boolean isStatistic) {
        ObjectName registeredObjectName = getObjectName(name, isStatistic);

        try {
            if (!isRegistered(mBeanServer, registeredObjectName))
                mBeanServer.registerMBean(mxbean, registeredObjectName);
        }
        catch (Exception e) {
            throw new CacheException("Failed to register MBean: " + registeredObjectName, e);
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
     * @param name cache name.
     * @param stats is mxbean, a statistics mxbean.
     */
    public void unregisterCacheObject(MBeanServer mBeanSrv, String name, boolean stats) {
        Set<ObjectName> registeredObjectNames;

        ObjectName objectName = getObjectName(name, stats);

        registeredObjectNames = mBeanSrv.queryNames(objectName, null);

        //should just be one
        for (ObjectName registeredObjectName : registeredObjectNames) {
            try {
                mBeanSrv.unregisterMBean(registeredObjectName);
            }
            catch (Exception e) {
                throw new CacheException("Failed to unregister MBean: " + registeredObjectName, e);
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
            IgniteBiTuple<Ignite, IgniteCacheMXBean>[] ignites;

            synchronized (igniteMap) {
                ignites = igniteMap.values().toArray(new IgniteBiTuple[igniteMap.values().size()]);
            }

            for (IgniteBiTuple<Ignite, IgniteCacheMXBean> tuple : ignites) {
                try {
                    tuple.get1().close();
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
