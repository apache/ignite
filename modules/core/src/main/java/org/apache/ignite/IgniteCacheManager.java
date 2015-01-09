/* @java.file.header */

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.apache.ignite;

import org.apache.ignite.configuration.*;
import org.gridgain.grid.cache.*;

import javax.cache.*;
import javax.cache.configuration.*;
import javax.cache.spi.*;
import java.net.*;
import java.util.*;
import java.util.concurrent.atomic.*;

/**
 *
 */
public class IgniteCacheManager implements CacheManager {
    /** */
    private final Map<String, Ignite> igniteMap = new HashMap<>();

    /** */
    private final URI uri;

    /** */
    private final CachingProvider cachingProvider;

    /** */
    private final ClassLoader clsLdr;

    /** */
    private final AtomicBoolean closed = new AtomicBoolean();

    /**
     * @param uri Uri.
     * @param cachingProvider Caching provider.
     * @param clsLdr Class loader.
     */
    public IgniteCacheManager(URI uri, CachingProvider cachingProvider, ClassLoader clsLdr) {
        this.uri = uri;
        this.cachingProvider = cachingProvider;
        this.clsLdr = clsLdr;
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
        return null;
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

            if (cfgCacheName != null && !cacheName.equals(cfgCacheName))
                throw new IllegalArgumentException();

            cacheCfg = (C)new GridCacheConfiguration((CompleteConfiguration)cacheCfg);

            ((GridCacheConfiguration)cacheCfg).setName(cacheName);
        }

        Ignite ignite;

        synchronized (igniteMap) {
            if (igniteMap.containsKey(cacheName))
                throw new CacheException("Cache already exists [cacheName=" + cacheName + ", manager=" + uri + ']');

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

            igniteMap.put(cacheName, ignite);
        }

        return ignite.jcache(cacheName);
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
        ensureNotClosed();

        String[] resArr;

        synchronized (igniteMap) {
            resArr = igniteMap.keySet().toArray(new String[igniteMap.keySet().size()]);
        }

        return Arrays.asList(resArr);
    }

    /**
     * @param ignite Ignite.
     */
    public boolean isManagedIgnite(Ignite ignite) {
        synchronized (igniteMap) {
            return igniteMap.values().contains(ignite);
        }
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

            }
        }
    }

    /** {@inheritDoc} */
    @Override public void enableManagement(String cacheName, boolean enabled) {
        if (cacheName == null)
            throw new NullPointerException();

        throw new UnsupportedOperationException();
    }

    /** {@inheritDoc} */
    @Override public void enableStatistics(String cacheName, boolean enabled) {
        if (cacheName == null)
            throw new NullPointerException();

        throw new UnsupportedOperationException();
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

//    /**
//     *
//     */
//    private static class Future<T> {
//        /** */
//        private volatile T res;
//
//        /** */
//        private volatile Throwable e;
//
//        public T get() throws CacheException {
//            if (res == null && e == null) {
//                synchronized (this) {
//                    try {
//                        while (res == null && e == null)
//                            wait();
//                    }
//                    catch (InterruptedException e) {
//                        Thread.currentThread().interrupt();
//
//                        throw new RuntimeException(e);
//                    }
//                }
//            }
//
//            if (res != null)
//                return res;
//
//            assert e != null;
//
//            throw new CacheException(e);
//        }
//
//        public synchronized void setException(Throwable e) {
//            this.e = e;
//
//            notifyAll();
//        }
//
//        public synchronized void setCacheManager(T res) {
//            assert res != null;
//
//            this.res = res;
//
//            notifyAll();
//        }
//    }
}
