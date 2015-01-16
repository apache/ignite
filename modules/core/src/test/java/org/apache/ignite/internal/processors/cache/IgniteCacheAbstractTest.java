/* @java.file.header */

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.apache.ignite.internal.processors.cache;

import org.apache.ignite.*;
import org.apache.ignite.cache.*;
import org.apache.ignite.cache.store.*;
import org.apache.ignite.configuration.*;
import org.apache.ignite.lang.*;
import org.apache.ignite.marshaller.optimized.*;
import org.apache.ignite.spi.discovery.tcp.*;
import org.apache.ignite.spi.discovery.tcp.ipfinder.*;
import org.apache.ignite.spi.discovery.tcp.ipfinder.vm.*;
import org.gridgain.grid.cache.*;
import org.gridgain.testframework.junits.common.*;
import org.jdk8.backport.*;

import javax.cache.*;
import javax.cache.configuration.*;
import javax.cache.integration.*;
import java.util.*;

import static org.gridgain.grid.cache.GridCacheAtomicityMode.*;
import static org.gridgain.grid.cache.GridCacheMode.*;
import static org.gridgain.grid.cache.GridCacheWriteSynchronizationMode.*;

/**
 * Abstract class for cache tests.
 */
public abstract class IgniteCacheAbstractTest extends GridCommonAbstractTest {
    /** */
    private static TcpDiscoveryIpFinder ipFinder = new TcpDiscoveryVmIpFinder(true);

    /** */
    protected static final Map<Object, Object> storeMap = new ConcurrentHashMap8<>();

    /**
     * @return Grids count to start.
     */
    protected abstract int gridCount();

    /** {@inheritDoc} */
    @Override protected void beforeTestsStarted() throws Exception {
        startGrids();
    }

    /**
     * @throws Exception If failed.
     */
    protected void startGrids() throws Exception {
        int cnt = gridCount();

        assert cnt >= 1 : "At least one grid must be started";

        startGridsMultiThreaded(cnt);

        awaitPartitionMapExchange();
    }

    /** {@inheritDoc} */
    @Override protected void afterTestsStopped() throws Exception {
        stopAllGrids();

        storeMap.clear();
    }

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String gridName) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(gridName);

        TcpDiscoverySpi disco = new TcpDiscoverySpi();

        disco.setMaxMissedHeartbeats(Integer.MAX_VALUE);

        disco.setIpFinder(ipFinder);

        if (isDebug())
            disco.setAckTimeout(Integer.MAX_VALUE);

        cfg.setDiscoverySpi(disco);

        cfg.setCacheConfiguration(cacheConfiguration(gridName));

        cfg.setMarshaller(new IgniteOptimizedMarshaller(false));

        return cfg;
    }

    /**
     * @param gridName Grid name.
     * @return Cache configuration.
     * @throws Exception In case of error.
     */
    @SuppressWarnings("unchecked")
    protected CacheConfiguration cacheConfiguration(String gridName) throws Exception {
        CacheConfiguration cfg = defaultCacheConfiguration();

        cfg.setSwapEnabled(swapEnabled());
        cfg.setCacheMode(cacheMode());
        cfg.setAtomicityMode(atomicityMode());

        if (atomicityMode() == ATOMIC && cacheMode() != LOCAL) {
            assert atomicWriteOrderMode() != null;

            cfg.setAtomicWriteOrderMode(atomicWriteOrderMode());
        }

        cfg.setWriteSynchronizationMode(writeSynchronization());
        cfg.setDistributionMode(distributionMode());
        cfg.setPortableEnabled(portableEnabled());

        cfg.setCacheLoaderFactory(loaderFactory());

        cfg.setCacheWriterFactory(writerFactory());

        cfg.setStore(cacheStore());

        if (cacheMode() == PARTITIONED)
            cfg.setBackups(1);

        return cfg;
    }

    /**
     * @return Cache store.
     */
    protected CacheStore<?, ?> cacheStore() {
        return null;
    }

    /**
     * @return Cache loader factory.
     */
    protected Factory<? extends CacheLoader> loaderFactory() {
        return null;
    }

    /**
     * @return Cache writer factory.
     */
    protected Factory<? extends CacheWriter> writerFactory() {
        return null;
    }

    /**
     * @return Default cache mode.
     */
    protected abstract GridCacheMode cacheMode();

    /**
     * @return Cache atomicity mode.
     */
    protected abstract GridCacheAtomicityMode atomicityMode();

    /**
     * @return Atomic cache write order mode.
     */
    protected GridCacheAtomicWriteOrderMode atomicWriteOrderMode() {
        return null;
    }

    /**
     * @return Partitioned mode.
     */
    protected abstract GridCacheDistributionMode distributionMode();

    /**
     * @return Write synchronization.
     */
    protected GridCacheWriteSynchronizationMode writeSynchronization() {
        return FULL_SYNC;
    }

    /**
     * @return Whether portable mode is enabled.
     */
    protected boolean portableEnabled() {
        return false;
    }

    /**
     * @return {@code true} if swap should be enabled.
     */
    protected boolean swapEnabled() {
        return false;
    }

    /**
     * @return Cache.
     */
    protected <K, V> IgniteCache<K, V> jcache() {
        return jcache(0);
    }

    /**
     * @param idx Grid index.
     * @return Cache.
     */
    protected <K, V> IgniteCache<K, V> jcache(int idx) {
        return grid(idx).jcache(null);
    }

    /**
     *
     */
    public class TestStore extends CacheStoreAdapter<Object, Object> {
        /** {@inheritDoc} */
        @Override public void loadCache(IgniteBiInClosure<Object, Object> clo, Object... args) {
            for (Map.Entry<Object, Object> e : storeMap.entrySet())
                clo.apply(e.getKey(), e.getValue());
        }

        /** {@inheritDoc} */
        @Override public Object load(Object key) {
            return storeMap.get(key);
        }

        /** {@inheritDoc} */
        @Override public void write(Cache.Entry<? extends Object, ? extends Object> entry) {
            storeMap.put(entry.getKey(), entry.getValue());
        }

        /** {@inheritDoc} */
        @Override public void delete(Object key) {
            storeMap.remove(key);
        }
    }
}
