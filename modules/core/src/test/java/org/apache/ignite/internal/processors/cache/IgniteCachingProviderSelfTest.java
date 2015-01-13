/* @java.file.header */

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.apache.ignite.internal.processors.cache;

import com.google.common.collect.*;
import org.apache.ignite.*;
import org.apache.ignite.configuration.*;
import org.gridgain.grid.cache.*;

import javax.cache.*;
import javax.cache.spi.*;
import java.util.*;

/**
 *
 */
public class IgniteCachingProviderSelfTest extends IgniteCacheAbstractTest {
    /** {@inheritDoc} */
    @Override protected int gridCount() {
        return 0;
    }

    /** {@inheritDoc} */
    @Override protected GridCacheMode cacheMode() {
        return GridCacheMode.REPLICATED;
    }

    /** {@inheritDoc} */
    @Override protected GridCacheAtomicityMode atomicityMode() {
        return GridCacheAtomicityMode.TRANSACTIONAL;
    }

    /** {@inheritDoc} */
    @Override protected GridCacheDistributionMode distributionMode() {
        return GridCacheDistributionMode.PARTITIONED_ONLY;
    }

    /** {@inheritDoc} */
    @Override public String getTestGridName(int idx) {
        assert idx == 0;

        return null;
    }

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String gridName) throws Exception {
        assert gridName == null;

        IgniteConfiguration cfg = super.getConfiguration(gridName);

        GridCacheConfiguration cache1 = cacheConfiguration(null);
        cache1.setName("cache1");

        GridCacheConfiguration cache2 = cacheConfiguration(null);
        cache2.setName("cache2");

        cfg.setCacheConfiguration(cacheConfiguration(null), cache1, cache2);

        return cfg;
    }

    /** {@inheritDoc} */
    @Override protected void beforeTestsStarted() throws Exception {
        // No-op. Disabling start of ignite.
    }

    /** {@inheritDoc} */
    @Override protected void afterTest() throws Exception {
        stopAllGrids();
    }

    /**
     *
     */
    public void testStartIgnite() {
        CachingProvider cachingProvider = Caching.getCachingProvider();

        assert cachingProvider instanceof IgniteCachingProvider;

        CacheManager cacheMgr = cachingProvider.getCacheManager();

        assertEquals(Collections.<String>emptySet(), Sets.newHashSet(cacheMgr.getCacheNames()));

        Cache<Integer, String> cacheA = cacheMgr.createCache("a", new GridCacheConfiguration());

        cacheA.put(1, "1");

        assertEquals("1", cacheA.get(1));

        cacheMgr.createCache("b", new GridCacheConfiguration());

        assertEquals(Sets.newHashSet("a", "b"), Sets.newHashSet(cacheMgr.getCacheNames()));

        cacheMgr.destroyCache("a");
        cacheMgr.destroyCache("b");

        assertEquals(Collections.<String>emptySet(), Sets.newHashSet(cacheMgr.getCacheNames()));
    }

    /**
     *
     */
    public void testCloseManager() throws Exception {
        startGridsMultiThreaded(1);

        CachingProvider cachingProvider = Caching.getCachingProvider();

        assert cachingProvider instanceof IgniteCachingProvider;

        CacheManager cacheMgr = cachingProvider.getCacheManager();

        cachingProvider.close();

        assertNotSame(cacheMgr, cachingProvider.getCacheManager());
    }
}
