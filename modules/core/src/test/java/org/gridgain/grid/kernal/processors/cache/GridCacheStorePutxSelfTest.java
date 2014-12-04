/* @java.file.header */

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.gridgain.grid.kernal.processors.cache;

import org.apache.ignite.configuration.*;
import org.apache.ignite.lang.*;
import org.gridgain.grid.*;
import org.gridgain.grid.cache.*;
import org.gridgain.grid.cache.store.*;
import org.gridgain.grid.lang.*;
import org.gridgain.grid.spi.discovery.tcp.*;
import org.gridgain.grid.spi.discovery.tcp.ipfinder.*;
import org.gridgain.grid.spi.discovery.tcp.ipfinder.vm.*;
import org.gridgain.testframework.junits.common.*;
import org.jetbrains.annotations.*;

import java.util.*;
import java.util.concurrent.atomic.*;

import static org.gridgain.grid.cache.GridCacheAtomicityMode.*;
import static org.gridgain.grid.cache.GridCacheMode.*;
import static org.gridgain.grid.cache.GridCacheWriteSynchronizationMode.*;

/**
 * Tests for reproduce problem with GG-6895:
 * putx calls CacheStore.load() when null GridPredicate passed in to avoid IDE warnings
 */
public class GridCacheStorePutxSelfTest extends GridCommonAbstractTest {
    /** */
    private static final GridTcpDiscoveryIpFinder IP_FINDER = new GridTcpDiscoveryVmIpFinder(true);

    /** */
    private static AtomicInteger loads;

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String gridName) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(gridName);

        GridCacheConfiguration cache = new GridCacheConfiguration();

        cache.setCacheMode(PARTITIONED);
        cache.setAtomicityMode(TRANSACTIONAL);
        cache.setWriteSynchronizationMode(FULL_SYNC);
        cache.setStore(new TestStore());

        cfg.setCacheConfiguration(cache);

        GridTcpDiscoverySpi disco = new GridTcpDiscoverySpi();

        disco.setIpFinder(IP_FINDER);

        cfg.setDiscoverySpi(disco);

        return cfg;
    }

    /** {@inheritDoc} */
    @Override protected void beforeTest() throws Exception {
        loads = new AtomicInteger();

        startGrid();
    }

    /** {@inheritDoc} */
    @Override protected void afterTest() throws Exception {
        stopGrid();
    }

    /**
     * @throws Exception If failed.
     */
    public void testPutxShouldNotTriggerLoad() throws Exception {
        assertTrue(cache().putx(1, 1));
        assertTrue(cache().putx(2, 2, (IgnitePredicate)null));

        assertEquals(0, loads.get());
    }

    /**
     * @throws Exception If failed.
     */
    public void testPutxShouldNotTriggerLoadWithTx() throws Exception {
        GridCache<Integer, Integer> cache = cache();

        try (GridCacheTx tx = cache.txStart()) {
            assertTrue(cache.putx(1, 1));
            assertTrue(cache.putx(2, 2, (IgnitePredicate)null));

            tx.commit();
        }

        assertEquals(0, loads.get());
    }

    /** */
    private static class TestStore implements GridCacheStore<Integer, Integer> {
        /** {@inheritDoc} */
        @Nullable @Override public Integer load(@Nullable GridCacheTx tx, Integer key) throws GridException {
            loads.incrementAndGet();

            return null;
        }

        /** {@inheritDoc} */
        @Override public void loadCache(IgniteBiInClosure<Integer, Integer> clo, @Nullable Object... args)
            throws GridException {
            // No-op.
        }

        /** {@inheritDoc} */
        @Override public void loadAll(@Nullable GridCacheTx tx, Collection<? extends Integer> keys,
            IgniteBiInClosure<Integer, Integer> c) throws GridException {
            // No-op.
        }

        /** {@inheritDoc} */
        @Override public void put(@Nullable GridCacheTx tx, Integer key,
            @Nullable Integer val) throws GridException {
            // No-op.
        }

        /** {@inheritDoc} */
        @Override public void putAll(@Nullable GridCacheTx tx,
            Map<? extends Integer, ? extends Integer> map) throws GridException {
            // No-op.
        }

        /** {@inheritDoc} */
        @Override public void remove(@Nullable GridCacheTx tx, Integer key)
            throws GridException {
            // No-op.
        }

        /** {@inheritDoc} */
        @Override public void removeAll(@Nullable GridCacheTx tx, Collection<? extends Integer> keys)
            throws GridException {
            // No-op.
        }

        /** {@inheritDoc} */
        @Override public void txEnd(GridCacheTx tx, boolean commit) throws GridException {
            // No-op.
        }
    }
}
