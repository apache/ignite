/* @java.file.header */

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.gridgain.grid.kernal.processors.cache;

import org.apache.ignite.configuration.*;
import org.gridgain.grid.cache.*;
import org.gridgain.grid.cache.store.*;
import org.gridgain.grid.spi.discovery.tcp.*;
import org.gridgain.grid.spi.discovery.tcp.ipfinder.vm.*;
import org.gridgain.testframework.junits.common.*;
import org.jetbrains.annotations.*;

import java.util.concurrent.*;
import java.util.concurrent.atomic.*;

import static org.gridgain.grid.cache.GridCacheWriteSynchronizationMode.*;

/**
 * Basic store test.
 */
public abstract class GridCacheBasicStoreMultithreadedAbstractTest extends GridCommonAbstractTest {
    /** Cache store. */
    private GridCacheStore<Integer, Integer> store;

    /**
     *
     */
    protected GridCacheBasicStoreMultithreadedAbstractTest() {
        super(false /*start grid. */);
    }

    /** {@inheritDoc} */
    @Override protected void afterTest() throws Exception {
        GridCache<?, ?> cache = cache();

        if (cache != null)
            cache.clearAll();

        stopAllGrids();
    }

    /**
     * @return Caching mode.
     */
    protected abstract GridCacheMode cacheMode();

    /** {@inheritDoc} */
    @Override protected final IgniteConfiguration getConfiguration(String gridName) throws Exception {
        IgniteConfiguration c = super.getConfiguration(gridName);

        TcpDiscoverySpi disco = new TcpDiscoverySpi();

        disco.setIpFinder(new TcpDiscoveryVmIpFinder(true));

        c.setDiscoverySpi(disco);

        GridCacheConfiguration cc = defaultCacheConfiguration();

        cc.setCacheMode(cacheMode());
        cc.setWriteSynchronizationMode(FULL_SYNC);
        cc.setSwapEnabled(false);

        cc.setStore(store);

        c.setCacheConfiguration(cc);

        return c;
    }

    /**
     * @throws Exception If failed.
     */
    public void testConcurrentGet() throws Exception {
        final AtomicInteger cntr = new AtomicInteger();

        store = new GridCacheStoreAdapter<Integer, Integer>() {
            @Override public Integer load(@Nullable GridCacheTx tx, Integer key) {
                return cntr.incrementAndGet();
            }

            /** {@inheritDoc} */
            @Override public void put(GridCacheTx tx, Integer key, @Nullable Integer val) {
                assert false;
            }

            /** {@inheritDoc} */
            @Override public void remove(GridCacheTx tx, Integer key) {
                assert false;
            }
        };

        startGrid();

        final GridCache<Integer, Integer> cache = cache();

        int threads = 2;

        final CyclicBarrier barrier = new CyclicBarrier(threads);

        multithreaded(new Callable<Object>() {
            @Override public Object call() throws Exception {
                barrier.await();

                cache.get(1);

                return null;
            }
        }, threads, "concurrent-get-worker");

        assertEquals(1, cntr.get());
    }
}
