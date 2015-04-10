/*
 *  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.apache.ignite.internal.processors.cache;

import org.apache.ignite.*;
import org.apache.ignite.cache.*;
import org.apache.ignite.cache.affinity.rendezvous.*;
import org.apache.ignite.configuration.*;
import org.apache.ignite.internal.IgniteInternalFuture;
import org.apache.ignite.spi.discovery.tcp.*;

import java.util.Arrays;
import java.util.concurrent.Callable;
import java.util.concurrent.atomic.*;

import static org.apache.ignite.cache.CacheMode.*;

/**
 * Tests TcpClientDiscovery SPI with multiple client nodes that interact with a cache concurrently.
 */
public class GridCacheTcpClientDiscoveryMultiThreadedTest extends GridCacheAbstractSelfTest {
    /** Server nodes count. */
    private final static int SERVER_NODES_COUNT = 3;

    /** Client nodes count. */
    private final static int CLIENT_NODES_COUNT = 3;

    /** Array to hold info on whether a client node has a near cache or not */
    private static boolean[] nearCacheNode;

    /** Grids counter. */
    private static AtomicInteger gridsCnt;

    /** {@inheritDoc} */
    @Override protected int gridCount() {
        return SERVER_NODES_COUNT + CLIENT_NODES_COUNT;
    }
    
    /** {@inheritDoc} */
    @Override protected void beforeTestsStarted() throws Exception {
        gridsCnt = new AtomicInteger();
        nearCacheNode = new boolean[CLIENT_NODES_COUNT];

        for (int i = 0; i < SERVER_NODES_COUNT; i++)
            startGrid(i);

        startGridsMultiThreaded(SERVER_NODES_COUNT, CLIENT_NODES_COUNT);

        checkTopology(gridCount());
        awaitPartitionMapExchange();

        // Explicitly create near cache for even client nodes
        for (int i = SERVER_NODES_COUNT; i < gridCount(); i++) {
            if (i % 2 == 0) {
                grid(i).createNearCache(null, new NearCacheConfiguration<>());
                nearCacheNode[i - SERVER_NODES_COUNT] = true;
            }
        }
    }

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String gridName) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(gridName);
        int nodesCount = gridsCnt.incrementAndGet();

        // Filling configuration for client nodes
        if (nodesCount > SERVER_NODES_COUNT) {
            TcpClientDiscoverySpi disco = new TcpClientDiscoverySpi();
            disco.setIpFinder(ipFinder);

            cfg.setClientMode(true);
            //cfg.setDiscoverySpi(disco);
        }

        return cfg;
    }

    /** {@inheritDoc} */
    @SuppressWarnings("unchecked")
    @Override protected CacheConfiguration cacheConfiguration(String gridName) throws Exception {
        CacheConfiguration cfg = super.cacheConfiguration(gridName);

        cfg.setCacheStoreFactory(null);
        cfg.setReadThrough(false);
        cfg.setWriteThrough(false);
        cfg.setAffinity(new RendezvousAffinityFunction(false, 32));
        cfg.setBackups(1);

        return cfg;
    }

    /** {@inheritDoc} */
    @Override protected void afterTestsStopped() throws Exception {
        super.afterTestsStopped();

        gridsCnt.set(0);
        Arrays.fill(nearCacheNode, false);
    }

    /** {@inheritDoc} */
    @Override protected CacheMode cacheMode() {
        return PARTITIONED;
    }


    public void testCacheConcurrentlyWithMultipleClientNodes() throws Exception {
        AtomicInteger threadsCnt = new AtomicInteger();

        IgniteInternalFuture<?> f = multithreadedAsync(
                new Callable<Object>() {
                    @Override public Object call() throws Exception {
                        int clientIdx = SERVER_NODES_COUNT + threadsCnt.getAndIncrement();
                        Ignite node = grid(clientIdx);

                        assert node.configuration().isClientMode();

                        IgniteCache<Integer, Integer> cache = node.cache(null);
                        boolean isNearCacheNode = nearCacheNode[clientIdx - SERVER_NODES_COUNT];

                        for (int i = 100 * clientIdx; i < 100 * (clientIdx + 1); i++) {
                            cache.put(i, i);
                        }

                        for (int i = 100 * clientIdx; i < 100 * (clientIdx + 1); i++) {
                            assertEquals(i, (int)cache.get(i));

                            if (isNearCacheNode)
                                assertEquals(i, (int) cache.localPeek(i, CachePeekMode.ONHEAP));
                        }

                        //stopGrid(clientIdx);

                        return null;
                    }
                },
                CLIENT_NODES_COUNT
        );

        f.get();
    }

}
