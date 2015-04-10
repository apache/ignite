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
import org.apache.ignite.internal.*;
import org.apache.ignite.spi.discovery.tcp.*;
import org.apache.ignite.spi.discovery.tcp.ipfinder.vm.*;
import java.net.*;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.*;

import static org.apache.ignite.cache.CacheMode.*;

/**
 * Tests TcpClientDiscovery SPI with multiple client nodes that interact with a cache concurrently.
 */
public class GridCacheTcpClientDiscoveryMultiThreadedTest extends GridCacheAbstractSelfTest {
    /** Server nodes count. */
    private volatile static int serverNodesCount;

    /** Client nodes count. */
    private volatile static int clientNodesCount;

    /** Client node or not. */
    private volatile static boolean client;

    /** {@inheritDoc} */
    @Override protected int gridCount() {
        return serverNodesCount + clientNodesCount;
    }
    
    /** {@inheritDoc} */
    @Override protected void beforeTestsStarted() throws Exception {
        // No-op.
    }

    /** {@inheritDoc} */
    @Override protected void beforeTest() throws Exception {
        // No-op.
    }

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String gridName) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(gridName);

        // Filling configuration for client nodes
        if (client) {
            TcpDiscoveryVmIpFinder clientFinder = new TcpDiscoveryVmIpFinder();
            ArrayList<String> addresses = new ArrayList<>(ipFinder.getRegisteredAddresses().size());

            for (InetSocketAddress sockAddr : ipFinder.getRegisteredAddresses()) {
                addresses.add(sockAddr.getHostString() + ":" + sockAddr.getPort());
            }

            clientFinder.setAddresses(addresses);

            TcpClientDiscoverySpi discoverySpi = new TcpClientDiscoverySpi();
            discoverySpi.setIpFinder(clientFinder);

            cfg.setDiscoverySpi(discoverySpi);
            cfg.setClientMode(true);
        }

        cfg.setLocalHost("127.0.0.1");

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
    @Override protected CacheMode cacheMode() {
        return PARTITIONED;
    }

    /**
     * @throws Exception If failed.
     */
    public void testCacheConcurrentlyWithMultipleClientNodes() throws Exception {
        try {
            serverNodesCount = 3;
            clientNodesCount = 4;

            startServerNodes();

            client = true;
            startGridsMultiThreaded(serverNodesCount, clientNodesCount);

            checkTopology(gridCount());
            awaitPartitionMapExchange();

            // Explicitly create near cache for even client nodes
            final boolean[] nearCacheNode = new boolean[clientNodesCount];
            for (int i = serverNodesCount; i < gridCount(); i++) {
                if (i % 2 == 0) {
                    grid(i).createNearCache(null, new NearCacheConfiguration<>());
                    nearCacheNode[i - serverNodesCount] = true;
                }
            }

            super.beforeTest();

            final AtomicInteger threadsCnt = new AtomicInteger();

            IgniteInternalFuture<?> f = multithreadedAsync(
                    new Callable<Object>() {
                        @Override
                        public Object call() throws Exception {
                            int clientIdx = serverNodesCount + threadsCnt.getAndIncrement();
                            Ignite node = grid(clientIdx);

                            assert node.configuration().isClientMode();

                            IgniteCache<Integer, Integer> cache = node.cache(null);
                            boolean isNearCacheNode = nearCacheNode[clientIdx - serverNodesCount];

                            for (int i = 100 * clientIdx; i < 100 * (clientIdx + 1); i++) {
                                cache.put(i, i);
                            }

                            for (int i = 100 * clientIdx; i < 100 * (clientIdx + 1); i++) {
                                assertEquals(i, (int) cache.get(i));

                                if (isNearCacheNode)
                                    assertEquals(i, (int) cache.localPeek(i, CachePeekMode.ONHEAP));
                            }

                            stopGrid(clientIdx);

                            return null;
                        }
                    },
                    clientNodesCount
            );

            f.get();

        }
        finally {
            afterTestsStopped();
        }
    }

    /**
     * @throws Exception If failed.
     */
    public void testCacheWithServerNodesRestart() throws Exception {
        try {
            serverNodesCount = 3;
            clientNodesCount = 1;

            startServerNodes();

            client = true;
            Ignite client = startGrid(serverNodesCount);

            checkTopology(gridCount());
            awaitPartitionMapExchange();
            super.beforeTest();

            IgniteCache<Integer, Integer> cache = client.cache(null);

            performSimpleOperationsOnCache(cache);

            // Restart server nodes, client node should reconnect automatically.
            stopServerNodes();
            startServerNodes();
            checkTopology(gridCount());
            awaitPartitionMapExchange();
            super.beforeTest();

            performSimpleOperationsOnCache(cache);
        }
        finally {
            afterTestsStopped();
        }
    }

    /**
     * @throws Exception If failed.
     */
    private void startServerNodes() throws Exception {
        client = false;
        for (int i = 0; i < serverNodesCount; i++)
            startGrid(i);
    }

    /**
     * @throws Exception
     */
    private void stopServerNodes() throws Exception {
        for (int i = 0; i < serverNodesCount; i++)
            stopGrid(i);
    }

    /**
     * Executes simple operation on the cache.
     *
     * @param cache Cache instance to use.
     */
    private void performSimpleOperationsOnCache(IgniteCache<Integer, Integer> cache) {
        for (int i = 100; i < 200; i++) {
            cache.put(i, i);
        }

        for (int i = 100; i < 200; i++) {
            assertEquals(i, (int) cache.get(i));
        }
    }
}
