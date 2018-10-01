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

package org.apache.ignite.internal.processors.cache;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.Callable;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.cache.CacheAtomicityMode;
import org.apache.ignite.cache.CacheServerNotFoundException;
import org.apache.ignite.cluster.ClusterNode;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.IgniteInternalFuture;
import org.apache.ignite.internal.IgniteKernal;
import org.apache.ignite.internal.IgniteNodeAttributes;
import org.apache.ignite.internal.TestRecordingCommunicationSpi;
import org.apache.ignite.internal.processors.affinity.AffinityTopologyVersion;
import org.apache.ignite.internal.processors.cache.distributed.dht.GridDhtAffinityAssignmentResponse;
import org.apache.ignite.internal.processors.cache.distributed.dht.topology.GridDhtPartitionTopology;
import org.apache.ignite.internal.processors.cache.distributed.dht.preloader.GridDhtPartitionsFullMessage;
import org.apache.ignite.internal.util.lang.GridAbsPredicate;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.lang.IgniteBiPredicate;
import org.apache.ignite.lang.IgnitePredicate;
import org.apache.ignite.plugin.extensions.communication.Message;
import org.apache.ignite.spi.discovery.tcp.TcpDiscoverySpi;
import org.apache.ignite.spi.discovery.tcp.ipfinder.TcpDiscoveryIpFinder;
import org.apache.ignite.spi.discovery.tcp.ipfinder.vm.TcpDiscoveryVmIpFinder;
import org.apache.ignite.testframework.GridTestUtils;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;

import static org.apache.ignite.cache.CacheAtomicityMode.ATOMIC;
import static org.apache.ignite.cache.CacheAtomicityMode.TRANSACTIONAL;
import static org.apache.ignite.cache.CacheWriteSynchronizationMode.FULL_SYNC;

/**
 *
 */
public class IgniteClientCacheStartFailoverTest extends GridCommonAbstractTest {
    /** */
    protected static TcpDiscoveryIpFinder ipFinder = new TcpDiscoveryVmIpFinder(true);

    /** */
    private boolean client;

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String igniteInstanceName) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(igniteInstanceName);

        ((TcpDiscoverySpi)cfg.getDiscoverySpi()).setIpFinder(ipFinder);

        cfg.setClientMode(client);

        cfg.setCommunicationSpi(new TestRecordingCommunicationSpi());

        return cfg;
    }

    /** {@inheritDoc} */
    @Override protected void afterTest() throws Exception {
        stopAllGrids();

        super.afterTest();
    }

    /**
     * @throws Exception If failed.
     */
    public void testClientStartCoordinatorFailsAtomic() throws Exception {
        clientStartCoordinatorFails(ATOMIC);
    }

    /**
     * @throws Exception If failed.
     */
    public void testClientStartCoordinatorFailsTx() throws Exception {
        clientStartCoordinatorFails(TRANSACTIONAL);
    }

    /**
     * @param atomicityMode Cache atomicity mode.
     * @throws Exception If failed.
     */
    private void clientStartCoordinatorFails(CacheAtomicityMode atomicityMode) throws Exception {
        Ignite srv0 = startGrids(3);

        final int KEYS = 500;

        IgniteCache<Object, Object> cache = srv0.createCache(cacheConfiguration(DEFAULT_CACHE_NAME, atomicityMode, 1));

        for (int i = 0; i < KEYS; i++)
            cache.put(i, i);

        client = true;

        final Ignite c = startGrid(3);

        TestRecordingCommunicationSpi.spi(srv0).blockMessages(GridDhtAffinityAssignmentResponse.class, c.name());

        IgniteInternalFuture<?> fut = GridTestUtils.runAsync(new Callable<Void>() {
            @Override public Void call() throws Exception {
                c.cache(DEFAULT_CACHE_NAME);

                return null;
            }
        }, "start-cache");

        U.sleep(1000);

        assertFalse(fut.isDone());

        stopGrid(0);

        fut.get();

        cache = c.cache(DEFAULT_CACHE_NAME);

        for (int i = 0; i < KEYS; i++) {
            assertEquals(i, cache.get(i));

            cache.put(i, i + 1);

            assertEquals(i + 1, cache.get(i));
        }
    }

    /**
     * @throws Exception If failed.
     */
    public void testClientStartLastServerFailsAtomic() throws Exception {
        clientStartLastServerFails(ATOMIC);
    }

    /**
     * @throws Exception If failed.
     */
    public void testClientStartLastServerFailsTx() throws Exception {
        clientStartLastServerFails(TRANSACTIONAL);
    }

    /**
     * @param atomicityMode Cache atomicity mode.
     * @throws Exception If failed.
     */
    private void clientStartLastServerFails(CacheAtomicityMode atomicityMode) throws Exception {
        startGrids(3);

        CacheConfiguration<Object, Object> cfg = cacheConfiguration(DEFAULT_CACHE_NAME, atomicityMode, 1);

        cfg.setNodeFilter(new TestNodeFilter(getTestIgniteInstanceName(1)));

        Ignite srv1 = ignite(1);

        srv1.createCache(cfg);

        client = true;

        final Ignite c = startGrid(3);

        client = false;

        TestRecordingCommunicationSpi.spi(srv1).blockMessages(GridDhtAffinityAssignmentResponse.class, c.name());

        IgniteInternalFuture<?> fut = GridTestUtils.runAsync(new Callable<Void>() {
            @Override public Void call() throws Exception {
                c.cache(DEFAULT_CACHE_NAME);

                return null;
            }
        }, "start-cache");

        U.sleep(1000);

        assertFalse(fut.isDone());

        stopGrid(1);

        fut.get();

        final IgniteCache<Object, Object> clientCache = c.cache(DEFAULT_CACHE_NAME);

        for (int i = 0; i < 10; i++) {
            final int k = i;

            GridTestUtils.assertThrows(log, new Callable<Void>() {
                @Override public Void call() throws Exception {
                    clientCache.get(k);

                    return null;
                }
            }, CacheServerNotFoundException.class, null);

            GridTestUtils.assertThrows(log, new Callable<Void>() {
                @Override public Void call() throws Exception {
                    clientCache.put(k, k);

                    return null;
                }
            }, CacheServerNotFoundException.class, null);

            GridTestUtils.assertThrows(log, new Callable<Void>() {
                @Override public Void call() throws Exception {
                    clientCache.remove(k);

                    return null;
                }
            }, CacheServerNotFoundException.class, null);
        }

        startGrid(1);

        awaitPartitionMapExchange();

        for (int i = 0; i < 100; i++) {
            assertNull(clientCache.get(i));

            clientCache.put(i, i);

            assertEquals(i, clientCache.get(i));
        }
    }

    /**
     * @throws Exception If failed.
     */
    public void testRebalanceState() throws Exception {
        final int SRVS = 3;

        startGrids(SRVS);

        List<String> cacheNames = startCaches(ignite(0), 100);

        client = true;

        Ignite c = startGrid(SRVS);

        assertTrue(c.configuration().isClientMode());

        awaitPartitionMapExchange();

        client = false;

        TestRecordingCommunicationSpi.spi(ignite(0)).blockMessages(new IgniteBiPredicate<ClusterNode, Message>() {
            @Override public boolean apply(ClusterNode clusterNode, Message msg) {
                return msg instanceof GridDhtPartitionsFullMessage &&
                    ((GridDhtPartitionsFullMessage) msg).exchangeId() == null;
            }
        });

        startGrid(SRVS + 1);

        for (String cacheName : cacheNames)
            c.cache(cacheName);

        U.sleep(1000);

        for (int i = 0; i < SRVS + 1; i++) {
            AffinityTopologyVersion topVer = new AffinityTopologyVersion(SRVS + 2);

            IgniteKernal node = (IgniteKernal)ignite(i);

            for (String cacheName : cacheNames) {
                GridDhtPartitionTopology top = node.context().cache().internalCache(cacheName).context().topology();

                waitForReadyTopology(top, topVer);

                assertEquals(topVer, top.readyTopologyVersion());

                assertFalse(top.rebalanceFinished(topVer));
            }
        }

        TestRecordingCommunicationSpi.spi(ignite(0)).stopBlock();

        for (int i = 0; i < SRVS + 1; i++) {
            final AffinityTopologyVersion topVer = new AffinityTopologyVersion(SRVS + 2, 1);

            final IgniteKernal node = (IgniteKernal)ignite(i);

            for (String cacheName : cacheNames) {
                final GridDhtPartitionTopology top = node.context().cache().internalCache(cacheName).context().topology();

                GridTestUtils.waitForCondition(new GridAbsPredicate() {
                    @Override public boolean apply() {
                        return top.rebalanceFinished(topVer);
                    }
                }, 5000);

                assertTrue(top.rebalanceFinished(topVer));
            }
        }
    }

    /**
     * @throws Exception If failed.
     */
    public void testRebalanceStateConcurrentStart() throws Exception {
        final int SRVS1 = 3;
        final int CLIENTS = 5;
        final int SRVS2 = 5;

        startGrids(SRVS1);

        Ignite srv0 = ignite(0);

        final int KEYS = 1000;

        final List<String> cacheNames = startCaches(srv0, KEYS);

        client = true;

        final List<Ignite> clients = new ArrayList<>();

        for (int i = 0; i < CLIENTS; i++)
            clients.add(startGrid(SRVS1 + i));

        client = false;

        final CyclicBarrier barrier = new CyclicBarrier(clients.size() + SRVS2);

        final AtomicInteger clientIdx = new AtomicInteger();

        final Set<Integer> keys = new HashSet<>();

        for (int i = 0; i < KEYS; i++)
            keys.add(i);

        IgniteInternalFuture<?> fut1 = GridTestUtils.runMultiThreadedAsync(new Callable<Void>() {
            @Override public Void call() throws Exception {
                barrier.await();

                Ignite client = clients.get(clientIdx.getAndIncrement());

                for (String cacheName : cacheNames)
                    client.cache(cacheName);

                ThreadLocalRandom rnd = ThreadLocalRandom.current();

                for (int i = 0; i < 10; i++) {
                    for (String cacheName : cacheNames) {
                        IgniteCache<Object, Object> cache = client.cache(cacheName);

                        Map<Object, Object> map0 = cache.getAll(keys);

                        assertEquals(KEYS, map0.size());

                        cache.put(rnd.nextInt(KEYS), i);
                    }
                }

                return null;
            }
        }, clients.size(), "client-cache-start");

        final AtomicInteger srvIdx = new AtomicInteger(SRVS1 + CLIENTS);

        IgniteInternalFuture<?> fut2 = GridTestUtils.runMultiThreadedAsync(new Callable<Void>() {
            @Override public Void call() throws Exception {
                barrier.await();

                startGrid(srvIdx.incrementAndGet());

                return null;
            }
        }, SRVS2, "node-start");

        fut1.get();
        fut2.get();

        final AffinityTopologyVersion topVer = new AffinityTopologyVersion(SRVS1 + SRVS2 + CLIENTS, 1);

        for (Ignite client : clients) {
            for (String cacheName : cacheNames) {
                final GridDhtPartitionTopology top =
                    ((IgniteKernal)client).context().cache().internalCache(cacheName).context().topology();

                GridTestUtils.waitForCondition(new GridAbsPredicate() {
                    @Override public boolean apply() {
                        return top.rebalanceFinished(topVer);
                    }
                }, 5000);

                assertTrue(top.rebalanceFinished(topVer));
            }
        }
    }

    /**
     * @throws Exception If failed.
     */
    public void testClientStartCloseServersRestart() throws Exception {
        final int SRVS = 4;
        final int CLIENTS = 4;

        startGrids(SRVS);

        final List<String> cacheNames = startCaches(ignite(0), 1000);

        client = true;

        final List<Ignite> clients = new ArrayList<>();

        for (int i = 0; i < CLIENTS; i++)
            clients.add(startGrid(SRVS + i));

        client = false;

        final AtomicBoolean stop = new AtomicBoolean();

        IgniteInternalFuture<?> restartFut = GridTestUtils.runAsync(new Callable<Void>() {
            @Override public Void call() throws Exception {
                ThreadLocalRandom rnd = ThreadLocalRandom.current();

                while (!stop.get()) {
                    int nodeIdx = rnd.nextInt(SRVS);

                    stopGrid(nodeIdx);

                    U.sleep(rnd.nextLong(200) + 1);

                    startGrid(nodeIdx);

                    U.sleep(rnd.nextLong(200) + 1);
                }

                return null;
            }
        }, "restart");

        final AtomicInteger clientIdx = new AtomicInteger();

        IgniteInternalFuture<?> clientsFut = GridTestUtils.runMultiThreadedAsync(new Callable<Void>() {
            @Override public Void call() throws Exception {
                Ignite client = clients.get(clientIdx.getAndIncrement());

                assertTrue(client.configuration().isClientMode());

                ThreadLocalRandom rnd = ThreadLocalRandom.current();

                while (!stop.get()) {
                    for (String cacheName : cacheNames)
                        client.cache(cacheName);

                    for (String cacheName : cacheNames) {
                        IgniteCache<Object, Object> cache = client.cache(cacheName);

                        cache.put(rnd.nextInt(1000), rnd.nextInt());

                        cache.get(rnd.nextInt(1000));
                    }

                    for (String cacheName : cacheNames) {
                        IgniteCache<Object, Object> cache = client.cache(cacheName);

                        cache.close();
                    }
                }

                return null;
            }
        }, CLIENTS, "client-thread");

        try {
            U.sleep(10_000);

            stop.set(true);

            restartFut.get();
            clientsFut.get();
        }
        finally {
            stop.set(true);
        }

        ThreadLocalRandom rnd = ThreadLocalRandom.current();

        for (Ignite client : clients) {
            for (String cacheName : cacheNames) {
                IgniteCache<Object, Object> cache = client.cache(cacheName);

                for (int i = 0; i < 10; i++) {
                    Integer key = rnd.nextInt(1000);

                    cache.put(key, i);

                    assertEquals(i, cache.get(key));
                }
            }
        }
    }

    /**
     * @param node Node.
     * @param keys Number of keys to put in caches.
     * @return Cache names.
     */
    private List<String> startCaches(Ignite node, int keys) {
        List<String> cacheNames = new ArrayList<>();

        final Map<Integer, Integer> map = new HashMap<>();

        for (int i = 0; i < keys; i++)
            map.put(i, i);

        for (int i = 0; i < 3; i++) {
            CacheConfiguration<Object, Object> ccfg = cacheConfiguration("atomic-" + i, ATOMIC, i);

            IgniteCache<Object, Object> cache = node.createCache(ccfg);

            cacheNames.add(ccfg.getName());

            cache.putAll(map);
        }

        for (int i = 0; i < 3; i++) {
            CacheConfiguration<Object, Object> ccfg = cacheConfiguration("tx-" + i, TRANSACTIONAL, i);

            IgniteCache<Object, Object> cache = node.createCache(ccfg);

            cacheNames.add(ccfg.getName());

            cache.putAll(map);
        }

        return cacheNames;
    }

    /**
     * @param name Cache name.
     * @param atomicityMode Cache atomicity mode.
     * @param backups Number of backups.
     * @return Cache configuration.
     */
    private CacheConfiguration<Object, Object> cacheConfiguration(String name, CacheAtomicityMode atomicityMode, int backups) {
        CacheConfiguration<Object, Object> ccfg = new CacheConfiguration<>(name);

        ccfg.setWriteSynchronizationMode(FULL_SYNC);
        ccfg.setAtomicityMode(atomicityMode);
        ccfg.setBackups(backups);

        return ccfg;
    }
    /**
     *
     */
    private static class TestNodeFilter implements IgnitePredicate<ClusterNode> {
        /** */
        private final String includeName;

        /**
         * @param includeName Node to include.
         */
        public TestNodeFilter(String includeName) {
            this.includeName = includeName;
        }

        /** {@inheritDoc} */
        @Override public boolean apply(ClusterNode node) {
            return includeName.equals(node.attribute(IgniteNodeAttributes.ATTR_IGNITE_INSTANCE_NAME));
        }
    }
}
