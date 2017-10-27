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

package org.apache.ignite.internal.processors.cache.distributed.dht;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.Callable;
import java.util.concurrent.CountDownLatch;
import javax.cache.Cache;
import javax.cache.configuration.Factory;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.IgniteException;
import org.apache.ignite.cache.store.CacheStore;
import org.apache.ignite.cache.store.CacheStoreAdapter;
import org.apache.ignite.cluster.ClusterNode;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.internal.IgniteKernal;
import org.apache.ignite.internal.processors.cache.GridCacheAbstractSelfTest;
import org.apache.ignite.internal.processors.cache.GridCacheAdapter;
import org.apache.ignite.internal.processors.cache.distributed.near.GridNearCacheAdapter;
import org.apache.ignite.internal.processors.cache.transactions.IgniteTxManager;
import org.apache.ignite.internal.transactions.IgniteTxHeuristicCheckedException;
import org.apache.ignite.internal.util.lang.GridAbsPredicate;
import org.apache.ignite.internal.util.typedef.G;
import org.apache.ignite.lang.IgniteCallable;
import org.apache.ignite.lang.IgniteFutureTimeoutException;
import org.apache.ignite.resources.IgniteInstanceResource;
import org.apache.ignite.spi.communication.tcp.TcpCommunicationSpi;
import org.apache.ignite.spi.discovery.tcp.TestTcpDiscoverySpi;
import org.apache.ignite.testframework.GridTestUtils;
import org.apache.ignite.transactions.Transaction;
import org.apache.ignite.transactions.TransactionIsolation;
import org.apache.ignite.util.TestTcpCommunicationSpi;
import org.jsr166.ConcurrentHashMap8;

import static org.apache.ignite.transactions.TransactionConcurrency.OPTIMISTIC;
import static org.apache.ignite.transactions.TransactionConcurrency.PESSIMISTIC;

/**
 * Tests that transaction is invalidated in case of {@link IgniteTxHeuristicCheckedException}.
 */
public class IgniteTxRecoveryAfterStoreCommitSelfTest extends GridCacheAbstractSelfTest {
    /** Cache name. */
    private static final String CACHE_NAME = "cache";

    /** Store map. */
    public static final Map<Object, Object> storeMap = new ConcurrentHashMap8<>();

    /** */
    private static volatile CountDownLatch storeCommitLatch;

    /** */
    private static volatile CountDownLatch nodeFailLatch;

    /** {@inheritDoc} */
    @Override protected int gridCount() {
        return 5;
    }

    /** {@inheritDoc} */
    @Override protected void beforeTestsStarted() throws Exception {
        // No-op.
    }

    /** {@inheritDoc} */
    @Override protected void beforeTest() throws Exception {
        startGridsMultiThreaded(gridCount());
    }

    /** {@inheritDoc} */
    @Override protected void afterTestsStopped() throws Exception {
        // No-op
    }

    /** {@inheritDoc} */
    @Override protected void afterTest() throws Exception {
        stopAllGrids();
    }

    /**
     * @return Index of node starting transaction.
     */
    protected int originatingNode() {
        return 0;
    }

    @Override protected long getTestTimeout() {
        return 300_000;
    }

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String igniteInstanceName) throws Exception {
        TcpCommunicationSpi comm = new TestTcpCommunicationSpi();

        comm.setSharedMemoryPort(-1);

        TestTcpDiscoverySpi discoSpi = new TestTcpDiscoverySpi();

        discoSpi.setIpFinder(GridCacheAbstractSelfTest.ipFinder);

        IgniteConfiguration cfg = super.getConfiguration(igniteInstanceName)
                .setFailureDetectionTimeout(5_000)
                .setDiscoverySpi(discoSpi)
                .setCommunicationSpi(comm);

        if (igniteInstanceName.endsWith("0"))
            cfg.setUserAttributes(Collections.singletonMap("ORIGINATOR", true));

        return cfg;
    }

    /** {@inheritDoc} */
    @Override protected CacheConfiguration cacheConfiguration(String igniteInstanceName) throws Exception {
        CacheConfiguration cfg = super.cacheConfiguration(igniteInstanceName);

        cfg.setName(CACHE_NAME);

        cfg.setCacheStoreFactory(new TestStoreFactory());

        cfg.setReadThrough(true);
        cfg.setWriteThrough(true);

        return cfg;
    }

    /**
     * @throws Exception If failed.
     */
    public void testManyKeysCommit() throws Exception {
        Collection<Integer> keys = new ArrayList<>(200);

        for (int i = 0; i < 20; i++)
            keys.add(i);

        testTxOriginatingNodeFails(keys);
    }

    /**
     * @param keys Keys to update.
     * @throws Exception If failed.
     */
    protected void testTxOriginatingNodeFails(final Collection<Integer> keys) throws Exception {
        assertFalse(keys.isEmpty());

        final Collection<IgniteKernal> grids = new ArrayList<>();

        ClusterNode txNode = grid(originatingNode()).localNode();

        for (int i = 1; i < gridCount(); i++)
            grids.add((IgniteKernal)grid(i));

        final Map<Integer, String> expectedStoreState = new HashMap<>();

        final String initVal = "initialValue";

        for (Integer key : keys) {
            grid(originatingNode()).cache(CACHE_NAME).put(key, initVal);

            expectedStoreState.put(key, String.valueOf(key));
        }

        Map<Integer, Collection<ClusterNode>> nodeMap = new HashMap<>();

        info("Node being checked: " + grid(1).localNode().id());

        for (Integer key : keys) {
            Collection<ClusterNode> nodes = new ArrayList<>();

            nodes.addAll(grid(1).affinity(CACHE_NAME).mapKeyToPrimaryAndBackups(key));

            nodes.remove(txNode);

            nodeMap.put(key, nodes);
        }

        info("Starting tx [values=" + expectedStoreState + ", topVer=" +
                grid(1).context().discovery().topologyVersion() + ']');

        final IgniteEx originatingNodeGrid = grid(originatingNode());

        storeCommitLatch = new CountDownLatch(1);

        nodeFailLatch = new CountDownLatch(1);

        GridTestUtils.runAsync(new Callable<Void>() {
            @Override public Void call() throws Exception {
                IgniteCache<Integer, String> cache = originatingNodeGrid.cache(CACHE_NAME);

                assertNotNull(cache);

                Transaction tx = originatingNodeGrid.transactions().txStart(OPTIMISTIC, TransactionIsolation.SERIALIZABLE);

                try {
                    cache.putAll(expectedStoreState);

                    info("Before commit");

                    tx.commit();
                }
                catch (IgniteFutureTimeoutException ignored) {
                    info("Failed to wait for commit future completion");
                }

                return null;
            }
        });

        nodeFailLatch.await();

        for (Integer key : expectedStoreState.keySet())
            assertEquals(expectedStoreState.get(key), storeMap.get(key));

        info(">>> Stopping originating node " + txNode);

        ((TestTcpDiscoverySpi)grid(originatingNode()).context().config().getDiscoverySpi()).simulateNodeFailure();
        ((TestTcpCommunicationSpi)grid(originatingNode()).context().config().getCommunicationSpi()).simulateNodeFailure();

        storeCommitLatch.countDown();

        G.stop(grid(originatingNode()).name(), true);

        info(">>> Stopped originating node: " + txNode.id());

        boolean txFinished = GridTestUtils.waitForCondition(new GridAbsPredicate() {
            @Override public boolean apply() {
                for (IgniteKernal g : grids) {
                    GridCacheAdapter<?, ?> cache = g.internalCache(CACHE_NAME);

                    IgniteTxManager txMgr = cache.isNear() ?
                            ((GridNearCacheAdapter)cache).dht().context().tm() :
                            cache.context().tm();

                    int txNum = txMgr.idMapSize();

                    if (txNum != 0)
                        return false;
                }

                return true;
            }
        }, 300_000);

        assertTrue(txFinished);

        info("Transactions finished.");

        for (Map.Entry<Integer, Collection<ClusterNode>> e : nodeMap.entrySet()) {
            final Integer key = e.getKey();

            final String val = expectedStoreState.get(key);

            assertFalse(e.getValue().isEmpty());

            for (ClusterNode node : e.getValue()) {
                final UUID checkNodeId = node.id();

                compute(G.ignite(checkNodeId).cluster().forNode(node)).call(new IgniteCallable<Void>() {
                    /** */
                    @IgniteInstanceResource
                    private Ignite ignite;

                    @Override public Void call() throws Exception {
                        IgniteCache<Integer, String> cache = ignite.cache(CACHE_NAME);

                        assertNotNull(cache);

                        assertEquals("Failed to check entry value on node: " + checkNodeId,
                                val, cache.get(key));

                        return null;
                    }
                });
            }
        }

        for (Map.Entry<Integer, String> e : expectedStoreState.entrySet()) {
            for (Ignite g : G.allGrids())
                assertEquals(e.getValue(), g.cache(CACHE_NAME).get(e.getKey()));
        }
    }

    /**
     *
     */
    public static class TestStoreFactory implements Factory<CacheStore> {
        @IgniteInstanceResource
        Ignite ignite;

        /** {@inheritDoc} */
        @Override public CacheStore create() {
            return new TestStore(ignite.cluster().localNode().attribute("ORIGINATOR") != null);
        }
    }

    /**
     *
     */
    public static class TestStore extends CacheStoreAdapter<Object, Object> {
        /** */
        private boolean originatorNodeFlag;

        /** */
        public TestStore(boolean originatorNodeFlag) {

            this.originatorNodeFlag = originatorNodeFlag;
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

        @Override public void sessionEnd(boolean commit) {
            if (!originatorNodeFlag)
                return;

            if (storeCommitLatch != null) {
                try {
                    nodeFailLatch.countDown();

                    storeCommitLatch.await();

                    throw new IgniteException();
                }
                catch (InterruptedException e) {
                    throw new IgniteException(e);
                }
            }
        }
    }
}
