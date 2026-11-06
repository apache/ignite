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

package org.apache.ignite.internal.processors.cache.persistence.snapshot.incremental;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.CountDownLatch;
import org.apache.ignite.cache.affinity.AffinityFunction;
import org.apache.ignite.cache.affinity.AffinityFunctionContext;
import org.apache.ignite.client.ClientCache;
import org.apache.ignite.client.ClientTransaction;
import org.apache.ignite.client.IgniteClient;
import org.apache.ignite.Ignition;
import org.apache.ignite.cluster.ClusterNode;
import org.apache.ignite.cluster.ClusterState;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.ClientConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.cluster.NodeOrderComparator;
import org.apache.ignite.internal.managers.discovery.CustomMessageWrapper;
import org.apache.ignite.internal.util.distributed.InitMessage;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.lang.IgniteFuture;
import org.apache.ignite.spi.discovery.tcp.TcpDiscoverySpi;
import org.apache.ignite.spi.discovery.tcp.messages.TcpDiscoveryAbstractMessage;
import org.apache.ignite.spi.discovery.tcp.messages.TcpDiscoveryCustomEventMessage;
import org.apache.ignite.testframework.GridTestUtils;
import org.junit.Test;

import static org.apache.ignite.internal.processors.cache.persistence.snapshot.AbstractSnapshotSelfTest.snp;
import static org.apache.ignite.internal.processors.odbc.ClientListenerProcessor.CLIENT_LISTENER_PORT;

/**
 * Test reproducing transaction ordering violation during incremental snapshot with thin client.
 */
public class IncrementalSnapshotOrderingTest extends AbstractIncrementalSnapshotTest {
    /** Latch to block InitMessage on nodes 2 and 3. */
    private static volatile CountDownLatch blockInitMsgLatch;

    /** Node indices where InitMessage is blocked. */
    private static final Set<Integer> BLOCKED_NODES = new HashSet<>(Arrays.asList(2, 3));

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String instanceName) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(instanceName);

        CacheConfiguration<Integer, Integer> cacheCfg = cacheConfiguration(CACHE);
        cacheCfg.setAffinity(new SimpleAffinity());
        cfg.setCacheConfiguration(cacheCfg);

        int nodeIdx = getTestIgniteInstanceIndex(instanceName);
        if (BLOCKED_NODES.contains(nodeIdx)) {
            cfg.setDiscoverySpi(new BlockingInitMessageDiscoverySpi()
                .setIpFinder(((TcpDiscoverySpi)cfg.getDiscoverySpi()).getIpFinder()));
        }

        return cfg;
    }

    /** {@inheritDoc} */
    @Override protected void beforeTest() throws Exception {
        cleanPersistenceDir();

        startGrids(nodes());

        grid(0).cluster().state(ClusterState.ACTIVE);

        grid(0).snapshot().createSnapshot(SNP).get();
    }

    /** {@inheritDoc} */
    @Override protected void afterTest() throws Exception {
        blockInitMsgLatch = null;
        super.afterTest();
    }

    /** {@inheritDoc} */
    @Override protected int nodes() {
        return 4;
    }

    /** {@inheritDoc} */
    @Override protected int backups() {
        return 0;
    }

    /**
     * Reproduces the ordering violation.
     */
    @Test
    public void testIncrementalSnapshotOrderingProblem() throws Exception {
        blockInitMsgLatch = new CountDownLatch(1);

        IgniteFuture<Void> snpFut = grid(0).snapshot().createIncrementalSnapshot(SNP);

        boolean started = GridTestUtils.waitForCondition(() -> {
            UUID id = snp(grid(0)).incrementalSnapshotId();
            return id != null;
        }, 10_000);
        assertTrue("Snapshot did not start on node 0", started);

        int clientPort = grid(0).localNode().<Integer>attribute(CLIENT_LISTENER_PORT);
        ClientConfiguration clientCfg = new ClientConfiguration()
            .setAddresses("127.0.0.1:" + clientPort);

        try (IgniteClient thinClient = Ignition.startClient(clientCfg)) {
            ClientCache<Integer, Integer> cache = thinClient.cache(CACHE);

            try (ClientTransaction tx = thinClient.transactions().txStart()) {
                cache.put(1, 100);
                cache.put(2, 200);
                tx.commit();
            }

            try (ClientTransaction tx = thinClient.transactions().txStart()) {
                cache.put(3, 300);
                cache.put(4, 400);
                tx.commit();
            }
        }

        assertTrue("Snapshot started on node 2 prematurely", snp(grid(2)).incrementalSnapshotId() == null);
        blockInitMsgLatch.countDown();

        snpFut.get(getTestTimeout());

        restartWithCleanPersistence(nodes(), Collections.singletonList(CACHE));
        grid(0).snapshot().restoreSnapshot(SNP, null, 1).get(getTestTimeout());

        Integer val1 = (Integer)grid(0).cache(CACHE).get(1);
        Integer val3 = (Integer)grid(2).cache(CACHE).get(3);

        if (val1 == null && val3 != null) {
            fail("Incremental snapshot ordering violation detected: " +
                "tx1 is missing (should be present as it was committed before tx2), " +
                "but tx2 (key3=" + val3 + ") is present.");
        }
    }

    /** Discovery SPI that blocks InitMessage processing on certain nodes. */
    private static class BlockingInitMessageDiscoverySpi extends TcpDiscoverySpi {
        /** {@inheritDoc} */
        @Override protected void startMessageProcess(TcpDiscoveryAbstractMessage msg) {
            if (msg instanceof TcpDiscoveryCustomEventMessage && blockInitMsgLatch != null) {
                TcpDiscoveryCustomEventMessage m = (TcpDiscoveryCustomEventMessage)msg;

                try {
                    CustomMessageWrapper m0 = (CustomMessageWrapper)m.message(
                        marshaller(), U.resolveClassLoader(ignite().configuration()));

                    if (m0.delegate() instanceof InitMessage) {
                        try {
                            blockInitMsgLatch.await();
                        } catch (InterruptedException e) {
                            Thread.currentThread().interrupt();
                            throw new RuntimeException(e);
                        }
                    }
                }
                catch (Throwable ignored) {
                    // No-op
                }
            }

            super.startMessageProcess(msg);
        }
    }

    /** Simple affinity mapping key N to node N-1. */
    private static class SimpleAffinity implements AffinityFunction {
        /** Serial version uid. */
        private static final long serialVersionUID = 0L;

        /** {@inheritDoc} */
        @Override public void reset() {
        }

        /** {@inheritDoc} */
        @Override public int partitions() {
            return 4;
        }

        /** {@inheritDoc} */
        @Override public int partition(Object key) {
            if (key instanceof Integer) {
                int k = (Integer)key;
                // Key 1 → partition 0, 2 → 1, 3 → 2, 4 → 3.
                return (k - 1) % partitions();
            }
            return Math.abs(key.hashCode() % partitions());
        }

        /** {@inheritDoc} */
        @Override public List<List<ClusterNode>> assignPartitions(AffinityFunctionContext affCtx) {
            List<ClusterNode> nodes = new ArrayList<>(affCtx.currentTopologySnapshot());
            nodes.sort(NodeOrderComparator.getInstance());

            List<List<ClusterNode>> result = new ArrayList<>(partitions());
            for (int i = 0; i < partitions(); i++)
                result.add(Collections.singletonList(nodes.get(i % nodes.size())));

            return result;
        }

        /** {@inheritDoc} */
        @Override public void removeNode(UUID nodeId) {
        }
    }
}
