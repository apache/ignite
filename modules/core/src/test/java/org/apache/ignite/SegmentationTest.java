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

package org.apache.ignite;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CyclicBarrier;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import org.apache.ignite.cluster.ClusterNode;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.IgniteInternalFuture;
import org.apache.ignite.internal.TestRecordingCommunicationSpi;
import org.apache.ignite.internal.processors.cache.distributed.GridCacheModuloAffinityFunction;
import org.apache.ignite.internal.processors.cache.distributed.dht.IgniteCacheTopologySplitAbstractTest;
import org.apache.ignite.internal.util.typedef.G;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.spi.discovery.tcp.TcpDiscoverySpi;
import org.apache.ignite.transactions.Transaction;
import org.junit.Test;

import static org.apache.ignite.cache.CacheAtomicityMode.TRANSACTIONAL;
import static org.apache.ignite.cache.CacheMode.PARTITIONED;
import static org.apache.ignite.cache.CacheWriteSynchronizationMode.FULL_SYNC;
import static org.apache.ignite.cache.PartitionLossPolicy.READ_ONLY_SAFE;
import static org.apache.ignite.internal.processors.cache.distributed.GridCacheModuloAffinityFunction.IDX_ATTR;
import static org.apache.ignite.spi.discovery.tcp.TcpDiscoverySpi.DFLT_PORT;
import static org.apache.ignite.testframework.GridTestUtils.runAsync;

/** */
public class SegmentationTest extends IgniteCacheTopologySplitAbstractTest {
    /** */
    public static final int NODES_CNT = 4;

    /** */
    public static final int KEYS_CNT = 10;

    /** */
    public static final int PUT_ATTEMPTS_CNT = 100000;

    /** {@inheritDoc} */
    @Override protected boolean isBlocked(int locPort, int rmtPort) {
        return isDiscoPort(locPort) && isDiscoPort(rmtPort) && segment(locPort) != segment(rmtPort);
    }

    /** */
    @Override public int segment(ClusterNode node) {
        return node.<Integer>attribute(IDX_ATTR) % 2;
    }

    /** */
    @Override protected IgniteConfiguration getConfiguration(String igniteInstanceName) throws Exception {
        return super.getConfiguration(igniteInstanceName)
            .setUserAttributes(Collections.singletonMap(IDX_ATTR, getTestIgniteInstanceIndex(igniteInstanceName)));
    }

    /** */
    @Test
    public void testImplicitTx() throws Exception {
        doTest(true);
    }

    /** */
    @Test
    public void testImplicitExplicitTx() throws Exception {
        doTest(false);
    }

    /** */
    private void doTest(boolean implicit) throws Exception {
        stopAllGrids();

        startGrids(NODES_CNT);

        for (Ignite ignite : G.allGrids()) {
            TestRecordingCommunicationSpi comm = (TestRecordingCommunicationSpi)
                ignite.configuration().getCommunicationSpi();

            comm.blockMessages(new SegmentBlocker(ignite.cluster().localNode()));
        }

        grid(0).createCache(new CacheConfiguration<>(DEFAULT_CACHE_NAME)
            .setBackups(NODES_CNT / 2)
            .setTopologyValidator(nodes -> nodes.size() == NODES_CNT)
            .setWriteSynchronizationMode(FULL_SYNC)
            .setAtomicityMode(TRANSACTIONAL)
            .setAffinity(new GridCacheModuloAffinityFunction(64, 2))
            .setPartitionLossPolicy(READ_ONLY_SAFE)
            .setCacheMode(PARTITIONED));

        CyclicBarrier barrier = new CyclicBarrier(3);

        IgniteInternalFuture<?> splitFut = runAsync(() -> {
            try {
                barrier.await();

                U.sleep(300);

                splitAndWait();
            }
            catch (Throwable ignored) {
                // No-op.
            }
        });

        IgniteInternalFuture<?> firstSegmentPutFut = runAsync(() -> {
            try {
                barrier.await();

                doPut(grid(0), 0, implicit);
            }
            catch (Throwable ignored) {
                // No-op.
            }
        });

        IgniteInternalFuture<?> secondSegmentPutFut = runAsync(() -> {
            try {
                barrier.await();

                doPut(grid(1), 1, implicit);
            }
            catch (Throwable ignored) {
                // No-op.
            }
        });

        splitFut.get();
        firstSegmentPutFut.get();
        secondSegmentPutFut.get();

        List<Map<Object, Object>> firstSegmentNodesEntries = new ArrayList<>();
        List<Map<Object, Object>> secondSegmentNodesEntries = new ArrayList<>();

        Set<Integer> testKeys = IntStream.range(0, KEYS_CNT).boxed().collect(Collectors.toSet());

        for (Ignite ignite : G.allGrids()) {
            (segment(ignite.cluster().localNode()) == 0 ? firstSegmentNodesEntries : secondSegmentNodesEntries)
                .add(ignite.cache(DEFAULT_CACHE_NAME).getAll(testKeys));
        }

        assertEquals(1, firstSegmentNodesEntries.stream().distinct().count());
        assertEquals(1, secondSegmentNodesEntries.stream().distinct().count());
        assertEquals(firstSegmentNodesEntries.get(0), secondSegmentNodesEntries.get(0));
    }

    /** */
    private void doPut(Ignite ignite, int val, boolean implicit) {
        IgniteCache<Object, Object> cache = ignite.cache(DEFAULT_CACHE_NAME);

        if (implicit) {
            for (int i = 0; i < PUT_ATTEMPTS_CNT; i++) {
                try {
                    cache.put(i % KEYS_CNT, val);
                }
                catch (Throwable ignored) {
                    // No-op.
                }
            }
        }
        else {
            IgniteTransactions txs = ignite.transactions();

            for (int i = 0; i < PUT_ATTEMPTS_CNT; i++) {
                try (Transaction tx = txs.txStart()) {
                    cache.put(i % KEYS_CNT, val);

                    tx.commit();
                }
                catch (Throwable ignored) {
                    // No-op.
                }
            }
        }

    }

    /**  */
    private int segment(int discoPort) {
        return (discoPort - DFLT_PORT) % 2;
    }

    /**  */
    private boolean isDiscoPort(int port) {
        return port >= DFLT_PORT &&
            port <= (DFLT_PORT + TcpDiscoverySpi.DFLT_PORT_RANGE);
    }
}
