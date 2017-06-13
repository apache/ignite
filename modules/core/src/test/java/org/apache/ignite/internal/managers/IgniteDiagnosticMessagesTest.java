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

package org.apache.ignite.internal.managers;

import java.util.UUID;
import java.util.concurrent.Callable;
import java.util.concurrent.ThreadLocalRandom;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.cluster.ClusterNode;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.IgniteDiagnosticPrepareContext;
import org.apache.ignite.internal.IgniteInternalFuture;
import org.apache.ignite.internal.IgniteKernal;
import org.apache.ignite.internal.TestRecordingCommunicationSpi;
import org.apache.ignite.internal.processors.cache.distributed.near.GridNearSingleGetResponse;
import org.apache.ignite.internal.util.future.GridFutureAdapter;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.lang.IgniteInClosure;
import org.apache.ignite.spi.communication.tcp.TcpCommunicationSpi;
import org.apache.ignite.spi.discovery.tcp.TcpDiscoverySpi;
import org.apache.ignite.spi.discovery.tcp.ipfinder.TcpDiscoveryIpFinder;
import org.apache.ignite.spi.discovery.tcp.ipfinder.vm.TcpDiscoveryVmIpFinder;
import org.apache.ignite.testframework.GridStringLogger;
import org.apache.ignite.testframework.GridTestUtils;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;

import static org.apache.ignite.IgniteSystemProperties.IGNITE_LONG_OPERATIONS_DUMP_TIMEOUT;
import static org.apache.ignite.cache.CacheAtomicityMode.TRANSACTIONAL;
import static org.apache.ignite.cache.CacheMode.PARTITIONED;
import static org.apache.ignite.cache.CacheMode.REPLICATED;
import static org.apache.ignite.cache.CacheWriteSynchronizationMode.FULL_SYNC;

/**
 *
 */
public class IgniteDiagnosticMessagesTest extends GridCommonAbstractTest {
    /** */
    private static TcpDiscoveryIpFinder ipFinder = new TcpDiscoveryVmIpFinder(true);

    /** */
    private boolean client;

    /** */
    private Integer connectionsPerNode;

    /** */
    private boolean testSpi;

    /** */
    private GridStringLogger strLog;

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String gridName) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(gridName);

        ((TcpDiscoverySpi)cfg.getDiscoverySpi()).setIpFinder(ipFinder);

        if (testSpi)
            cfg.setCommunicationSpi(new TestRecordingCommunicationSpi());

        if (connectionsPerNode != null)
            ((TcpCommunicationSpi)cfg.getCommunicationSpi()).setConnectionsPerNode(connectionsPerNode);

        cfg.setClientMode(client);

        if (strLog != null) {
            cfg.setGridLogger(strLog);

            strLog = null;
        }

        return cfg;
    }

    /** {@inheritDoc} */
    @Override protected void afterTest() throws Exception {
        stopAllGrids();

        super.afterTestsStopped();
    }


    /**
     * @throws Exception If failed.
     */
    public void testDiagnosticMessages1() throws Exception {
        checkBasicDiagnosticInfo();
    }

    /**
     * @throws Exception If failed.
     */
    public void testDiagnosticMessages2() throws Exception {
        connectionsPerNode = 5;

        checkBasicDiagnosticInfo();
    }

    /**
     * @throws Exception If failed.
     */
    public void testLongRunning() throws Exception {
        System.setProperty(IGNITE_LONG_OPERATIONS_DUMP_TIMEOUT, "3500");

        try {
            testSpi = true;

            startGrid(0);

            GridStringLogger strLog = this.strLog = new GridStringLogger();

            startGrid(1);

            awaitPartitionMapExchange();

            CacheConfiguration ccfg = new CacheConfiguration(DEFAULT_CACHE_NAME);

            ccfg.setWriteSynchronizationMode(FULL_SYNC);
            ccfg.setCacheMode(PARTITIONED);
            ccfg.setAtomicityMode(TRANSACTIONAL);

            final Ignite node0 = ignite(0);

            node0.createCache(ccfg);

            final Ignite node1 = ignite(1);

            UUID id0 = node0.cluster().localNode().id();
            UUID id1 = node1.cluster().localNode().id();

            TestRecordingCommunicationSpi.spi(node0).blockMessages(GridNearSingleGetResponse.class, node1.name());

            IgniteInternalFuture fut = GridTestUtils.runAsync(new Callable<Void>() {
                @Override public Void call() throws Exception {
                    Integer key = primaryKey(node0.cache(DEFAULT_CACHE_NAME));

                    node1.cache(DEFAULT_CACHE_NAME).get(key);

                    return null;
                }
            }, "get");

            U.sleep(10_000);

            assertFalse(fut.isDone());

            TestRecordingCommunicationSpi.spi(node0).stopBlock();

            fut.get();

            String log = strLog.toString();

            assertTrue(log.contains("GridPartitionedSingleGetFuture waiting for response [node=" + id0));
            assertTrue(log.contains("General node info [id=" + id0));
        }
        finally {
            System.clearProperty(IGNITE_LONG_OPERATIONS_DUMP_TIMEOUT);
        }
    }

    /**
     * @throws Exception If failed.
     */
    private void checkBasicDiagnosticInfo() throws Exception {
        startGrids(3);

        client = true;

        startGrid(3);

        startGrid(4);

        CacheConfiguration ccfg = new CacheConfiguration(DEFAULT_CACHE_NAME);

        ccfg.setWriteSynchronizationMode(FULL_SYNC);
        ccfg.setCacheMode(REPLICATED);
        ccfg.setAtomicityMode(TRANSACTIONAL);

        ignite(0).createCache(ccfg);

        awaitPartitionMapExchange();

        sendDiagnostic();

        for (int i = 0; i < 5; i++) {
            final IgniteCache<Object, Object> cache = ignite(i).cache(DEFAULT_CACHE_NAME);

            // Put from multiple threads to create multiple connections.
            GridTestUtils.runMultiThreaded(new Runnable() {
                @Override public void run() {
                    for (int j = 0; j < 10; j++)
                        cache.put(ThreadLocalRandom.current().nextInt(), j);
                }
            }, 10, "cache-thread");
        }

        sendDiagnostic();
    }

    /**
     * @throws Exception If failed.
     */
    private void sendDiagnostic() throws Exception {
        for (int i = 0; i < 5; i++) {
            IgniteKernal node = (IgniteKernal)ignite(i);

            for (int j = 0; j < 5; j++) {
                if (i != j) {
                    ClusterNode dstNode = ignite(j).cluster().localNode();

                    final GridFutureAdapter<String> fut = new GridFutureAdapter<>();

                    IgniteDiagnosticPrepareContext ctx = new IgniteDiagnosticPrepareContext(node.getLocalNodeId());

                    ctx.basicInfo(dstNode.id(), "Test diagnostic");

                    ctx.send(node.context(), new IgniteInClosure<IgniteInternalFuture<String>>() {
                        @Override public void apply(IgniteInternalFuture<String> diagFut) {
                            try {
                                fut.onDone(diagFut.get());
                            }
                            catch (Exception e) {
                                fut.onDone(e);
                            }
                        }
                    });

                    String msg = fut.get();

                    assertTrue("Unexpected message: " + msg,
                        msg.contains("Test diagnostic") &&
                            msg.contains("General node info [id=" + dstNode.id() + ", client=" + dstNode.isClient() + ", discoTopVer=AffinityTopologyVersion [topVer=5, minorTopVer=") &&
                            msg.contains("Partitions exchange info [readyVer=AffinityTopologyVersion [topVer=5, minorTopVer="));
                }
            }
        }
    }
}
