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

import java.util.concurrent.ThreadLocalRandom;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.IgniteSystemProperties;
import org.apache.ignite.cache.CacheMode;
import org.apache.ignite.cluster.ClusterNode;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.IgniteInternalFuture;
import org.apache.ignite.internal.IgniteKernal;
import org.apache.ignite.internal.util.future.GridFutureAdapter;
import org.apache.ignite.lang.IgniteInClosure;
import org.apache.ignite.spi.discovery.tcp.TcpDiscoverySpi;
import org.apache.ignite.spi.discovery.tcp.ipfinder.TcpDiscoveryIpFinder;
import org.apache.ignite.spi.discovery.tcp.ipfinder.vm.TcpDiscoveryVmIpFinder;
import org.apache.ignite.testframework.GridTestUtils;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;

import static org.apache.ignite.cache.CacheAtomicityMode.TRANSACTIONAL;
import static org.apache.ignite.cache.CacheWriteSynchronizationMode.FULL_SYNC;

/**
 *
 */
public class IgniteDiagnosticMessagesTest extends GridCommonAbstractTest {
    /** */
    private static TcpDiscoveryIpFinder ipFinder = new TcpDiscoveryVmIpFinder(true);

    /** */
    private boolean client;

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String gridName) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(gridName);

        ((TcpDiscoverySpi)cfg.getDiscoverySpi()).setIpFinder(ipFinder);

        cfg.setClientMode(client);

        CacheConfiguration ccfg = new CacheConfiguration();

        ccfg.setWriteSynchronizationMode(FULL_SYNC);
        ccfg.setCacheMode(CacheMode.REPLICATED);
        ccfg.setAtomicityMode(TRANSACTIONAL);
        ccfg.setName("c1");

        cfg.setCacheConfiguration(ccfg);

        return cfg;
    }

    /** {@inheritDoc} */
    @Override protected void beforeTestsStarted() throws Exception {
        super.beforeTestsStarted();

        System.setProperty(IgniteSystemProperties.IGNITE_DIAGNOSTIC_ENABLED, "true");

        startGrids(3);

        client = true;

        startGrid(3);

        startGrid(4);
    }

    /** {@inheritDoc} */
    @Override protected void afterTestsStopped() throws Exception {
        System.setProperty(IgniteSystemProperties.IGNITE_DIAGNOSTIC_ENABLED, "false");

        stopAllGrids();

        super.afterTestsStopped();
    }

    /**
     * @throws Exception If failed.
     */
    public void testDiagnosticMessages() throws Exception {
        awaitPartitionMapExchange();

        sendDiagnostic();

        for (int i = 0; i < 5; i++) {
            final IgniteCache cache = ignite(i).cache("c1");

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

                    node.context().cluster().dumpBasicInfo(dstNode.id(), "Test diagnostic",
                        new IgniteInClosure<IgniteInternalFuture<String>>() {
                            @Override public void apply(IgniteInternalFuture<String> diagFut) {
                                try {
                                    fut.onDone(diagFut.get());
                                }
                                catch (Exception e) {
                                    fut.onDone(e);
                                }
                            }
                        }
                    );

                    String msg = fut.get();

                    assertTrue("Unexpected message: " + msg,
                        msg.contains("Test diagnostic") &&
                        msg.contains("General node info [id=" + dstNode.id() + ", client=" + dstNode.isClient() + ", discoTopVer=AffinityTopologyVersion [topVer=5, minorTopVer=0]") &&
                        msg.contains("Partitions exchange info [readyVer=AffinityTopologyVersion [topVer=5, minorTopVer="));
                }
            }
        }
    }
}
