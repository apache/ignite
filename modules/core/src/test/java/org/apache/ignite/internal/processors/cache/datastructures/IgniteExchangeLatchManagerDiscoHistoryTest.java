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

package org.apache.ignite.internal.processors.cache.datastructures;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.IgniteException;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.failure.AbstractFailureHandler;
import org.apache.ignite.failure.FailureContext;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.internal.IgniteInternalFuture;
import org.apache.ignite.internal.IgnitionEx;
import org.apache.ignite.internal.processors.cache.distributed.dht.preloader.GridDhtPartitionsExchangeFuture;
import org.apache.ignite.internal.processors.cache.distributed.dht.preloader.PartitionsExchangeAware;
import org.apache.ignite.internal.processors.cache.distributed.dht.preloader.latch.ExchangeLatchManager;
import org.apache.ignite.internal.processors.metastorage.DistributedMetastorageLifecycleListener;
import org.apache.ignite.internal.processors.metastorage.ReadableDistributedMetaStorage;
import org.apache.ignite.internal.util.typedef.X;
import org.apache.ignite.lifecycle.LifecycleBean;
import org.apache.ignite.lifecycle.LifecycleEventType;
import org.apache.ignite.resources.IgniteInstanceResource;
import org.apache.ignite.spi.discovery.tcp.TcpDiscoverySpi;
import org.apache.ignite.spi.discovery.tcp.TestTcpDiscoverySpi;
import org.apache.ignite.spi.discovery.tcp.ipfinder.TcpDiscoveryIpFinder;
import org.apache.ignite.testframework.GridTestUtils;
import org.apache.ignite.testframework.junits.WithSystemProperty;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import static org.apache.ignite.IgniteSystemProperties.IGNITE_DISCOVERY_HISTORY_SIZE;

/**
 * Test {@link ExchangeLatchManager} throws {@link IgniteException} with appropriate message when topology history
 * cannot be obtained.
 */
public class IgniteExchangeLatchManagerDiscoHistoryTest extends GridCommonAbstractTest {
    /** Topology history size. */
    private static final int TOPOLOGY_HISTORY_SIZE = 2;

    /** Disco cache size. */
    private static final String DISCO_HISTORY_SIZE = "2";

    /** Timeout in millis. */
    private static final long DEFAULT_TIMEOUT = 30_000;

    /** Lifecycle bean that is used to register required listeners. */
    private LifecycleBean lifecycleBean;

    /** Flag indicates that a node, that is starting, should have short topology history. */
    private boolean victim;

    /** Discovery SPI that is used by the node with short topology history. */
    private CustomTcpDiscoverySpi disco;

    /** Failure context. */
    private final AtomicReference<FailureContext> cpFailureCtx = new AtomicReference<>();

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String igniteInstanceName) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(igniteInstanceName);

        TcpDiscoveryIpFinder ipFinder = ((TcpDiscoverySpi)cfg.getDiscoverySpi()).getIpFinder();

        int topHistSize = victim ? TOPOLOGY_HISTORY_SIZE : TcpDiscoverySpi.DFLT_TOP_HISTORY_SIZE;

        CustomTcpDiscoverySpi discoSpi = new CustomTcpDiscoverySpi(topHistSize, ipFinder);

        cfg.setDiscoverySpi(discoSpi);

        if (victim) {
            cfg.setFailureHandler(new AbstractFailureHandler() {
                /** {@inheritDoc} */
                @Override protected boolean handle(Ignite ignite, FailureContext failureCtx) {
                    cpFailureCtx.compareAndSet(null, failureCtx);

                    // Invalidate kernel context.
                    return true;
                }
            });

            cfg.setLifecycleBeans(lifecycleBean);

            disco = discoSpi;
        }

        return cfg;
    }

    /**
     * Prepares test for execution.
     */
    @Before
    public void startup() {
        shutdown();
    }

    /**
     * Cleans after the test.
     */
    @After
    public void shutdown() {
        lifecycleBean = null;

        victim = false;

        cpFailureCtx.set(null);

        disco = null;

        stopAllGrids();
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    @WithSystemProperty(key = IGNITE_DISCOVERY_HISTORY_SIZE, value = DISCO_HISTORY_SIZE)
    public void testProperException() throws Exception {
        final IgniteEx crd = startGrid(0);

        final CountDownLatch exchangeLatch = new CountDownLatch(1);

        final CountDownLatch startSrvsLatch = new CountDownLatch(1);

        final AtomicReference<Exception> err = new AtomicReference<>();

        // Lifecycle bean that is used to register PartitionsExchangeAware listener.
        lifecycleBean = new LifecycleBean() {
            /** Ignite instance. */
            @IgniteInstanceResource
            IgniteEx ignite;

            /** {@inheritDoc} */
            @Override public void onLifecycleEvent(LifecycleEventType evt) throws IgniteException {
                if (evt == LifecycleEventType.BEFORE_NODE_START) {
                    // The goal is registering PartitionsExchangeAware listener before the discovery manager is started.
                    ignite.context().internalSubscriptionProcessor()
                        .registerDistributedMetastorageListener(new DistributedMetastorageLifecycleListener() {
                            @Override public void onReadyForRead(ReadableDistributedMetaStorage metastorage) {
                                ignite.context().cache().context().exchange()
                                    .registerExchangeAwareComponent(new PartitionsExchangeAware() {
                                        /** {@inheritDoc} */
                                        @Override public void onInitBeforeTopologyLock(GridDhtPartitionsExchangeFuture fut) {
                                            try {
                                                // Let's start nodes.
                                                startSrvsLatch.countDown();

                                                // Blocks the initial exchange and waits for other nodes.
                                                exchangeLatch.await(DEFAULT_TIMEOUT, TimeUnit.MILLISECONDS);
                                            }
                                            catch (Exception e) {
                                                err.compareAndSet(null, e);
                                            }
                                        }
                                });
                            }
                    });
                }
            }
        };

        // Start server node with short topology history.
        victim = true;

        GridTestUtils.runAsync(() -> startGrid(1));

        // Waits for the initial exchange.
        startSrvsLatch.await(DEFAULT_TIMEOUT, TimeUnit.MILLISECONDS);

        victim = false;

        lifecycleBean = null;

        List<IgniteInternalFuture> srvFuts = new ArrayList<>(TOPOLOGY_HISTORY_SIZE);

        try {
            // Major topology version that is corresponding to the start of the node with short topology history.
            final long topVer = 2;

            // Starting server nodes to exhaust the topology history.
            for (int i = 2; i < 3 * TOPOLOGY_HISTORY_SIZE && !disco.isEmptyTopologyHistory(topVer); ++i) {
                final int currNodeIdx = i;

                final int joinedNodesCnt = disco.totalJoinedNodes();

                srvFuts.add(GridTestUtils.runAsync(() -> startGrid(currNodeIdx)));

                assertTrue("Failed to wait for a new server node [joinedNodesCnt=" + joinedNodesCnt + "]",
                    GridTestUtils.waitForCondition(
                        () -> disco.totalJoinedNodes() >= (joinedNodesCnt + 1), DEFAULT_TIMEOUT));
            }

            assertTrue(
                "Disco cache history is not empty for the topology [majorTopVer=" + topVer + ']',
                disco.isEmptyTopologyHistory(topVer));

            // Let's continue the ongoing exchange.
            exchangeLatch.countDown();

            boolean failureHnd = GridTestUtils.waitForCondition(() -> cpFailureCtx.get() != null, DEFAULT_TIMEOUT);

            assertNull(
                "Unexpected exception (probably, the topology history still exists [err=" + err + ']',
                err.get());

            assertTrue("Failure handler was not triggered.", failureHnd);

            // Check that IgniteException was thrown instead of NullPointerException.
            assertTrue(
                "IgniteException must be thrown.",
                X.hasCause(cpFailureCtx.get().error(), IgniteException.class));

            // Check that message contains a hint to fix the issue.
            GridTestUtils.assertContains(
                log,
                cpFailureCtx.get().error().getMessage(),
                "Consider increasing IGNITE_DISCOVERY_HISTORY_SIZE property. Current value is " + DISCO_HISTORY_SIZE);
        }
        finally {
            IgnitionEx.stop(getTestIgniteInstanceName(1), true, true);

            srvFuts.forEach(f -> {
                try {
                    f.get(DEFAULT_TIMEOUT);
                }
                catch (IgniteCheckedException e) {
                    err.compareAndSet(null, e);
                }
            });
        }

        assertNull("Unexpected exception [err=" + err.get() + ']', err.get());
    }

    /**
     * Custom discovery SPI that allows specifying a short topology history for testing purposes.
     */
    private static class CustomTcpDiscoverySpi extends TestTcpDiscoverySpi {
        /**
         * Creates a new instance of discovery spi with the given size of topology snapshots history.
         *
         * @param topHistSize Size of topology snapshots history.
         * @param ipFinder IP finder
         */
        CustomTcpDiscoverySpi(int topHistSize, TcpDiscoveryIpFinder ipFinder) {
            this.topHistSize = topHistSize;

            setIpFinder(ipFinder);
        }

        /**
         * Returns the number of joined nodes.
         *
         * @return Number of joined nodes.
         */
        int totalJoinedNodes() {
            return stats.joinedNodesCount();
        }

        /**
         * Returns {@code true} if the given topology version is already removed from the history.
         *
         * @param topVer Major topology version.
         * @return {@code true} if the given topology version is already removed from the history.
         */
        boolean isEmptyTopologyHistory(long topVer) {
            return ((IgniteEx)ignite).context().discovery().topology(topVer) == null;
        }
    }
}
