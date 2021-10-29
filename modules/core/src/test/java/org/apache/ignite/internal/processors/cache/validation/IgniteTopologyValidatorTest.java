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

package org.apache.ignite.internal.processors.cache.validation;

import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.stream.Collectors;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.cache.validation.IgniteCacheTopologyValidator;
import org.apache.ignite.cache.validation.SegmentationResolverPluginProvider;
import org.apache.ignite.cluster.ClusterNode;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.internal.processors.cache.CacheInvalidStateException;
import org.apache.ignite.internal.processors.cache.distributed.dht.IgniteCacheTopologySplitAbstractTest;
import org.apache.ignite.internal.util.typedef.G;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.spi.IgniteSpiException;
import org.apache.ignite.spi.discovery.tcp.TcpDiscoverySpi;
import org.apache.ignite.spi.discovery.tcp.ipfinder.vm.TcpDiscoveryVmIpFinder;
import org.apache.ignite.testframework.GridTestUtils.RunnableX;
import org.junit.Test;

import static java.util.Collections.singletonMap;
import static java.util.stream.Collectors.toList;
import static org.apache.ignite.cache.CacheMode.REPLICATED;
import static org.apache.ignite.cache.CacheWriteSynchronizationMode.PRIMARY_SYNC;
import static org.apache.ignite.cache.validation.IgnitePluggableSegmentationResolver.SEG_RESOLVER_ENABLED_PROP_NAME;
import static org.apache.ignite.cluster.ClusterState.ACTIVE;
import static org.apache.ignite.cluster.ClusterState.ACTIVE_READ_ONLY;
import static org.apache.ignite.events.EventType.EVT_NODE_JOINED;
import static org.apache.ignite.internal.processors.cache.distributed.GridCacheModuloAffinityFunction.IDX_ATTR;
import static org.apache.ignite.spi.discovery.tcp.TcpDiscoverySpi.DFLT_PORT;
import static org.apache.ignite.spi.discovery.tcp.TcpDiscoverySpi.DFLT_PORT_RANGE;
import static org.apache.ignite.testframework.GridTestUtils.assertThrowsAnyCause;
import static org.apache.ignite.testframework.GridTestUtils.assertThrowsWithCause;
import static org.apache.ignite.testframework.GridTestUtils.waitForCondition;

/** */
@SuppressWarnings("ThrowableNotThrown")
public class IgniteTopologyValidatorTest extends IgniteCacheTopologySplitAbstractTest {
    /** */
    private static final int CACHE_KEY_CNT = 1000;

    /** */
    public static final int CACHE_CNT = 2;

    /** */
    @Override protected IgniteConfiguration getConfiguration(String igniteInstanceName) throws Exception {
        return getConfiguration(igniteInstanceName, true);
    }

    /** */
    private IgniteConfiguration getConfiguration(
        String igniteInstanceName,
        boolean configureSegmentationResolverPlugin
    ) throws Exception {
        int idx = getTestIgniteInstanceIndex(igniteInstanceName);

        IgniteConfiguration cfg = super.getConfiguration(igniteInstanceName)
            .setUserAttributes(singletonMap(IDX_ATTR, idx));

        if (configureSegmentationResolverPlugin)
            cfg.setPluginProviders(new SegmentationResolverPluginProvider());

        ((TcpDiscoverySpi)cfg.getDiscoverySpi())
            .setIpFinder(sharedStaticIpFinder)
            .setLocalPortRange(1)
            .setLocalPort(discoPort(idx))
            .setConnectionRecoveryTimeout(0);

        return cfg;
    }

    /** {@inheritDoc} */
    @Override protected void afterTest() throws Exception {
        super.afterTest();

        stopAllGrids();
    }

    /** {@inheritDoc} */
    @Override protected boolean isBlocked(int locPort, int rmtPort) {
        return isDiscoPort(locPort) && isDiscoPort(rmtPort) && segment(locPort) != segment(rmtPort);
    }

    /**  */
    private int segment(int discoPort) {
        return (discoPort - DFLT_PORT) % 2 == 0 ? 0 : 1;
    }

    /** */
    @Override public int segment(ClusterNode node) {
        return node.<Integer>attribute(IDX_ATTR) % 2 == 0 ? 0 : 1;
    }

    /** {@inheritDoc} */
    @Override protected IgniteEx startGrid(IgniteConfiguration cfg) throws Exception {
        return super.startGrid(optimize(cfg));
    }

    /** {@inheritDoc} */
    @Override protected IgniteEx startClientGrid(IgniteConfiguration cfg) throws Exception {
        return super.startClientGrid(optimize(cfg));
    }

    /** */
    @Test
    public void testConnectionToIncompatibleCluster() throws Exception {
        startGrid(getConfiguration(getTestIgniteInstanceName(0), false));

        startGrid(1);

        grid(0).cluster().baselineAutoAdjustEnabled(false);

        assertTrue(waitForCondition(
            () -> !(Boolean)grid(1).context().distributedConfiguration().property(SEG_RESOLVER_ENABLED_PROP_NAME).get(),
            getTestTimeout()
        ));

        splitAndWait();

        connectNodeToSegment(3, 1);

        assertTrue(waitForCondition(
            () -> !(Boolean)grid(1).context().distributedConfiguration().property(SEG_RESOLVER_ENABLED_PROP_NAME).get(),
            getTestTimeout()
        ));
    }

    /** */
    @Test
    public void testIncompatibleNodeConnection() throws Exception {
        IgniteEx srv = startGrid(0);

        assertThrowsAnyCause(
            log,
            () -> startGrid(getConfiguration(getTestIgniteInstanceName(1), false)),
            IgniteSpiException.class,
            "The Segmentation Resolver plugin is not configured for the server node that is trying to join the cluster."
        );

        startClientGrid(getConfiguration(getTestIgniteInstanceName(2), false));

        assertEquals(2, srv.cluster().nodes().size());
    }

    /** */
    @Test
    public void testCacheCreationWithSegmentationResolverMissed() throws Exception {
        IgniteEx srv = startGrid(getConfiguration(getTestIgniteInstanceName(0), false));

        assertThrowsWithCause(
            () -> srv.createCache(new CacheConfiguration<>(DEFAULT_CACHE_NAME)
                .setTopologyValidator(new IgniteCacheTopologyValidator())),
            IgniteCheckedException.class
        );
    }

    /** */
    @Test
    public void testCacheCreationWithSegmentationResolverMissedOnClient() throws Exception {
        startGrid(0);

        IgniteEx cli = startClientGrid(getConfiguration(getTestIgniteInstanceName(1), false));

        cli.createCache(new CacheConfiguration<>(DEFAULT_CACHE_NAME)
            .setTopologyValidator(new IgniteCacheTopologyValidator()));
    }

    /** */
    @Test
    public void testConnectionToSegmentedCluster() throws Exception {
        startGridsMultiThreaded(4);

        grid(0).cluster().baselineAutoAdjustEnabled(false);

        createCaches();

        splitAndWait();

        checkPutGet(G.allGrids(), false);

        connectNodeToSegment(4, 0);
        checkPutGet(0, false);

        connectNodeToSegment(5, 1);
        checkPutGet(1, false);
    }

    /** */
    @Test
    public void testRegularNodeStartStop() throws Exception {
        startGrid(0);

        createCaches();

        checkPutGetAfter(() -> startGrid(1));
        checkPutGetAfter(() -> stopGrid(1));

        checkPutGetAfter(() -> startClientGrid(2));
        checkPutGetAfter(() -> stopGrid(2));

        checkPutGetAfter(() -> startGrid(1));

        grid(0).cluster().baselineAutoAdjustEnabled(false);

        checkPutGetAfter(() -> startGrid(3));
        checkPutGetAfter(() -> stopGrid(3));

        checkPutGetAfter(() -> stopGrid(1));

        checkPutGetAfter(() -> startClientGrid(2));
        checkPutGetAfter(() -> stopGrid(2));
    }

    /** */
    @Test
    public void testClientNodeSegmentationIgnored() throws Exception {
        IgniteEx srv = startGrid(0);

        startClientGrid(1);

        srv.cluster().baselineAutoAdjustEnabled(false);

        createCaches();

        failNode(1, Collections.singleton(srv));

        checkPutGet(Collections.singleton(srv), true);
    }

    /** */
    @Test
    public void testSplitWithoutBaseline() throws Exception {
        startGridsMultiThreaded(3);

        createCaches();

        startGrid(3);

        splitAndWait();

        checkPutGet(G.allGrids(), true);

        stopSegmentNodes(1);

        unsplit();

        grid(0).cluster().state(ACTIVE_READ_ONLY);

        grid(0).cluster().state(ACTIVE);

        checkPutGet(G.allGrids(), true);
    }

    /** */
    @Test
    public void testSplitWithBaseline() throws Exception {
        startGridsMultiThreaded(3);

        grid(0).cluster().baselineAutoAdjustEnabled(false);

        createCaches();

        startGrid(3);

        splitAndWait();

        checkPutGet(0, true);
        checkPutGet(1, false);

        assertTrue(waitForCondition(() -> ACTIVE_READ_ONLY == grid(1).cluster().state(), getTestTimeout()));

        stopSegmentNodes(1);

        unsplit();

        startGrid(1);
        startGrid(3);

        grid(0).cluster().setBaselineTopology(grid(0).cluster().topologyVersion());

        splitAndWait();

        checkPutGet(G.allGrids(), false);

        assertTrue(waitForCondition(() -> ACTIVE_READ_ONLY == grid(1).cluster().state(), getTestTimeout()));
        assertTrue(waitForCondition(() -> ACTIVE_READ_ONLY == grid(0).cluster().state(), getTestTimeout()));

        grid(0).cluster().state(ACTIVE);

        checkPutGet(0, true);
        checkPutGet(1, false);
    }

    /** */
    @Test
    public void testConsequentSegmentationResolving() throws Exception {
        startGridsMultiThreaded(4);

        grid(0).cluster().baselineAutoAdjustEnabled(false);

        createCaches();

        splitAndWait();

        checkPutGet(G.allGrids(), false);

        grid(1).cluster().state(ACTIVE);

        checkPutGet(0, false);
        checkPutGet(1, true);

        stopSegmentNodes(0);

        unsplit();

        failNode(1, Collections.singleton(grid(3)));

        checkPutGet(Collections.singleton(grid(3)), false);

        grid(3).cluster().state(ACTIVE);

        checkPutGet(Collections.singleton(grid(3)), true);
    }

    /** */
    @Test
    public void testEnableProperty() throws Exception {
        startGridsMultiThreaded(4);

        grid(0).cluster().baselineAutoAdjustEnabled(false);

        createCaches();

        grid(1).context().distributedConfiguration().property(SEG_RESOLVER_ENABLED_PROP_NAME).propagate(false);

        splitAndWait();

        checkPutGet(G.allGrids(), true);

        stopSegmentNodes(0);

        unsplit();

        grid(1).context().distributedConfiguration().property(SEG_RESOLVER_ENABLED_PROP_NAME).propagate(true);

        failNode(1, Collections.singleton(grid(3)));

        checkPutGet(Collections.singleton(grid(3)), false);
    }

    /** */
    @Test
    public void testNodeJoinWithHalfBaselineNodesLeft() throws Exception {
        startGridsMultiThreaded(4);

        grid(0).cluster().baselineAutoAdjustEnabled(false);

        createCaches();

        stopGrid(0);
        stopGrid(1);
        stopGrid(2);

        checkPutGet(G.allGrids(), true);

        startGrid(0);

        checkPutGet(G.allGrids(), true);
    }

    /** */
    @Test
    public void testNodeJoinConcurrentWithLeftRejected() throws Exception {
        IgniteEx srv = startGrids(2);

        grid(0).cluster().baselineAutoAdjustEnabled(false);

        createCaches();

        CountDownLatch discoveryWorkerBlockedLatch = new CountDownLatch(1);

        try {
            srv.events().localListen(evt -> {
                try {
                    discoveryWorkerBlockedLatch.await();
                }
                catch (InterruptedException e) {
                    U.error(log, e);
                }

                return true;
            }, EVT_NODE_JOINED);

            startGrid(2);

            stopGrid(1);

            assertThrowsAnyCause(
                log,
                () -> startGrid(3),
                IgniteSpiException.class,
                "Node join request will be rejected due to concurrent node left process handling"
            );
        }
        finally {
            discoveryWorkerBlockedLatch.countDown();
        }
    }

    /** */
    private IgniteEx connectNodeToSegment(int nodeIdx, int segment) throws Exception {
        IgniteConfiguration cfg = getConfiguration(getTestIgniteInstanceName(nodeIdx));

        List<String> segmentDiscoPorts = segmentNodes(segment, false).stream()
            .map(node -> "127.0.0.1:" + discoPort(node.localNode().<Integer>attribute(IDX_ATTR)))
            .collect(toList());

        ((TcpDiscoverySpi)cfg.getDiscoverySpi()).setIpFinder(new TcpDiscoveryVmIpFinder().setAddresses(segmentDiscoPorts));

        return startGrid(optimize(cfg));
    }

    /**  */
    private boolean isDiscoPort(int port) {
        return port >= DFLT_PORT &&
            port <= (DFLT_PORT + DFLT_PORT_RANGE);
    }

    /** */
    public void createCaches() {
        for (int cacheIdx = 0; cacheIdx < CACHE_CNT; cacheIdx++) {
            grid(0).createCache(new CacheConfiguration<>()
                .setName(cacheName(cacheIdx))
                .setCacheMode(REPLICATED)
                .setReadFromBackup(false)
                .setWriteSynchronizationMode(PRIMARY_SYNC)
                .setTopologyValidator(new IgniteCacheTopologyValidator())
            );
        }

        checkPutGet(G.allGrids(), true);
    }

    /** */
    private void checkPutGetAfter(RunnableX r) {
        r.run();

        checkPutGet(G.allGrids(), true);
    }

    /** */
    private void checkPutGet(int segment, boolean successfulPutExpected) {
        checkPutGet(segmentNodes(segment, true), successfulPutExpected);
    }

    /** */
    private String cacheName(int cacheIdx) {
        return DEFAULT_CACHE_NAME + '_' + cacheIdx;
    }

    /**  */
    private int discoPort(int idx) {
        return DFLT_PORT + idx;
    }

    /** */
    private void checkPutGet(Collection<? extends Ignite> nodes, boolean successfulPutExpected) {
        assertFalse(nodes.isEmpty());

        for (Ignite node : nodes) {
            for (int cacheIdx = 0; cacheIdx < CACHE_CNT; cacheIdx++) {
                IgniteCache<Object, Object> cache = node.cache(cacheName(cacheIdx));

                for (int i = 0; i < CACHE_KEY_CNT; i++) {
                    int key = i;

                    if (!successfulPutExpected) {
                        assertThrowsAnyCause(
                            null,
                            () -> {
                                cache.put(key, key);

                                return null;
                            },
                            CacheInvalidStateException.class,
                            "Failed to perform cache operation");
                    }
                    else
                        cache.put(key, key);

                    assertEquals(key, cache.get(key));
                }
            }
        }
    }

    /** */
    private void stopSegmentNodes(int segment) {
        for (IgniteEx node : segmentNodes(segment, true))
            stopGrid(node.name());
    }

    /** */
    private void failNode(int idx, Collection<Ignite> awaitingNodes) {
        assertFalse(awaitingNodes.isEmpty());

        long topVer = awaitingNodes.iterator().next().cluster().topologyVersion();

        TcpDiscoverySpi spi = (TcpDiscoverySpi)grid(idx).context().discovery().getInjectedDiscoverySpi();

        spi.setClientReconnectDisabled(true);
        spi.disconnect();

        awaitExchangeVersionFinished(awaitingNodes, topVer + 1);
    }

    /** */
    private Collection<IgniteEx> segmentNodes(int segment, boolean includeClients) {
        return G.allGrids().stream()
            .filter(ignite -> includeClients || !ignite.cluster().localNode().isClient())
            .filter(ignite -> segment(ignite.cluster().localNode()) == segment)
            .map(ignite -> (IgniteEx)ignite)
            .collect(Collectors.toList());
    }
}
