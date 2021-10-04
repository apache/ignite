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
import java.util.stream.Collectors;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.cache.validation.IgniteCacheTopologyValidator;
import org.apache.ignite.cache.validation.PluggableSegmentationResolver;
import org.apache.ignite.cache.validation.SegmentationResolverPluginProvider;
import org.apache.ignite.cluster.ClusterNode;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.internal.processors.cache.CacheInvalidStateException;
import org.apache.ignite.internal.processors.cache.distributed.dht.IgniteCacheTopologySplitAbstractTest;
import org.apache.ignite.internal.util.typedef.G;
import org.apache.ignite.spi.IgniteSpiException;
import org.apache.ignite.spi.discovery.tcp.TcpDiscoverySpi;
import org.apache.ignite.spi.discovery.tcp.ipfinder.vm.TcpDiscoveryVmIpFinder;
import org.apache.ignite.testframework.GridTestUtils.RunnableX;
import org.junit.Test;

import static java.util.Collections.singletonMap;
import static java.util.stream.Collectors.toList;
import static org.apache.ignite.cache.CacheWriteSynchronizationMode.PRIMARY_SYNC;
import static org.apache.ignite.cluster.ClusterState.ACTIVE;
import static org.apache.ignite.cluster.ClusterState.ACTIVE_READ_ONLY;
import static org.apache.ignite.internal.processors.cache.distributed.GridCacheModuloAffinityFunction.IDX_ATTR;
import static org.apache.ignite.spi.discovery.tcp.TcpDiscoverySpi.DFLT_PORT;
import static org.apache.ignite.spi.discovery.tcp.TcpDiscoverySpi.DFLT_PORT_RANGE;
import static org.apache.ignite.testframework.GridTestUtils.assertThrows;
import static org.apache.ignite.testframework.GridTestUtils.assertThrowsAnyCause;
import static org.apache.ignite.testframework.GridTestUtils.assertThrowsWithCause;
import static org.apache.ignite.testframework.GridTestUtils.waitForCondition;

/** */
@SuppressWarnings("ThrowableNotThrown")
public class SegmentationResolverTest extends IgniteCacheTopologySplitAbstractTest {
    /** */
    private static final int CACHE_KEY_CNT = 1000;

    /** */
    public static final int CACHE_CNT = 2;

    /** */
    @Override protected IgniteConfiguration getConfiguration(String igniteInstanceName) throws Exception {
        return getConfiguration(igniteInstanceName, false);
    }

    /** */
    private IgniteConfiguration getConfiguration(
        String igniteInstanceName,
        boolean skipSegmentationPluginConfiguration
    ) throws Exception {
        int idx = getTestIgniteInstanceIndex(igniteInstanceName);

        IgniteConfiguration cfg = super.getConfiguration(igniteInstanceName)
            .setUserAttributes(singletonMap(IDX_ATTR, idx));

        if (!skipSegmentationPluginConfiguration)
            cfg.setPluginProviders(new SegmentationResolverPluginProvider());

        ((TcpDiscoverySpi)cfg.getDiscoverySpi())
            .setIpFinder(sharedStaticIpFinder)
            .setLocalPortRange(1)
            .setLocalPort(getDiscoPort(idx))
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
        startGrid(getConfiguration(getTestIgniteInstanceName(0), true));

        assertThrows(
            log,
            () -> startGrid(1),
            IgniteCheckedException.class,
            "The segmentation resolver plugin is not configured for the cluster the current server node is trying to join."
        );
    }

    /** */
    @Test
    public void testIncompatibleNodeConnectionToCluster() throws Exception {
        IgniteEx srv = startGrid(0);

        assertThrowsAnyCause(
            log,
            () -> startGrid(getConfiguration(getTestIgniteInstanceName(1), true)),
            IgniteSpiException.class,
            "The segmentation resolver plugin is not configured for the server node that is trying to join the cluster."
        );

        startClientGrid(getConfiguration(getTestIgniteInstanceName(2), true));

        assertEquals(2, srv.cluster().nodes().size());
    }

    /** */
    @Test
    public void testMissingSegmentationResolverPlugin() throws Exception {
        IgniteEx srv = startGrid(super.getConfiguration(getTestIgniteInstanceName(0)));

        assertThrowsWithCause(
            () -> srv.createCache(new CacheConfiguration<>(DEFAULT_CACHE_NAME)
                .setTopologyValidator(new IgniteCacheTopologyValidator())),
            IgniteCheckedException.class
        );
    }

    /** */
    @Test
    public void testConnectionToSegmentedCluster() throws Exception {
        startGrids(4);

        splitAndWait();

        checkSegmentState(0, false);
        checkSegmentState(0, false);

        assertThrowsAnyCause(
            log,
            () -> connectNodeToSegment(4, 0),
            IgniteSpiException.class,
            "The node cannot join the cluster because the cluster was marked as segmented."
        );

        assertThrowsAnyCause(
            log,
            () -> connectNodeToSegment(5, 1),
            IgniteSpiException.class,
            "The node cannot join the cluster because the cluster was marked as segmented."
        );
    }

    /** */
    @Test
    public void testRegularNodeStartStop() throws Exception {
        startGrid(0);

        startCaches(2);

        checkPutGetAfter(() -> startGrid(1));
        checkPutGetAfter(() -> stopGrid(1));
        checkPutGetAfter(() -> startClientGrid(2));
        checkPutGetAfter(() -> stopGrid(2));
        checkPutGetAfter(() -> startGrid(1));

        grid(0).cluster().baselineAutoAdjustEnabled(false);

        checkPutGetAfter(() -> startGrid(3));
        checkPutGetAfter(() -> stopGrid(3));
        checkPutGetAfter(() -> stopGrid(1));
    }

    /** */
    @Test
    public void testClientNodeSegmentationIgnored() throws Exception {
        IgniteEx srv = startGrid(0);

        IgniteEx cli = startClientGrid(1);

        startCaches(0);

        checkPut(0, true);

        long topVer = srv.cluster().topologyVersion();

        TcpDiscoverySpi spi = (TcpDiscoverySpi)cli.context().discovery().getInjectedDiscoverySpi();

        spi.setClientReconnectDisabled(true);
        spi.disconnect();

        awaitExchangeVersionFinished(Collections.singleton(srv), topVer + 1);

        checkSegmentState(0, true);

        checkPut(0, true);
        checkGet(0);
    }

    /** */
    @Test
    public void testSplitWithBaseline() throws Exception {
        startGrids(3);

        grid(0).cluster().baselineAutoAdjustEnabled(false);

        startCaches(2);

        checkPut(0, true);

        startGrid(3);

        splitAndWait();

        checkPut(0, true);
        checkGet(0);

        checkPut(1, false);
        checkGet(1);

        assertTrue(waitForCondition(() -> ACTIVE_READ_ONLY == grid(1).cluster().state(), getTestTimeout()));
    }

    /** */
    @Test
    public void testSplitWithoutBaseline() throws Exception {
        startGrids(3);

        startCaches(2);

        checkPut(0, true);

        splitAndWait();

        checkPut(1, false);
        checkGet(1);

        checkPut(0, true);
        checkGet(0);

        assertTrue(waitForCondition(() -> ACTIVE_READ_ONLY == grid(1).cluster().state(), getTestTimeout()));

        stopGrid(1);

        unsplit();

        startGrid(1);
        startGrid(3);

        awaitPartitionMapExchange();

        splitAndWait();

        checkPut(1, false);
        checkGet(1);

        checkPut(0, false);
        checkGet(0);

        assertTrue(waitForCondition(() -> ACTIVE_READ_ONLY == grid(1).cluster().state(), getTestTimeout()));
        assertTrue(waitForCondition(() -> ACTIVE_READ_ONLY == grid(0).cluster().state(), getTestTimeout()));

        grid(0).cluster().state(ACTIVE);

        checkPut(0, true);
        checkGet(0);

        checkPut(1, false);
        checkGet(1);
    }

    /** */
    private IgniteEx connectNodeToSegment(int nodeIdx, int segment) throws Exception {
        IgniteConfiguration cfg = getConfiguration(getTestIgniteInstanceName(nodeIdx));

        List<String> segmentDiscoPorts = segmentNodes(segment, false).stream()
            .map(node -> "127.0.0.1:" + getDiscoPort(node.localNode().<Integer>attribute(IDX_ATTR)))
            .collect(toList());

        ((TcpDiscoverySpi)cfg.getDiscoverySpi()).setIpFinder(new TcpDiscoveryVmIpFinder().setAddresses(segmentDiscoPorts));

        return startGrid(optimize(cfg));
    }

    /** */
    private void checkSegmentState(int segment, boolean isValid) {
        for (IgniteEx node : segmentNodes(segment, false)) {
            assertEquals(isValid, node.context().plugins()
                .extensions(PluggableSegmentationResolver.class)[0]
                .isValidSegment());
        }
    }

    /**  */
    private boolean isDiscoPort(int port) {
        return port >= DFLT_PORT &&
            port <= (DFLT_PORT + DFLT_PORT_RANGE);
    }

    /** */
    public void startCaches(int backups) {
        for (int cacheIdx = 0; cacheIdx < CACHE_CNT; cacheIdx++) {
            grid(0).createCache(new CacheConfiguration<>()
                .setName(cacheName(cacheIdx))
                .setBackups(backups)
                .setWriteSynchronizationMode(PRIMARY_SYNC)
                .setTopologyValidator(new IgniteCacheTopologyValidator())
            );
        }
    }

    /** */
    private String cacheName(int cacheIdx) {
        return DEFAULT_CACHE_NAME + '_' + cacheIdx;
    }

    /** */
    private void checkPutGetAfter(RunnableX r) {
        r.run();

        checkPut(0, true);

        checkGet(0);
    }

    /**  */
    private int getDiscoPort(int idx) {
        return DFLT_PORT + idx;
    }

    /** */
    private void checkPut(int segment, boolean success) {
        Collection<IgniteEx> segmentNodes = segmentNodes(segment, true);

        for (Ignite node : segmentNodes) {
            for (int cacheIdx = 0; cacheIdx < CACHE_CNT; cacheIdx++) {
                IgniteCache<Object, Object> cache = node.cache(cacheName(cacheIdx));

                for (int i = 0; i < CACHE_KEY_CNT; i++) {
                    int key = i;

                    if (!success) {
                        assertThrowsAnyCause(
                            log,
                            () -> {
                                cache.put(key, key);

                                return null;
                            },
                            CacheInvalidStateException.class,
                            "Failed to perform cache operation");
                    }
                    else
                        cache.put(key, key);
                }
            }
        }
    }

    /** */
    private void checkGet(int segment) {
        Collection<IgniteEx> segmentNodes = segmentNodes(segment, true);

        for (int cacheIdx = 0; cacheIdx < CACHE_CNT; cacheIdx++) {
            for (Ignite node : segmentNodes) {
                IgniteCache<Object, Object> cache = node.cache(cacheName(cacheIdx));

                for (int key = 0; key < CACHE_KEY_CNT; key++)
                    assertEquals(key, cache.get(key));
            }
        }
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
