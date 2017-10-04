/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.ignite.internal.processors.cache.persistence.standbycluster.reconnect;

import com.google.common.collect.Sets;
import java.util.Collection;
import java.util.Collections;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import org.apache.ignite.cluster.ClusterNode;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.configuration.PersistentStoreConfiguration;
import org.apache.ignite.events.Event;
import org.apache.ignite.events.EventType;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.internal.processors.cache.DynamicCacheDescriptor;
import org.apache.ignite.internal.util.typedef.internal.CU;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.lang.IgnitePredicate;
import org.apache.ignite.spi.discovery.DiscoverySpiCustomMessage;
import org.apache.ignite.spi.discovery.DiscoverySpiListener;
import org.apache.ignite.spi.discovery.tcp.TcpDiscoverySpi;
import org.apache.ignite.spi.discovery.tcp.ipfinder.vm.TcpDiscoveryVmIpFinder;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.jetbrains.annotations.Nullable;
import org.junit.Assert;

import static org.apache.ignite.events.EventType.EVT_CLIENT_NODE_DISCONNECTED;
import static org.apache.ignite.internal.processors.cache.persistence.file.FilePageStoreManager.DFLT_STORE_DIR;

/**
 *
 */
public abstract class IgniteAbstractStandByClientReconnectTest extends GridCommonAbstractTest {

    private static final TcpDiscoveryVmIpFinder vmIpFinder = new TcpDiscoveryVmIpFinder(true);

    private static final TcpDiscoveryVmIpFinder clientIpFinder = new TcpDiscoveryVmIpFinder(true);

    protected final String node1 = "node1";
    protected final String node2 = "node2";
    protected final String nodeClient = "nodeClient";

    protected final String ccfg1staticName = "cache1static";
    protected final String ccfg2staticName = "cache2static";
    protected final String ccfg3staticName = "cache3static";

    protected final String ccfg1staticWithFilterName = "ccfg1staticWithFilter";
    protected final String ccfg2staticWithFilterName = "ccfg2staticWithFilter";
    protected final String ccfg3staticWithFilterName = "ccfg3staticWithFilter";

    protected final String ccfgDynamicName = "ccfgDynamic";
    protected final String ccfgDynamicWithFilterName = "ccfgDynamicWithFilter";

    protected final CacheConfiguration ccfg1static = new CacheConfiguration(ccfg1staticName);
    protected final CacheConfiguration ccfg2static = new CacheConfiguration(ccfg2staticName);
    protected final CacheConfiguration ccfg3static = new CacheConfiguration(ccfg3staticName);

    protected final CacheConfiguration ccfg1staticWithFilter =
        new CacheConfiguration(ccfg1staticWithFilterName).setNodeFilter(new FilterNode(node2));

    protected final CacheConfiguration ccfg2staticWithFilter =
        new CacheConfiguration(ccfg2staticWithFilterName).setNodeFilter(new FilterNode(nodeClient));

    protected final CacheConfiguration ccfg3staticWithFilter =
        new CacheConfiguration(ccfg3staticWithFilterName).setNodeFilter(new FilterNode(node1));

    protected final CacheConfiguration ccfgDynamic = new CacheConfiguration(ccfgDynamicName);

    protected final CacheConfiguration ccfgDynamicWithFilter =
        new CacheConfiguration(ccfgDynamicWithFilterName).setNodeFilter(new FilterNode(node2));

    protected final Set<String> staticCacheNames = Sets.newHashSet(
        ccfg1staticName, ccfg2staticName, ccfg3staticName,
        ccfg1staticWithFilterName, ccfg2staticWithFilterName, ccfg3staticWithFilterName
    );

    protected final Set<String> allCacheNames = Sets.newHashSet(
        ccfg1staticName, ccfg2staticName, ccfg3staticName,
        ccfg1staticWithFilterName, ccfg2staticWithFilterName, ccfg3staticWithFilterName,
        ccfgDynamicName, ccfgDynamicWithFilterName
    );

    @Override protected IgniteConfiguration getConfiguration(String name) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(name);

        if (!nodeClient.equals(name))
            cfg.setDiscoverySpi(new TcpDiscoverySpi().setIpFinder(vmIpFinder));
        else {
            clientIpFinder.setAddresses(Collections.singletonList("127.0.0.1:47501"));

            cfg.setDiscoverySpi(new TcpDiscoverySpi().setIpFinder(clientIpFinder));
        }

        cfg.setPersistentStoreConfiguration(new PersistentStoreConfiguration());
        cfg.setConsistentId(name);

        return cfg;
    }

    protected void addDisconnectListener(
        final CountDownLatch disconnectedLatch,
        final CountDownLatch reconnectedLatch
    ) {
        grid(nodeClient).events().localListen(new IgnitePredicate<Event>() {
            @Override public boolean apply(Event event) {
                switch (event.type()) {
                    case EventType.EVT_CLIENT_NODE_DISCONNECTED:
                        info("Client disconnected");

                        disconnectedLatch.countDown();

                        break;
                    case EventType.EVT_CLIENT_NODE_RECONNECTED:
                        info("Client reconnected");

                        reconnectedLatch.countDown();
                }

                return true;
            }
        }, EventType.EVT_CLIENT_NODE_DISCONNECTED, EventType.EVT_CLIENT_NODE_RECONNECTED);
    }

    protected void checkDescriptors(IgniteEx ig, Set<String> cacheNames) {
        Collection<DynamicCacheDescriptor> descs = ig.context().cache().cacheDescriptors().values();

        assertEquals("Node name: " + ig.name(), cacheNames.size() + 1, descs.size());

        int systemCnt = 0;

        for (DynamicCacheDescriptor desc : descs)
            if (!CU.isSystemCache(desc.cacheName()))
                assertTrue(desc.cacheName(), cacheNames.contains(desc.cacheName()));
            else
                systemCnt++;

        assertEquals(1, systemCnt);
    }

    protected void startNodes(CountDownLatch activateLatch) throws Exception {
        IgniteConfiguration cfg1 = getConfiguration(node1)
            .setCacheConfiguration(ccfg1static, ccfg1staticWithFilter);

        IgniteConfiguration cfg2 = getConfiguration(node2)
            .setCacheConfiguration(ccfg2static, ccfg2staticWithFilter);

        IgniteConfiguration cfg3 = getConfiguration(nodeClient)
            .setCacheConfiguration(ccfg3static, ccfg3staticWithFilter);

        if (activateLatch != null)
            cfg3.setDiscoverySpi(
                new AwaitTcpDiscoverySpi(activateLatch)
                    .setIpFinder(clientIpFinder)
            );

        cfg3.setClientMode(true);

        IgniteEx ig1 = startGrid(cfg1);
        IgniteEx ig2 = startGrid(cfg2);
        IgniteEx client = startGrid(cfg3);
    }

    protected void checkStaticCaches() {
        IgniteEx ig1 = grid(node1);
        IgniteEx ig2 = grid(node2);
        IgniteEx client = grid(nodeClient);

        Assert.assertNotNull(ig1.cache(ccfg1staticName));
        Assert.assertNotNull(ig1.cache(ccfg2staticName));
        Assert.assertNotNull(ig1.cache(ccfg3staticName));

        Assert.assertNotNull(ig1.cache(ccfg1staticWithFilterName));
        Assert.assertNotNull(ig1.cache(ccfg2staticWithFilterName));

        Assert.assertNotNull(ig2.cache(ccfg1staticName));
        Assert.assertNotNull(ig2.cache(ccfg2staticName));
        Assert.assertNotNull(ig2.cache(ccfg3staticName));

        Assert.assertNotNull(ig2.cache(ccfg3staticWithFilterName));
        Assert.assertNotNull(ig2.cache(ccfg2staticWithFilterName));

        Assert.assertNotNull(client.cache(ccfg1staticName));
        Assert.assertNotNull(client.cache(ccfg2staticName));
        Assert.assertNotNull(client.cache(ccfg3staticName));

        Assert.assertNotNull(client.cache(ccfg3staticWithFilterName));
        Assert.assertNotNull(client.cache(ccfg1staticWithFilterName));
    }

    protected void checkAllCaches() {
        IgniteEx ig1 = grid(node1);
        IgniteEx ig2 = grid(node2);
        IgniteEx client = grid(nodeClient);

        checkStaticCaches();

        Assert.assertNotNull(ig1.cache(ccfgDynamicName));
        Assert.assertNotNull(ig1.cache(ccfgDynamicWithFilterName));

        Assert.assertNotNull(ig2.cache(ccfgDynamicName));

        Assert.assertNotNull(client.cache(ccfgDynamicName));
        Assert.assertNotNull(client.cache(ccfgDynamicWithFilterName));
    }

    protected void checkOnlySystemCaches() {
        IgniteEx ig1 = grid(node1);
        IgniteEx ig2 = grid(node2);
        IgniteEx client = grid(nodeClient);

        Assert.assertNull(ig1.cache(ccfg1staticName));
        Assert.assertNull(ig1.cache(ccfg2staticName));
        Assert.assertNull(ig1.cache(ccfg3staticName));

        Assert.assertNull(ig1.cache(ccfg1staticWithFilterName));
        Assert.assertNull(ig1.cache(ccfg2staticWithFilterName));

        Assert.assertNull(ig2.cache(ccfg1staticName));
        Assert.assertNull(ig2.cache(ccfg2staticName));
        Assert.assertNull(ig2.cache(ccfg3staticName));

        Assert.assertNull(ig2.cache(ccfg3staticWithFilterName));
        Assert.assertNull(ig2.cache(ccfg2staticWithFilterName));

        Assert.assertNull(client.cache(ccfg1staticName));
        Assert.assertNull(client.cache(ccfg2staticName));
        Assert.assertNull(client.cache(ccfg3staticName));

        Assert.assertNull(client.cache(ccfg3staticWithFilterName));
        Assert.assertNull(client.cache(ccfg1staticWithFilterName));

        checkDescriptors(ig1,Collections.<String>emptySet());
        checkDescriptors(ig2,Collections.<String>emptySet());
        checkDescriptors(client, Collections.<String>emptySet());
    }

    private static class FilterNode implements IgnitePredicate<ClusterNode> {

        private final String consistentId;

        private FilterNode(String id) {
            consistentId = id;
        }

        @Override public boolean apply(ClusterNode node) {
            return !consistentId.equals(node.consistentId());
        }
    }

    private static class AwaitTcpDiscoverySpi extends TcpDiscoverySpi {

        private final CountDownLatch latch;

        private AwaitTcpDiscoverySpi(CountDownLatch latch) {
            this.latch = latch;
        }

        @Override public void setListener(@Nullable DiscoverySpiListener lsnr) {
            super.setListener(new AwaitDiscoverySpiListener(latch, lsnr));
        }
    }

    private static class AwaitDiscoverySpiListener implements DiscoverySpiListener {

        private final CountDownLatch latch;

        private final DiscoverySpiListener delegate;

        private AwaitDiscoverySpiListener(
            CountDownLatch latch,
            DiscoverySpiListener delegate
        ) {
            this.latch = latch;
            this.delegate = delegate;
        }

        @Override public void onLocalNodeInitialized(ClusterNode locNode) {
            delegate.onLocalNodeInitialized(locNode);
        }

        @Override public void onDiscovery(
            int type,
            long topVer,
            ClusterNode node,
            Collection<ClusterNode> topSnapshot,
            @Nullable Map<Long, Collection<ClusterNode>> topHist,
            @Nullable DiscoverySpiCustomMessage data
        ) {
            delegate.onDiscovery(type, topVer, node, topSnapshot, topHist, data);

            if (type == EVT_CLIENT_NODE_DISCONNECTED)
                try {
                    System.out.println("Await cluster change state");

                    latch.await();
                }
                catch (InterruptedException e) {
                    throw new RuntimeException(e);
                }
        }
    }

    @Override protected void beforeTest() throws Exception {
        super.beforeTest();

        stopAllGrids();

        deleteRecursively(U.resolveWorkDirectory(U.defaultWorkDirectory(), DFLT_STORE_DIR, true));
    }

    @Override protected void afterTest() throws Exception {
        super.beforeTest();

        stopAllGrids();

        deleteRecursively(U.resolveWorkDirectory(U.defaultWorkDirectory(), DFLT_STORE_DIR, true));
    }

}
