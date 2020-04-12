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

import java.util.Collection;
import java.util.Collections;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import com.google.common.collect.Sets;
import org.apache.ignite.cluster.ClusterNode;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.DataRegionConfiguration;
import org.apache.ignite.configuration.DataStorageConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.events.Event;
import org.apache.ignite.events.EventType;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.internal.processors.cache.DynamicCacheDescriptor;
import org.apache.ignite.internal.util.typedef.internal.CU;
import org.apache.ignite.lang.IgniteFuture;
import org.apache.ignite.lang.IgnitePredicate;
import org.apache.ignite.spi.discovery.DiscoverySpiCustomMessage;
import org.apache.ignite.spi.discovery.DiscoverySpiListener;
import org.apache.ignite.spi.discovery.tcp.TcpDiscoverySpi;
import org.apache.ignite.spi.discovery.tcp.ipfinder.vm.TcpDiscoveryVmIpFinder;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.jetbrains.annotations.Nullable;
import org.junit.Assert;

import static org.apache.ignite.events.EventType.EVT_CLIENT_NODE_DISCONNECTED;

/**
 *
 */
public abstract class IgniteAbstractStandByClientReconnectTest extends GridCommonAbstractTest {
    /** */
    private static final TcpDiscoveryVmIpFinder vmIpFinder = new TcpDiscoveryVmIpFinder(true);

    /** */
    private static final TcpDiscoveryVmIpFinder clientIpFinder = new TcpDiscoveryVmIpFinder(true);

    /** */
    protected static final String NODE_1 = "node1";

    /** */
    protected static final String NODE_2 = "node2";

    /** */
    protected static final String NODE_CLIENT = "nodeClient";

    /** */
    protected static final String CCFG_1_STATIC_NAME = "cache1static";

    /** */
    protected static final String CCFG_2_STATIC_NAME = "cache2static";

    /** */
    protected static final String CCFG_3_STATIC_NAME = "cache3static";

    /** */
    protected static final String CCFG_1_STATIC_WITH_FILTER_NAME = "ccfg1staticWithFilter";

    /** */
    protected static final String CCFG_2_STATIC_WITH_FILTER_NAME = "ccfg2staticWithFilter";

    /** */
    protected static final String CCFG_3_STATIC_WITH_FILTER_NAME = "ccfg3staticWithFilter";

    /** */
    protected static final String CCFG_DYNAMIC_NAME = "ccfgDynamic";

    /** */
    protected static final String CCFG_DYNAMIC_WITH_FILTER_NAME = "ccfgDynamicWithFilter";

    /** */
    protected final CacheConfiguration ccfg1static = new CacheConfiguration(CCFG_1_STATIC_NAME);

    /** */
    protected final CacheConfiguration ccfg2static = new CacheConfiguration(CCFG_2_STATIC_NAME);

    /** */
    protected final CacheConfiguration ccfg3static = new CacheConfiguration(CCFG_3_STATIC_NAME);

    /** */
    protected final CacheConfiguration ccfg1staticWithFilter =
        new CacheConfiguration(CCFG_1_STATIC_WITH_FILTER_NAME).setNodeFilter(new FilterNode(NODE_2));

    /** */
    protected final CacheConfiguration ccfg2staticWithFilter =
        new CacheConfiguration(CCFG_2_STATIC_WITH_FILTER_NAME).setNodeFilter(new FilterNode(NODE_CLIENT));

    /** */
    protected final CacheConfiguration ccfg3staticWithFilter =
        new CacheConfiguration(CCFG_3_STATIC_WITH_FILTER_NAME).setNodeFilter(new FilterNode(NODE_1));

    /** */
    protected final CacheConfiguration<Object, Object> ccfgDynamic = new CacheConfiguration<>(CCFG_DYNAMIC_NAME);

    /** */
    protected final CacheConfiguration<Object, Object> ccfgDynamicWithFilter =
        new CacheConfiguration<>(CCFG_DYNAMIC_WITH_FILTER_NAME).setNodeFilter(new FilterNode(NODE_2));

    /** */
    protected final Set<String> staticCacheNames = Sets.newHashSet(
        CCFG_1_STATIC_NAME, CCFG_2_STATIC_NAME, CCFG_3_STATIC_NAME,
        CCFG_1_STATIC_WITH_FILTER_NAME, CCFG_2_STATIC_WITH_FILTER_NAME, CCFG_3_STATIC_WITH_FILTER_NAME
    );

    /** */
    protected final Set<String> allCacheNames = Sets.newHashSet(
        CCFG_1_STATIC_NAME, CCFG_2_STATIC_NAME, CCFG_3_STATIC_NAME,
        CCFG_1_STATIC_WITH_FILTER_NAME, CCFG_2_STATIC_WITH_FILTER_NAME, CCFG_3_STATIC_WITH_FILTER_NAME,
        CCFG_DYNAMIC_NAME, CCFG_DYNAMIC_WITH_FILTER_NAME
    );

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String name) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(name);

        cfg.setAutoActivationEnabled(false);

        if (!NODE_CLIENT.equals(name))
            cfg.setDiscoverySpi(new TcpDiscoverySpi().setIpFinder(vmIpFinder));
        else {
            clientIpFinder.setAddresses(Collections.singletonList("127.0.0.1:47501"));

            cfg.setDiscoverySpi(new TcpDiscoverySpi().setIpFinder(clientIpFinder));
        }

        cfg.setDataStorageConfiguration(new DataStorageConfiguration()
            .setDefaultDataRegionConfiguration(new DataRegionConfiguration()
                .setMaxSize(100L * 1024 * 1024)
                .setPersistenceEnabled(true)));

        cfg.setConsistentId(name);

        return cfg;
    }

    /**
     * @param disconnectedLatch Disconnect latch. Will be fired when client disconnect event is received.
     * @param reconnectedLatch Reconnect latch. Will be fired when cilent reconnect event is receoved.
     */
    protected void addDisconnectListener(
        final CountDownLatch disconnectedLatch,
        final CountDownLatch reconnectedLatch
    ) {
        grid(NODE_CLIENT).events().localListen(new IgnitePredicate<Event>() {
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

    /**
     * @param ig Ignite.
     * @param cacheNames Cache names.
     */
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

    /**
     * @param activateLatch Activate latch. Will be fired when cluster is activated.
     * @throws Exception If failed.
     */
    protected void startNodes(CountDownLatch activateLatch) throws Exception {
        IgniteConfiguration cfg1 = getConfiguration(NODE_1)
            .setCacheConfiguration(ccfg1static, ccfg1staticWithFilter);

        IgniteConfiguration cfg2 = getConfiguration(NODE_2)
            .setCacheConfiguration(ccfg2static, ccfg2staticWithFilter);

        IgniteConfiguration cfg3 = getConfiguration(NODE_CLIENT)
            .setCacheConfiguration(ccfg3static, ccfg3staticWithFilter);

        if (activateLatch != null)
            cfg3.setDiscoverySpi(
                new AwaitTcpDiscoverySpi(activateLatch)
                    .setIpFinder(clientIpFinder)
            );

        startGrid(cfg1);
        startGrid(cfg2);
        startClientGrid(cfg3);
    }

    /**
     *
     */
    protected void checkStaticCaches() {
        IgniteEx ig1 = grid(NODE_1);
        IgniteEx ig2 = grid(NODE_2);
        IgniteEx client = grid(NODE_CLIENT);

        Assert.assertNotNull(ig1.cache(CCFG_1_STATIC_NAME));
        Assert.assertNotNull(ig1.cache(CCFG_2_STATIC_NAME));
        Assert.assertNotNull(ig1.cache(CCFG_3_STATIC_NAME));

        Assert.assertNotNull(ig1.cache(CCFG_1_STATIC_WITH_FILTER_NAME));
        Assert.assertNotNull(ig1.cache(CCFG_2_STATIC_WITH_FILTER_NAME));

        Assert.assertNotNull(ig2.cache(CCFG_1_STATIC_NAME));
        Assert.assertNotNull(ig2.cache(CCFG_2_STATIC_NAME));
        Assert.assertNotNull(ig2.cache(CCFG_3_STATIC_NAME));

        Assert.assertNotNull(ig2.cache(CCFG_3_STATIC_WITH_FILTER_NAME));
        Assert.assertNotNull(ig2.cache(CCFG_2_STATIC_WITH_FILTER_NAME));

        Assert.assertNotNull(client.cache(CCFG_1_STATIC_NAME));
        Assert.assertNotNull(client.cache(CCFG_2_STATIC_NAME));
        Assert.assertNotNull(client.cache(CCFG_3_STATIC_NAME));

        Assert.assertNotNull(client.cache(CCFG_3_STATIC_WITH_FILTER_NAME));
        Assert.assertNotNull(client.cache(CCFG_1_STATIC_WITH_FILTER_NAME));
    }

    /**
     *
     */
    protected void checkAllCaches() {
        IgniteEx ig1 = grid(NODE_1);
        IgniteEx ig2 = grid(NODE_2);
        IgniteEx client = grid(NODE_CLIENT);

        checkStaticCaches();

        Assert.assertNotNull(ig1.cache(CCFG_DYNAMIC_NAME));
        Assert.assertNotNull(ig1.cache(CCFG_DYNAMIC_WITH_FILTER_NAME));

        Assert.assertNotNull(ig2.cache(CCFG_DYNAMIC_NAME));

        Assert.assertNotNull(client.cache(CCFG_DYNAMIC_NAME));
        Assert.assertNotNull(client.cache(CCFG_DYNAMIC_WITH_FILTER_NAME));
    }

    /**
     * @param checkClientCaches Check presence of client caches, false to skip.
     */
    protected void checkOnlySystemCaches(boolean checkClientCaches) {
        IgniteEx ig1 = grid(NODE_1);
        IgniteEx ig2 = grid(NODE_2);
        IgniteEx client = grid(NODE_CLIENT);

        Assert.assertNull(ig1.cache(CCFG_1_STATIC_NAME));
        Assert.assertNull(ig1.cache(CCFG_2_STATIC_NAME));

        Assert.assertNull(ig1.cache(CCFG_1_STATIC_WITH_FILTER_NAME));
        Assert.assertNull(ig1.cache(CCFG_2_STATIC_WITH_FILTER_NAME));

        Assert.assertNull(ig2.cache(CCFG_1_STATIC_NAME));
        Assert.assertNull(ig2.cache(CCFG_2_STATIC_NAME));

        Assert.assertNull(ig2.cache(CCFG_2_STATIC_WITH_FILTER_NAME));

        Assert.assertNull(client.cache(CCFG_1_STATIC_NAME));
        Assert.assertNull(client.cache(CCFG_2_STATIC_NAME));

        Assert.assertNull(client.cache(CCFG_1_STATIC_WITH_FILTER_NAME));

        if (checkClientCaches) {
            Assert.assertNull(ig1.cache(CCFG_3_STATIC_NAME));

            Assert.assertNull(ig2.cache(CCFG_3_STATIC_NAME));
            Assert.assertNull(ig2.cache(CCFG_3_STATIC_WITH_FILTER_NAME));

            Assert.assertNull(client.cache(CCFG_3_STATIC_NAME));
            Assert.assertNull(client.cache(CCFG_3_STATIC_WITH_FILTER_NAME));
        }

        Set cachesToCheck = checkClientCaches ? Collections.emptySet()
            : Sets.newHashSet(CCFG_3_STATIC_NAME, CCFG_3_STATIC_WITH_FILTER_NAME);

        checkDescriptors(ig1, cachesToCheck);
        checkDescriptors(ig2, cachesToCheck);
        checkDescriptors(client, cachesToCheck);
    }

    /**
     *
     */
    private static class FilterNode implements IgnitePredicate<ClusterNode> {
        /** */
        private final String consistentId;

        /**
         * @param id Consistent ID.
         */
        private FilterNode(String id) {
            consistentId = id;
        }

        /** {@inheritDoc} */
        @Override public boolean apply(ClusterNode node) {
            return !consistentId.equals(node.consistentId());
        }
    }

    /**
     *
     */
    private static class AwaitTcpDiscoverySpi extends TcpDiscoverySpi {
        /** */
        private final CountDownLatch latch;

        /**
         * @param latch Latch.
         */
        private AwaitTcpDiscoverySpi(CountDownLatch latch) {
            this.latch = latch;
        }

        /**
         * @param lsnr Listener.
         */
        @Override public void setListener(@Nullable DiscoverySpiListener lsnr) {
            super.setListener(new AwaitDiscoverySpiListener(latch, lsnr));
        }
    }

    /**
     *
     */
    private static class AwaitDiscoverySpiListener implements DiscoverySpiListener {
        /** */
        private final CountDownLatch latch;

        /** */
        private final DiscoverySpiListener delegate;

        /**
         * @param latch Latch.
         * @param delegate Delegate.
         */
        private AwaitDiscoverySpiListener(
            CountDownLatch latch,
            DiscoverySpiListener delegate
        ) {
            this.latch = latch;
            this.delegate = delegate;
        }

        /** {@inheritDoc} */
        @Override public void onLocalNodeInitialized(ClusterNode locNode) {
            delegate.onLocalNodeInitialized(locNode);
        }

        /** {@inheritDoc} */
        @Override public IgniteFuture<?> onDiscovery(
            int type,
            long topVer,
            ClusterNode node,
            Collection<ClusterNode> topSnapshot,
            @Nullable Map<Long, Collection<ClusterNode>> topHist,
            @Nullable DiscoverySpiCustomMessage data
        ) {
            IgniteFuture<?> fut = delegate.onDiscovery(type, topVer, node, topSnapshot, topHist, data);

            if (type == EVT_CLIENT_NODE_DISCONNECTED) {
                try {
                    System.out.println("Await cluster change state");

                    latch.await();
                }
                catch (InterruptedException e) {
                    throw new RuntimeException(e);
                }
            }

            return fut;
        }
    }

    /** {@inheritDoc} */
    @Override protected void beforeTest() throws Exception {
        super.beforeTest();

        stopAllGrids();

        cleanPersistenceDir();
    }

    /** {@inheritDoc} */
    @Override protected void afterTest() throws Exception {
        super.beforeTest();

        stopAllGrids();

        cleanPersistenceDir();
    }
}
