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

package org.apache.ignite.internal.processors.cache;

import org.apache.ignite.Ignite;
import org.apache.ignite.cluster.ClusterGroup;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.IgniteKernal;
import org.apache.ignite.spi.discovery.DiscoverySpi;
import org.apache.ignite.spi.discovery.tcp.TcpDiscoverySpi;
import org.apache.ignite.spi.discovery.tcp.ipfinder.TcpDiscoveryIpFinder;
import org.apache.ignite.spi.discovery.tcp.ipfinder.vm.TcpDiscoveryVmIpFinder;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;

/**
 * Tests of cache related cluster projections for daemon node.
 */
public class GridProjectionForCachesOnDaemonNodeSelfTest extends GridCommonAbstractTest {
    /** Ip finder. */
    private static final TcpDiscoveryIpFinder IP_FINDER = new TcpDiscoveryVmIpFinder(true);

    /** Daemon node. */
    private static boolean daemonNode;

    /** Daemon. */
    private static Ignite ignite;

    /** Daemon. */
    private static Ignite daemon;

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String igniteInstanceName) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(igniteInstanceName);

        cfg.setDiscoverySpi(discoverySpi());

        cfg.setDaemon(daemonNode);

        return cfg;
    }

    /**
     * @return Discovery SPI;
     */
    private DiscoverySpi discoverySpi() {
        TcpDiscoverySpi spi = new TcpDiscoverySpi();

        spi.setIpFinder(IP_FINDER);

        return spi;
    }

    /** {@inheritDoc} */
    @Override protected void beforeTestsStarted() throws Exception {
        ignite = startGrid(0);

        daemonNode = true;

        daemon = startGrid(1);

        assert ((IgniteKernal)daemon).localNode().isDaemon();
    }

    /** {@inheritDoc} */
    @Override protected void beforeTest() throws Exception {
        ignite.getOrCreateCache(DEFAULT_CACHE_NAME);
    }

    /** {@inheritDoc} */
    @Override protected void afterTest() throws Exception {
        ignite.cache(DEFAULT_CACHE_NAME).destroy();
    }

    /**
     * @throws Exception If failed.
     */
    public void testForDataNodes() throws Exception {
        ClusterGroup grp = ignite.cluster().forDataNodes(DEFAULT_CACHE_NAME);

        assertFalse(grp.nodes().isEmpty());

        try {
            daemon.cluster().forDataNodes(DEFAULT_CACHE_NAME);
        }
        catch (IllegalStateException ignored) {
            return;
        }

        fail();
    }

    /**
     * @throws Exception If failed.
     */
    public void testForClientNodes() throws Exception {
        ClusterGroup grp = ignite.cluster().forClientNodes(DEFAULT_CACHE_NAME);

        assertTrue(grp.nodes().isEmpty());

        try {
            daemon.cluster().forClientNodes(DEFAULT_CACHE_NAME);
        }
        catch (IllegalStateException ignored) {
            return;
        }

        fail();
    }

    /**
     * @throws Exception If failed.
     */
    public void testForCacheNodes() throws Exception {
        ClusterGroup grp = ignite.cluster().forCacheNodes(DEFAULT_CACHE_NAME);

        assertFalse(grp.nodes().isEmpty());

        try {
            daemon.cluster().forCacheNodes(DEFAULT_CACHE_NAME);
        }
        catch (IllegalStateException ignored) {
            return;
        }

        fail();
    }
}
