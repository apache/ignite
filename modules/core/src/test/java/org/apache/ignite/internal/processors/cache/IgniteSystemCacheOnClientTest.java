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

import java.util.Collection;
import org.apache.ignite.cluster.ClusterNode;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.IgniteKernal;
import org.apache.ignite.internal.util.lang.GridAbsPredicate;
import org.apache.ignite.internal.util.typedef.internal.CU;
import org.apache.ignite.spi.discovery.tcp.TcpDiscoverySpi;
import org.apache.ignite.spi.discovery.tcp.ipfinder.TcpDiscoveryIpFinder;
import org.apache.ignite.spi.discovery.tcp.ipfinder.vm.TcpDiscoveryVmIpFinder;
import org.apache.ignite.testframework.GridTestUtils;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;

/**
 *
 */
public class IgniteSystemCacheOnClientTest extends GridCommonAbstractTest {
    /** */
    private static TcpDiscoveryIpFinder ipFinder = new TcpDiscoveryVmIpFinder(true);

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String gridName) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(gridName);

        ((TcpDiscoverySpi)cfg.getDiscoverySpi()).setIpFinder(ipFinder);

        if (gridName.equals(getTestGridName(1)))
            cfg.setClientMode(true);

        return cfg;
    }

    /** {@inheritDoc} */
    @Override protected void afterTestsStopped() throws Exception {
        super.afterTestsStopped();

        stopAllGrids();
    }

    /**
     * @throws Exception If failed.
     */
    public void testSystemCacheOnClientNode() throws Exception {
        startGrids(2);

        final IgniteKernal ignite = (IgniteKernal)ignite(1);

        assertTrue(ignite.configuration().isClientMode());

        GridTestUtils.waitForCondition(new GridAbsPredicate() {
            @Override public boolean apply() {
                return ignite.internalCache(CU.MARSH_CACHE_NAME) != null;
            }
        }, 5000);

        GridCacheAdapter marshCache = ignite.internalCache(CU.MARSH_CACHE_NAME);

        assertNotNull(marshCache);

        assertFalse(marshCache.context().isNear());

        marshCache = ((IgniteKernal)ignite(0)).internalCache(CU.MARSH_CACHE_NAME);

        assertFalse(marshCache.context().isNear());

        Collection<ClusterNode> affNodes = marshCache.affinity().mapKeyToPrimaryAndBackups(1);

        assertEquals(1, affNodes.size());
        assertTrue(affNodes.contains(ignite(0).cluster().localNode()));

        GridCacheAdapter utilityCache = ((IgniteKernal)ignite(0)).internalCache(CU.UTILITY_CACHE_NAME);

        affNodes = utilityCache.affinity().mapKeyToPrimaryAndBackups(1);

        assertEquals(1, affNodes.size());
        assertTrue(affNodes.contains(ignite(0).cluster().localNode()));
    }
}