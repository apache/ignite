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

package org.apache.ignite.spi.discovery;

import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.internal.IgniteInternalFuture;
import org.apache.ignite.internal.managers.discovery.DiscoCache;
import org.apache.ignite.internal.processors.affinity.AffinityTopologyVersion;
import org.apache.ignite.internal.processors.affinity.GridAffinityAssignmentCache;
import org.apache.ignite.internal.util.GridBoundedConcurrentLinkedHashMap;
import org.apache.ignite.internal.util.typedef.G;
import org.apache.ignite.internal.util.typedef.internal.CU;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.spi.discovery.tcp.TcpDiscoverySpi;
import org.apache.ignite.spi.discovery.tcp.ipfinder.TcpDiscoveryIpFinder;
import org.apache.ignite.spi.discovery.tcp.ipfinder.vm.TcpDiscoveryVmIpFinder;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

/**
 * Tests discovery cache reuse between topology events.
 */
@RunWith(JUnit4.class)
public class IgniteDiscoveryCacheReuseSelfTest extends GridCommonAbstractTest {
    /** */
    private static final TcpDiscoveryIpFinder IP_FINDER = new TcpDiscoveryVmIpFinder(true);

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String igniteInstanceName) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(igniteInstanceName);

        ((TcpDiscoverySpi)cfg.getDiscoverySpi()).setIpFinder(IP_FINDER);

        cfg.setCacheConfiguration(new CacheConfiguration(DEFAULT_CACHE_NAME));

        return cfg;
    }

    /** {@inheritDoc} */
    @Override protected void afterTest() throws Exception {
        super.afterTest();

        stopAllGrids();
    }

    /**
     * Tests correct reuse of discovery cache.
     *
     * @throws Exception If failed.
     */
    @Test
    public void testDiscoCacheReuseOnNodeJoin() throws Exception {
        startGridsMultiThreaded(2);

        assertDiscoCacheReuse(new AffinityTopologyVersion(2, 0), new AffinityTopologyVersion(2, 1));
    }

    /**
     * Assert disco cache reuse.
     *
     * @param v1 First version.
     * @param v2 Next version.
     */
    private void assertDiscoCacheReuse(AffinityTopologyVersion v1, AffinityTopologyVersion v2) throws IgniteCheckedException {
        assertTrue(v2.compareTo(v1) > 0);

        for (Ignite ignite : G.allGrids()) {
            final IgniteInternalFuture<AffinityTopologyVersion> fut =
                ((IgniteEx)ignite).context().cache().cacheGroup(CU.cacheId(DEFAULT_CACHE_NAME)).affinity().readyFuture(v2);

            if (fut != null)
                fut.get(3000);

            GridBoundedConcurrentLinkedHashMap<AffinityTopologyVersion, DiscoCache> discoCacheHist =
                U.field(((IgniteEx) ignite).context().discovery(), "discoCacheHist");

            DiscoCache discoCache1 = discoCacheHist.get(v1);
            DiscoCache discoCache2 = discoCacheHist.get(v2);

            assertEquals(v1, discoCache1.version());
            assertEquals(v2, discoCache2.version());

            String[] props = new String[] {
                "state", "loc", "rmtNodes", "allNodes", "srvNodes", "daemonNodes", "rmtNodesWithCaches",
                "allCacheNodes", "allCacheNodes", "cacheGrpAffNodes", "nodeMap", "minNodeVer"
            };

            for (String prop : props)
                assertSame(U.field(discoCache1, prop), U.field(discoCache2, prop));

            // Do not remove type hint or polymorphic resolution won't work as expected.
            assertNotSame(U.<Object>field(discoCache1, "alives"), U.field(discoCache2, "alives"));
            assertEquals(U.<Object>field(discoCache1, "alives"), U.field(discoCache2, "alives"));
        }
    }
}
