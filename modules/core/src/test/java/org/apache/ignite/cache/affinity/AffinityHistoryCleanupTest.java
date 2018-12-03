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

package org.apache.ignite.cache.affinity;

import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteSystemProperties;
import org.apache.ignite.cache.affinity.rendezvous.RendezvousAffinityFunction;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.IgniteKernal;
import org.apache.ignite.internal.processors.affinity.AffinityTopologyVersion;
import org.apache.ignite.internal.processors.affinity.GridAffinityAssignmentCache;
import org.apache.ignite.internal.processors.cache.GridCacheContext;
import org.apache.ignite.internal.processors.cache.GridCacheProcessor;
import org.apache.ignite.internal.util.typedef.F;
import org.apache.ignite.spi.discovery.tcp.TcpDiscoverySpi;
import org.apache.ignite.spi.discovery.tcp.ipfinder.TcpDiscoveryIpFinder;
import org.apache.ignite.spi.discovery.tcp.ipfinder.vm.TcpDiscoveryVmIpFinder;
import org.apache.ignite.testframework.GridTestUtils;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

/**
 *
 */
@RunWith(JUnit4.class)
public class AffinityHistoryCleanupTest extends GridCommonAbstractTest {
    /** */
    private static final TcpDiscoveryIpFinder ipFinder = new TcpDiscoveryVmIpFinder(true);

    /** */
    private boolean client;

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String igniteInstanceName) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(igniteInstanceName);

        ((TcpDiscoverySpi)cfg.getDiscoverySpi()).setIpFinder(ipFinder);

        CacheConfiguration[] ccfgs = new CacheConfiguration[4];

        for (int i = 0; i < ccfgs.length; i++) {
            CacheConfiguration ccfg = new CacheConfiguration(DEFAULT_CACHE_NAME);

            ccfg.setName("static-cache-" + i);
            ccfg.setAffinity(new RendezvousAffinityFunction());

            ccfgs[i] = ccfg;
        }

        cfg.setCacheConfiguration(ccfgs);

        cfg.setClientMode(client);

        return cfg;
    }

    /** {@inheritDoc} */
    @Override protected void afterTest() throws Exception {
        stopAllGrids();

        super.afterTest();
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testAffinityHistoryCleanup() throws Exception {
        String histProp = System.getProperty(IgniteSystemProperties.IGNITE_AFFINITY_HISTORY_SIZE);

        try {
            System.setProperty(IgniteSystemProperties.IGNITE_AFFINITY_HISTORY_SIZE, "5");

            Ignite ignite = startGrid(0);

            checkHistory(ignite, F.asList(topVer(1, 0)), 1); //fullHistSize = 1

            startGrid(1);

            checkHistory(ignite, F.asList(
                topVer(1, 0), // FullHistSize = 1.
                topVer(2, 0), // FullHistSize = 2.
                topVer(2, 1)), // FullHistSize = 3.
                3);

            startGrid(2);

            checkHistory(ignite, F.asList(
                topVer(1, 0), // FullHistSize = 1.
                topVer(2, 0), // FullHistSize = 2.
                topVer(2, 1), // FullHistSize = 3.
                topVer(3, 0), // FullHistSize = 4.
                topVer(3, 1)), // FullHistSize = 5.
                5);

            startGrid(3);

            checkHistory(ignite, F.asList(
                topVer(2, 1), // FullHistSize = 3.
                topVer(3, 0), // FullHistSize = 4.
                topVer(3, 1), // FullHistSize = 5.
                topVer(4, 0), // FullHistSize = (6 - IGNITE_AFFINITY_HISTORY_SIZE(5)/2) = 4.
                topVer(4, 1)), // FullHistSize = 5.
                5);

            client = true;

            startGrid(4);

            stopGrid(4);

            checkHistory(ignite, F.asList(
                topVer(3, 1), // FullHistSize = 5.
                topVer(4, 0), // FullHistSize = (6 - IGNITE_AFFINITY_HISTORY_SIZE(5)/2) = 4.
                topVer(4, 1), // FullHistSize = 5.
                topVer(5, 0), // FullHistSize = (6 - IGNITE_AFFINITY_HISTORY_SIZE(5)/2) = 4.
                topVer(6, 0)), // FullHistSize = 5.
                5);

            startGrid(4);

            stopGrid(4);

            checkHistory(ignite, F.asList(
                topVer(4, 1), // FullHistSize = 5.
                topVer(5, 0), // FullHistSize = (6 - IGNITE_AFFINITY_HISTORY_SIZE(5)/2) = 4.
                topVer(6, 0), // FullHistSize = 5.
                topVer(7, 0), // FullHistSize = (6 - IGNITE_AFFINITY_HISTORY_SIZE(5)/2) = 4.
                topVer(8, 0)), // FullHistSize = 5.
                5);

            startGrid(4);

            stopGrid(4);

            checkHistory(ignite, F.asList(
                topVer(6, 0), // FullHistSize = 5.
                topVer(7, 0), // FullHistSize = (6 - IGNITE_AFFINITY_HISTORY_SIZE(5)/2) = 4.
                topVer(8, 0), // FullHistSize = 5.
                topVer(9, 0), // FullHistSize = (6 - IGNITE_AFFINITY_HISTORY_SIZE(5)/2) = 4.
                topVer(10, 0)), // FullHistSize = 5.
                5);

            client = false;

            startGrid(4);

            checkHistory(ignite, F.asList(
                topVer(8, 0), // FullHistSize = 5.
                topVer(9, 0), // FullHistSize = (6 - IGNITE_AFFINITY_HISTORY_SIZE(5)/2) = 4.
                topVer(10, 0), // FullHistSize = 5.
                topVer(11, 0), // FullHistSize = (6 - IGNITE_AFFINITY_HISTORY_SIZE(5)/2) = 4.
                topVer(11, 1)), // FullHistSize = 5.
                5);
        }
        finally {
            if (histProp != null)
                System.setProperty(IgniteSystemProperties.IGNITE_AFFINITY_HISTORY_SIZE, histProp);
            else
                System.clearProperty(IgniteSystemProperties.IGNITE_AFFINITY_HISTORY_SIZE);
        }
    }

    /**
     * @param ignite Node.
     * @param expHist Expected history.
     * @param expSize Expected 'non client events' history size.
     * @throws Exception If failed.
     */
    private void checkHistory(Ignite ignite, List<AffinityTopologyVersion> expHist, int expSize) throws Exception {
        awaitPartitionMapExchange();

        GridCacheProcessor proc = ((IgniteKernal)ignite).context().cache();

        int cnt = 0;

        for (GridCacheContext cctx : proc.context().cacheContexts()) {
            GridAffinityAssignmentCache aff = GridTestUtils.getFieldValue(cctx.affinity(), "aff");

            AtomicInteger fullHistSize = GridTestUtils.getFieldValue(aff, "fullHistSize");

            assertEquals(expSize, fullHistSize.get());

            Map<AffinityTopologyVersion, Object> cache = GridTestUtils.getFieldValue(aff, "affCache");

            assertEquals("Unexpected history: " + cache.keySet(), expHist.size(), cache.size());

            for (AffinityTopologyVersion topVer : expHist)
                assertTrue("No history [ver=" + topVer + ", hist=" + cache.keySet() + ']', cache.containsKey(topVer));

            cnt++;
        }

        assert cnt > 4;
    }

    /**
     * @param major Major version.
     * @param minor Minor version.
     * @return Version.
     */
    private static AffinityTopologyVersion topVer(int major, int minor) {
        return new AffinityTopologyVersion(major, minor);
    }
}
