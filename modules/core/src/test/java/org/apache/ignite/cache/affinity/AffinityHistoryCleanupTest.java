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
import org.apache.ignite.cache.affinity.rendezvous.RendezvousAffinityFunction;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.IgniteKernal;
import org.apache.ignite.internal.processors.affinity.AffinityTopologyVersion;
import org.apache.ignite.internal.processors.affinity.GridAffinityAssignmentCache;
import org.apache.ignite.internal.processors.cache.GridCacheContext;
import org.apache.ignite.internal.processors.cache.GridCacheProcessor;
import org.apache.ignite.internal.util.typedef.F;
import org.apache.ignite.testframework.GridTestUtils;
import org.apache.ignite.testframework.junits.WithSystemProperty;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.junit.Test;

import static org.apache.ignite.IgniteSystemProperties.IGNITE_AFFINITY_HISTORY_SIZE;

/**
 *
 */
public class AffinityHistoryCleanupTest extends GridCommonAbstractTest {
    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String igniteInstanceName) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(igniteInstanceName);

        CacheConfiguration[] ccfgs = new CacheConfiguration[4];

        for (int i = 0; i < ccfgs.length; i++) {
            CacheConfiguration ccfg = new CacheConfiguration(DEFAULT_CACHE_NAME);

            ccfg.setName("static-cache-" + i);
            ccfg.setAffinity(new RendezvousAffinityFunction());

            ccfgs[i] = ccfg;
        }

        cfg.setCacheConfiguration(ccfgs);

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
    @WithSystemProperty(key = IGNITE_AFFINITY_HISTORY_SIZE, value = "5")
    public void testAffinityHistoryCleanup() throws Exception {
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
            topVer(4, 0), // FullHistSize = 6 - 1 = 5.
            topVer(4, 1)), // FullHistSize = 6 - 1 = 5.
            5);

        startClientGrid(4);

        stopGrid(4);

        checkHistory(ignite, F.asList(
            topVer(2, 1), // FullHistSize = 3.
            topVer(3, 0), // FullHistSize = 4.
            topVer(3, 1), // FullHistSize = 5.
            topVer(4, 0), // FullHistSize = 6 - 1 = 5.
            topVer(4, 1), // FullHistSize = 6 - 1 = 5.
            topVer(5, 0), // Client event -> FullHistSize = 5.
            topVer(6, 0)), // Client event -> FullHistSize = 5.
            5);

        startClientGrid(4);

        stopGrid(4);

        checkHistory(ignite, F.asList(
            topVer(2, 1), // FullHistSize = 3.
            topVer(3, 0), // FullHistSize = 4.
            topVer(3, 1), // FullHistSize = 5.
            topVer(4, 0), // FullHistSize = 6 - 1 = 5.
            topVer(4, 1), // FullHistSize = 6 - 1 = 5.
            topVer(5, 0), // Client event -> FullHistSize = 5.
            topVer(6, 0), // Client event -> FullHistSize = 5.
            topVer(7, 0), // Client event -> FullHistSize = 5.
            topVer(8, 0)), // Client event -> FullHistSize = 5.
            5);

        startClientGrid(4);

        stopGrid(4);

        checkHistory(ignite, F.asList(
            topVer(2, 1), // FullHistSize = 3.
            topVer(3, 0), // FullHistSize = 4.
            topVer(3, 1), // FullHistSize = 5.
            topVer(4, 0), // FullHistSize = 6 - 1 = 5.
            topVer(4, 1), // FullHistSize = 6 - 1 = 5.
            topVer(5, 0), // Client event -> FullHistSize = 5.
            topVer(6, 0), // Client event -> FullHistSize = 5.
            topVer(7, 0), // Client event -> FullHistSize = 5.
            topVer(8, 0), // Client event -> FullHistSize = 5.
            topVer(9, 0), // Client event -> FullHistSize = 5.
            topVer(10, 0)), // Client event -> FullHistSize = 5.
            5);

        startGrid(4);

        checkHistory(ignite, F.asList(
            topVer(3, 1), // FullHistSize = 5.
            topVer(4, 0), // FullHistSize = 6 - 1 = 5.
            topVer(4, 1), // FullHistSize = 6 - 1 = 5.
            topVer(5, 0), // Client event -> FullHistSize = 5.
            topVer(6, 0), // Client event -> FullHistSize = 5.
            topVer(7, 0), // Client event -> FullHistSize = 5.
            topVer(8, 0), // Client event -> FullHistSize = 5.
            topVer(9, 0), // Client event -> FullHistSize = 5.
            topVer(10, 0), // Client event -> FullHistSize = 5.
            topVer(11, 0), // FullHistSize = 6 - 1 = 5.
            topVer(11, 1)), // FullHistSize = 6 - 1 = 5.
            5);

        stopGrid(4);

        startGrid(4);

        checkHistory(ignite, F.asList(
            topVer(11, 0), // FullHistSize = 5.
            topVer(11, 1), // FullHistSize = 5.
            topVer(12, 0), // FullHistSize = 6 - 1 = 5.
            topVer(13, 0), // FullHistSize = 5.
            topVer(13, 1)), // FullHistSize = 6 - 1 = 5.
            5);
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

            AtomicInteger fullHistSize = GridTestUtils.getFieldValue(aff, "nonShallowHistSize");

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
