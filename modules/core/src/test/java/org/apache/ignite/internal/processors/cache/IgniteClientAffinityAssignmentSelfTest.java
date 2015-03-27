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

import org.apache.ignite.*;
import org.apache.ignite.cache.*;
import org.apache.ignite.cache.affinity.*;
import org.apache.ignite.cache.affinity.fair.*;
import org.apache.ignite.cache.affinity.rendezvous.*;
import org.apache.ignite.configuration.*;
import org.apache.ignite.internal.*;
import org.apache.ignite.internal.util.lang.*;
import org.apache.ignite.spi.discovery.tcp.*;
import org.apache.ignite.spi.discovery.tcp.ipfinder.*;
import org.apache.ignite.spi.discovery.tcp.ipfinder.vm.*;
import org.apache.ignite.testframework.*;
import org.apache.ignite.testframework.junits.common.*;

import java.util.concurrent.*;

/**
 * Tests affinity assignment for different affinity types.
 */
public class IgniteClientAffinityAssignmentSelfTest extends GridCommonAbstractTest {
    /** */
    private static final TcpDiscoveryIpFinder ipFinder = new TcpDiscoveryVmIpFinder(true);

    /** */
    public static final int PARTS = 256;

    /** */
    private boolean cache;

    /** */
    private int aff;

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String gridName) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(gridName);

        ((TcpDiscoverySpi)cfg.getDiscoverySpi()).setIpFinder(ipFinder);

        if (cache) {
            CacheConfiguration ccfg = new CacheConfiguration();

            ccfg.setCacheMode(CacheMode.PARTITIONED);
            ccfg.setBackups(1);
            ccfg.setAtomicityMode(CacheAtomicityMode.TRANSACTIONAL);

            ccfg.setNearConfiguration(null);

            if (aff == 0)
                ccfg.setAffinity(new RendezvousAffinityFunction(false, PARTS));
            else
                ccfg.setAffinity(new FairAffinityFunction(PARTS));

            cfg.setCacheConfiguration(ccfg);
        }
        else
            cfg.setClientMode(true);

        return cfg;
    }

    /**
     * @throws Exception If failed.
     */
    public void testRendezvousAssignment() throws Exception {
        aff = 0;

        checkAffinityFunction();
    }

    /**
     * @throws Exception If failed.
     */
    public void testFairAssignment() throws Exception {
        aff = 1;

        checkAffinityFunction();
    }

    /**
     * @throws Exception If failed.
     */
    private void checkAffinityFunction() throws Exception {
        cache = true;

        startGrids(3);

        long topVer = 3;

        try {
            checkAffinity(topVer++);

            cache = false;

            final Ignite ignite3 = startGrid(3);

            GridTestUtils.assertThrows(log, new Callable<Object>() {
                @Override public Object call() throws Exception {
                    ((IgniteKernal)ignite3).getCache(null);

                    return null;
                }
            }, IllegalArgumentException.class, null);

            assertNotNull(ignite3.cache(null)); // Start client cache.

            ((IgniteKernal)ignite3).getCache(null);

            checkAffinity(topVer++);

            final Ignite ignite4 = startGrid(4);

            GridTestUtils.assertThrows(log, new Callable<Object>() {
                @Override public Object call() throws Exception {
                    ((IgniteKernal)ignite4).getCache(null);

                    return null;
                }
            }, IllegalArgumentException.class, null);

            assertNotNull(ignite4.cache(null)); // Start client cache.

            ((IgniteKernal)ignite4).getCache(null);

            checkAffinity(topVer++);

            final Ignite ignite5 = startGrid(5); // Node without cache.

            GridTestUtils.assertThrows(log, new Callable<Object>() {
                @Override public Object call() throws Exception {
                    ((IgniteKernal)ignite5).getCache(null);

                    return null;
                }
            }, IllegalArgumentException.class, null);

            checkAffinity(topVer++);

            stopGrid(5);

            checkAffinity(topVer++);

            stopGrid(4);

            checkAffinity(topVer++);

            stopGrid(3);

            checkAffinity(topVer);
        }
        finally {
            stopAllGrids();
        }
    }

    /**
     * @param topVer Topology version.
     * @throws Exception If failed.
     */
    private void checkAffinity(long topVer) throws Exception {
        awaitTopology(topVer);

        Affinity<Object> aff = ((IgniteKernal)grid(0)).getCache(null).affinity();

        for (Ignite grid : Ignition.allGrids()) {
            try {
                if (grid.cluster().localNode().id().equals(grid(0).localNode().id()))
                    continue;

                Affinity<Object> checkAff = ((IgniteKernal)grid).getCache(null).affinity();

                for (int p = 0; p < PARTS; p++)
                    assertEquals(aff.mapPartitionToPrimaryAndBackups(p), checkAff.mapPartitionToPrimaryAndBackups(p));
            }
            catch (IllegalArgumentException ignored) {
                // Skip the node without cache.
            }
        }
    }

    /**
     * @param topVer Topology version.
     * @throws Exception If failed.
     */
    private void awaitTopology(final long topVer) throws Exception {
        for (Ignite grid : Ignition.allGrids()) {
            final GridCacheAdapter cache = ((IgniteKernal)grid).internalCache(null);

            if (cache == null)
                continue;

            GridTestUtils.waitForCondition(new GridAbsPredicate() {
                @Override public boolean apply() {
                    return cache.context().affinity().affinityTopologyVersion().topologyVersion() == topVer;
                }
            }, 5000);

            assertEquals(topVer, cache.context().affinity().affinityTopologyVersion().topologyVersion());
        }
    }
}
