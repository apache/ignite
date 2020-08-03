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
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.internal.managers.discovery.DiscoCache;
import org.apache.ignite.internal.processors.affinity.AffinityTopologyVersion;
import org.apache.ignite.internal.util.GridBoundedConcurrentLinkedHashMap;
import org.apache.ignite.internal.util.GridConcurrentHashSet;
import org.apache.ignite.internal.util.typedef.G;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.testframework.GridTestUtils;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.junit.Test;

/**
 * Tests discovery cache reuse between topology events.
 */
public class IgniteDiscoveryCacheReuseSelfTest extends GridCommonAbstractTest {
    /** {@inheritDoc} */
    @Override protected void afterTest() throws Exception {
        stopAllGrids();

        super.afterTest();
    }

    /**
     * Tests correct reuse of discovery cache.
     *
     * @throws Exception If failed.
     */
    @Test
    public void testDiscoCacheReuseOnNodeJoin() throws Exception {
        Ignite ignite1 = startGridsMultiThreaded(2);

        ignite1.cluster().baselineAutoAdjustEnabled(false);
        // The final topology version after 2 node joins and one CacheAffinityChange message.
        AffinityTopologyVersion waited = new AffinityTopologyVersion(2, 1);

        // Wait for this topology version on all grids.
        GridTestUtils.waitForCondition(() -> {
            boolean verChanged = true;
            for (Ignite ignite : G.allGrids())
                verChanged &= ((IgniteEx) ignite).context().discovery().topologyVersionEx().equals(waited);
            return verChanged;
        }, 5000);

        assertDiscoCacheReuse(new AffinityTopologyVersion(2, 0), new AffinityTopologyVersion(2, 1));
    }

    /**
     * Assert disco cache reuse.
     *
     * @param v1 First version.
     * @param v2 Next version.
     */
    private void assertDiscoCacheReuse(AffinityTopologyVersion v1, AffinityTopologyVersion v2) {
        for (Ignite ignite : G.allGrids()) {
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

            assertNotSame(U.field(discoCache1, "alives"), U.field(discoCache2, "alives"));

            GridConcurrentHashSet alives1 = U.field(discoCache1, "alives");
            GridConcurrentHashSet alives2 = U.field(discoCache2, "alives");
            assertEquals("Discovery caches are not equal", alives1, alives2);
        }
    }
}
