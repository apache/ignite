/*
 *                   GridGain Community Edition Licensing
 *                   Copyright 2019 GridGain Systems, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License") modified with Commons Clause
 * Restriction; you may not use this file except in compliance with the License. You may obtain a
 * copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the
 * License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied. See the License for the specific language governing permissions
 * and limitations under the License.
 *
 * Commons Clause Restriction
 *
 * The Software is provided to you by the Licensor under the License, as defined below, subject to
 * the following condition.
 *
 * Without limiting other conditions in the License, the grant of rights under the License will not
 * include, and the License does not grant to you, the right to Sell the Software.
 * For purposes of the foregoing, “Sell” means practicing any or all of the rights granted to you
 * under the License to provide to third parties, for a fee or other consideration (including without
 * limitation fees for hosting or consulting/ support services related to the Software), a product or
 * service whose value derives, entirely or substantially, from the functionality of the Software.
 * Any license notice or attribution required by the License must also include this Commons Clause
 * License Condition notice.
 *
 * For purposes of the clause above, the “Licensor” is Copyright 2019 GridGain Systems, Inc.,
 * the “License” is the Apache License, Version 2.0, and the Software is the GridGain Community
 * Edition software provided with this notice.
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
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

/**
 * Tests discovery cache reuse between topology events.
 */
@RunWith(JUnit4.class)
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
        startGridsMultiThreaded(2);

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
