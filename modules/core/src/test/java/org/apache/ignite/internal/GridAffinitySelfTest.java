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

package org.apache.ignite.internal;

import java.util.Arrays;
import java.util.Collection;
import java.util.Map;
import java.util.UUID;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.cluster.ClusterNode;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.util.typedef.F;
import org.apache.ignite.internal.util.typedef.internal.CU;
import org.apache.ignite.lang.IgnitePredicate;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.junit.Test;

import static org.apache.ignite.cache.CacheMode.PARTITIONED;

/**
 * Tests affinity mapping.
 */
public class GridAffinitySelfTest extends GridCommonAbstractTest {
    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String igniteInstanceName) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(igniteInstanceName);

        if (igniteInstanceName.endsWith("1"))
            cfg.setClientMode(true);
        else {
            assert igniteInstanceName.endsWith("2");

            CacheConfiguration cacheCfg = defaultCacheConfiguration();

            cacheCfg.setCacheMode(PARTITIONED);
            cacheCfg.setBackups(1);

            cfg.setCacheConfiguration(cacheCfg);
        }

        return cfg;
    }

    /** {@inheritDoc} */
    @Override protected void beforeTest() throws Exception {
        startGrid(2);

        startGrid(1);
    }

    /** {@inheritDoc} */
    @Override protected void afterTest() throws Exception {
        stopAllGrids();
    }

    /**
     * @throws IgniteCheckedException If failed.
     */
    @Test
    public void testAffinity() throws Exception {
        Ignite g1 = grid(1);
        Ignite g2 = grid(2);

        assert caches(g1).isEmpty();
        assert F.first(caches(g2)).getCacheMode() == PARTITIONED;

        awaitPartitionMapExchange();

        Map<ClusterNode, Collection<String>> map = g1.<String>affinity(DEFAULT_CACHE_NAME).mapKeysToNodes(F.asList("1"));

        assertNotNull(map);
        assertEquals("Invalid map size: " + map.size(), 1, map.size());
        assertEquals(F.first(map.keySet()), g2.cluster().localNode());

        UUID id1 = g1.affinity(DEFAULT_CACHE_NAME).mapKeyToNode("2").id();

        assertNotNull(id1);
        assertEquals(g2.cluster().localNode().id(), id1);

        UUID id2 = g1.affinity(DEFAULT_CACHE_NAME).mapKeyToNode("3").id();

        assertNotNull(id2);
        assertEquals(g2.cluster().localNode().id(), id2);
    }

    /**
     * @param g Grid.
     * @return Non-system caches.
     */
    private Collection<CacheConfiguration> caches(Ignite g) {
        return F.view(Arrays.asList(g.configuration().getCacheConfiguration()), new IgnitePredicate<CacheConfiguration>() {
            @Override public boolean apply(CacheConfiguration c) {
                return !CU.UTILITY_CACHE_NAME.equals(c.getName()) && !CU.SYS_CACHE_HADOOP_MR.equals(c.getName());
            }
        });
    }
}
