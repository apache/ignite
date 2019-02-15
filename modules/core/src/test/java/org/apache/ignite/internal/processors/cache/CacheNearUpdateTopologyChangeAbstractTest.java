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

package org.apache.ignite.internal.processors.cache;

import java.util.ArrayList;
import java.util.List;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.cache.CacheMode;
import org.apache.ignite.cache.affinity.Affinity;
import org.apache.ignite.cluster.ClusterNode;
import org.apache.ignite.configuration.NearCacheConfiguration;
import org.apache.ignite.internal.util.lang.GridAbsPredicate;
import org.apache.ignite.testframework.GridTestUtils;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

import static org.apache.ignite.cache.CacheMode.PARTITIONED;
import static org.apache.ignite.cache.CachePeekMode.ONHEAP;

/**
 *
 */
@RunWith(JUnit4.class)
public abstract class CacheNearUpdateTopologyChangeAbstractTest extends IgniteCacheAbstractTest {
    /** {@inheritDoc} */
    @Override protected int gridCount() {
        return 3;
    }

    /** {@inheritDoc} */
    @Override protected CacheMode cacheMode() {
        return PARTITIONED;
    }

    /** {@inheritDoc} */
    @Override protected NearCacheConfiguration nearConfiguration() {
        return new NearCacheConfiguration();
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testNearUpdateTopologyChange() throws Exception {
        awaitPartitionMapExchange();

        final Affinity<Integer> aff = grid(0).affinity(DEFAULT_CACHE_NAME);

        final Integer key = 9;

        IgniteCache<Integer, Integer> primaryCache = primaryCache(key, DEFAULT_CACHE_NAME);

        final Ignite primaryIgnite = primaryCache.unwrap(Ignite.class);

        log.info("Primary node: " + primaryIgnite.name());

        primaryCache.put(key, 1);

        IgniteCache<Integer, Integer> nearCache = nearCache(key);

        log.info("Near node: " + nearCache.unwrap(Ignite.class).name());

        assertEquals((Object)1, nearCache.get(key));

        assertEquals((Object)1, nearCache.localPeek(key, ONHEAP));

        boolean gotNewPrimary = false;

        List<Ignite> newNodes = new ArrayList<>();

        for (int i = 0; i < 10; i++) {
            int idx = gridCount() + i;

            log.info("Start new node: " + i);

            Ignite ignite = startGrid(idx);

            awaitPartitionMapExchange();

            newNodes.add(ignite);

            ClusterNode primaryNode = aff.mapKeyToNode(key);

            Ignite primary = grid(primaryNode);

            log.info("Primary node on new topology: " + primary.name());

            if (!primaryNode.equals(primaryIgnite.cluster().localNode())) {
                log.info("Update from new primary: " + primary.name());

                primary = grid(primaryNode);

                gotNewPrimary = true;

                primary.cache(DEFAULT_CACHE_NAME).put(key, 2);

                break;
            }
            else
                assertEquals((Object)1, nearCache.localPeek(key, ONHEAP));
        }

        assertTrue(gotNewPrimary);

        for (Ignite ignite : newNodes) {
            log.info("Stop started node: " + ignite.name());

            ignite.close();
        }

        awaitPartitionMapExchange();

        boolean wait = GridTestUtils.waitForCondition(new GridAbsPredicate() {
            @Override public boolean apply() {
                return aff.isPrimary(primaryIgnite.cluster().localNode(), key);
            }
        }, 10_000);

        assertTrue(wait);

        log.info("Primary node: " + primaryNode(key, DEFAULT_CACHE_NAME).name());

        assertTrue(aff.isPrimary(primaryIgnite.cluster().localNode(), key));

        assertFalse(aff.isPrimaryOrBackup(nearCache.unwrap(Ignite.class).cluster().localNode(), key));

        assertEquals((Object)2, nearCache.get(key));

        assertEquals((Object)2, nearCache.localPeek(key, ONHEAP));
    }
}
