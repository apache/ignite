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

package org.apache.ignite.internal.processors.cache.distributed.dht;

import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

import static org.apache.ignite.cache.CacheAtomicityMode.TRANSACTIONAL;
import static org.apache.ignite.cache.CacheMode.PARTITIONED;
import static org.apache.ignite.cache.CacheWriteSynchronizationMode.FULL_SYNC;

/**
 * Test cache closure execution.
 */
@RunWith(JUnit4.class)
public class GridCacheDhtEvictionsDisabledSelfTest extends GridCommonAbstractTest {
    /**
     *
     */
    public GridCacheDhtEvictionsDisabledSelfTest() {
        super(false); // Don't start grid node.
    }

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String igniteInstanceName) throws Exception {
        IgniteConfiguration c = super.getConfiguration(igniteInstanceName);

        CacheConfiguration cc = defaultCacheConfiguration();

        cc.setName("test");
        cc.setCacheMode(PARTITIONED);
        cc.setWriteSynchronizationMode(FULL_SYNC);
        cc.setAtomicityMode(TRANSACTIONAL);
        cc.setNearConfiguration(null);

        c.setCacheConfiguration(cc);

        return c;
    }

    /** {@inheritDoc} */
    @Override protected void afterTest() throws Exception {
        stopAllGrids();
    }

    /** @throws Exception If failed. */
    @Test
    public void testOneNode() throws Exception {
        checkNodes(startGridsMultiThreaded(1));

        assertEquals(26, colocated(0, "test").size());
        assertEquals(26, jcache(0, "test").localSize());
    }

    /** @throws Exception If failed. */
    @Test
    public void testTwoNodes() throws Exception {
        checkNodes(startGridsMultiThreaded(2));

        assertTrue(!colocated(0, "test").isEmpty());
        assertTrue(jcache(0, "test").localSize() > 0);
    }

    /** @throws Exception If failed. */
    @Test
    public void testThreeNodes() throws Exception {
        checkNodes(startGridsMultiThreaded(3));

        assertTrue(!colocated(0, "test").isEmpty());
        assertTrue(jcache(0, "test").localSize() > 0);
    }

    /**
     * @param g Grid.
     * @throws Exception If failed.
     */
    private void checkNodes(Ignite g) throws Exception {
        IgniteCache<String, String> cache = g.cache("test");

        for (char c = 'a'; c <= 'z'; c++) {
            String key = Character.toString(c);

            cache.put(key, "val-" + key);

            String v1 = cache.get(key);
            String v2 = cache.get(key); // Get second time.

            info("v1: " + v1);
            info("v2: " + v2);

            assertNotNull(v1);
            assertNotNull(v2);

            // TODO GG-11148: can do assertSame if on-heap storage is implemented.
            if (false && affinity(cache).mapKeyToNode(key).isLocal())
                assertSame(v1, v2);
            else
                assertEquals(v1, v2);
        }
    }
}
