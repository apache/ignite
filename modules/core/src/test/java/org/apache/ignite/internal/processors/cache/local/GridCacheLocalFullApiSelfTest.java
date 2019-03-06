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

package org.apache.ignite.internal.processors.cache.local;

import java.util.Arrays;
import java.util.Collection;
import java.util.HashSet;
import java.util.Map;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.cache.CacheMode;
import org.apache.ignite.cluster.ClusterNode;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.processors.cache.GridCacheAbstractFullApiSelfTest;
import org.apache.ignite.internal.util.typedef.F;
import org.junit.Test;

import static org.apache.ignite.cache.CacheMode.LOCAL;

/**
 * Tests for local cache.
 */
public class GridCacheLocalFullApiSelfTest extends GridCacheAbstractFullApiSelfTest {
    /** {@inheritDoc} */
    @Override protected CacheMode cacheMode() {
        return LOCAL;
    }

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String igniteInstanceName) throws Exception {
        IgniteConfiguration c = super.getConfiguration(igniteInstanceName);

        c.getTransactionConfiguration().setTxSerializableEnabled(true);

        return c;
    }

    /**
     * @throws Exception In case of error.
     */
    @Test
    public void testMapKeysToNodes() throws Exception {
        IgniteCache<String, Integer> cache = jcache();

        cache.put("key1", 1);
        cache.put("key2", 2);

        Map<ClusterNode, Collection<String>> map = grid(0).<String>affinity(DEFAULT_CACHE_NAME).mapKeysToNodes(F.asList("key1", "key2"));

        assert map.size() == 1;

        Collection<String> keys = map.get(dfltIgnite.cluster().localNode());

        assert keys != null;
        assert keys.size() == 2;

        for (String key : keys)
            assert "key1".equals(key) || "key2".equals(key);

        map = grid(0).<String>affinity(DEFAULT_CACHE_NAME).mapKeysToNodes(F.asList("key1", "key2"));

        assert map.size() == 1;

        keys = map.get(dfltIgnite.cluster().localNode());

        assert keys != null;
        assert keys.size() == 2;

        for (String key : keys)
            assert "key1".equals(key) || "key2".equals(key);
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testLocalClearAsync() throws Exception {
        localCacheClear(true);
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testLocalClear() throws Exception {
        localCacheClear(false);
    }

    /**
     * @param async If {@code true} uses async method.
     * @throws Exception If failed.
     */
    private void localCacheClear(boolean async) throws Exception {
        // In addition to the existing tests, it confirms the data is cleared only on one node,
        // not on all nodes that have local caches with same names.
        try {
            startGrid(1);

            IgniteCache<String, Integer> cache = jcache();

            for (int i = 0; i < 5; i++) {
                cache.put(String.valueOf(i), i);
                jcache(1).put(String.valueOf(i), i);
            }

            if (async)
                cache.clearAsync("4").get();
            else
                cache.clear("4");

            assertNull(peek(cache, "4"));
            assertNotNull(peek(jcache(1), "4"));

            if (async)
                cache.clearAllAsync(new HashSet<>(Arrays.asList("2", "3"))).get();
            else
                cache.clearAll(new HashSet<>(Arrays.asList("2", "3")));

            for (int i = 2; i < 4; i++) {
                assertNull(peek(cache, String.valueOf(i)));
                assertNotNull(peek(jcache(1), String.valueOf(i)));
            }

            if (async)
                cache.clearAsync().get();
            else
                cache.clear();

            for (int i = 0; i < 2; i++) {
                assertNull(peek(cache, String.valueOf(i)));
                assertNotNull(peek(jcache(1), String.valueOf(i)));
            }

            if (async)
                jcache(1).clearAsync().get();
            else
                jcache(1).clear();

            for (int i = 0; i < 2; i++)
                assert jcache(i).localSize() == 0;
        }
        finally {
            stopGrid(1);
        }
    }
}
