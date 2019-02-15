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

import java.io.Serializable;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.cache.CacheMode;
import org.apache.ignite.cache.CacheWriteSynchronizationMode;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.junit.Test;

/**
 *
 */
public class IgniteCacheGetCustomCollectionsSelfTest extends GridCommonAbstractTest {
    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String igniteInstanceName) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(igniteInstanceName);

        cfg.setMarshaller(null);

        final CacheConfiguration<String, MyMap> mapCacheConfig = new CacheConfiguration<>(DEFAULT_CACHE_NAME);

        mapCacheConfig.setCacheMode(CacheMode.PARTITIONED);
        mapCacheConfig.setWriteSynchronizationMode(CacheWriteSynchronizationMode.FULL_SYNC);
        mapCacheConfig.setBackups(1);
        mapCacheConfig.setName("cache");

        cfg.setCacheConfiguration(mapCacheConfig);

        return cfg;
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testPutGet() throws Exception {
        startGrids(3);

        try {
            IgniteEx ignite = grid(0);

            IgniteCache<String, MyMap> cache = ignite.cache("cache");

            Set<String> keys = new HashSet<>();

            for (int i = 0; i < 100; i++) {
                String key = "a" + i;;

                MyMap map = new MyMap();

                map.put("a", new Value());

                cache.put(key, map);

                map = cache.get(key);

                keys.add(key);

                Object a = map.get("a");

                assertNotNull(a);
                assertEquals(Value.class, a.getClass());
            }

            Map<String, MyMap> vals = cache.getAll(keys);

            for (String key : keys) {
                MyMap map = vals.get(key);

                Object a = map.get("a");

                assertNotNull(a);
                assertEquals(Value.class, a.getClass());
            }
        }
        finally {
            stopAllGrids();
        }
    }

    /**
     *
     */
    private static class MyMap extends HashMap implements Serializable {

    }

    /**
     *
     */
    private static class Value implements Serializable {
        private int val;
    }
}
