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

package org.apache.ignite.internal.processors.cache.distributed;

import org.apache.ignite.IgniteCache;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.internal.processors.cache.GridCacheAbstractSelfTest;
import org.apache.ignite.internal.util.typedef.X;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.apache.ignite.cache.CacheAtomicityMode.TRANSACTIONAL;
import static org.apache.ignite.cache.CacheMode.PARTITIONED;
import static org.apache.ignite.cache.CacheWriteSynchronizationMode.FULL_SYNC;

/**
 *
 */
public class CacheRemoveAllTest extends GridCacheAbstractSelfTest {
    /** */
    private static final int ENTRIES_NUM = 100_000;

    /** {@inheritDoc} */
    @Override protected int gridCount() {
        return 2;
    }

    /** {@inheritDoc} */
    @Override protected long getTestTimeout() {
        return 60_000 * 30;
    }

    /**
     * @throws Exception
     */
    public void testPutAllRemoveAll() throws Exception {
        final CacheConfiguration<Integer, Integer> cfg = new CacheConfiguration<>();

        cfg.setWriteSynchronizationMode(FULL_SYNC);
        cfg.setCacheMode(PARTITIONED);
        cfg.setAtomicityMode(TRANSACTIONAL);
        cfg.setStatisticsEnabled(false);
        cfg.setBackups(0);
        cfg.setName("test_cache");

        final IgniteEx ignite1 = grid(0);
        final IgniteEx ignite2 = grid(1);

        final Map<Integer, Integer> data = new HashMap<>();

        for (int i = 0; i < ENTRIES_NUM; i++)
            data.put(i, i);

        final IgniteCache<Integer, Integer> cache1 = ignite1.createCache(cfg);
        final IgniteCache<Integer, Integer> cache2 = ignite2.getOrCreateCache(cfg);

        final List<Map<Integer, Integer>> split = split(data, ENTRIES_NUM);

        for (final Map<Integer, Integer> map : split) {
            cache1.putAll(map);

            X.println("Data have been added");
        }

        assert cache1.containsKeys(data.keySet());
        assert cache2.containsKeys(data.keySet());

        for (final Map<Integer, Integer> map : split) {
            cache1.removeAll(map.keySet());

            X.println("Data have been removed");
        }

        assert cache1.getAll(data.keySet()).isEmpty();
        assert cache2.getAll(data.keySet()).isEmpty();
    }

    /**
     * Split map on partitions.
     *
     * @param map Map to split.
     * @param partSize Size of the partition.
     * @param <K> Key type.
     * @param <V> Val type.
     * @return List of the partitions.
     */
    private <K, V> List<Map<K, V>> split(final Map<K, V> map, final int partSize) {
        final List<Map<K, V>> res = new ArrayList<>();

        Map<K, V> resMap = new HashMap<>(partSize);

        int cnt = 0;

        for (final Map.Entry<K, V> entry : map.entrySet()) {
            resMap.put(entry.getKey(), entry.getValue());

            if (++cnt == partSize) {
                res.add(resMap);

                resMap = new HashMap<>(partSize);

                cnt = 0;
            }
        }

        return res;
    }
}
