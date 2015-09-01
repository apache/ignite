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

package org.apache.ignite.internal.processors.cache.local;

import java.util.Collection;
import java.util.Map;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.cache.CacheMode;
import org.apache.ignite.cluster.ClusterNode;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.processors.cache.GridCacheAbstractFullApiSelfTest;
import org.apache.ignite.internal.util.typedef.F;

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
    @Override protected IgniteConfiguration getConfiguration(String gridName) throws Exception {
        IgniteConfiguration c = super.getConfiguration(gridName);

        c.getTransactionConfiguration().setTxSerializableEnabled(true);

        return c;
    }

    /** {@inheritDoc} */
    @Override protected CacheConfiguration cacheConfiguration(String gridName) throws Exception {
        CacheConfiguration cfg = super.cacheConfiguration(gridName);

        cfg.setSwapEnabled(true);

        return cfg;
    }

    /**
     * @throws Exception In case of error.
     */
    public void testMapKeysToNodes() throws Exception {
        IgniteCache<String, Integer> cache = jcache();

        cache.put("key1", 1);
        cache.put("key2", 2);

        Map<ClusterNode, Collection<String>> map = grid(0).<String>affinity(null).mapKeysToNodes(F.asList("key1", "key2"));

        assert map.size() == 1;

        Collection<String> keys = map.get(dfltIgnite.cluster().localNode());

        assert keys != null;
        assert keys.size() == 2;

        for (String key : keys)
            assert "key1".equals(key) || "key2".equals(key);

        map = grid(0).<String>affinity(null).mapKeysToNodes(F.asList("key1", "key2"));

        assert map.size() == 1;

        keys = map.get(dfltIgnite.cluster().localNode());

        assert keys != null;
        assert keys.size() == 2;

        for (String key : keys)
            assert "key1".equals(key) || "key2".equals(key);
    }
}