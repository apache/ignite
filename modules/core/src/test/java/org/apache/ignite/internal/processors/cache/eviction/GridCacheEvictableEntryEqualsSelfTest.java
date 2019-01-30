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

package org.apache.ignite.internal.processors.cache.eviction;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Collection;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.cache.eviction.EvictableEntry;
import org.apache.ignite.cache.eviction.EvictionPolicy;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.junit.Test;

/**
 * Test for EvictableEntry.equals().
 */
public class GridCacheEvictableEntryEqualsSelfTest extends GridCommonAbstractTest {
    /**
     * @throws Exception If failed.
     */
    @Test
    public void testEquals() throws Exception {
        try (Ignite ignite = startGrid()) {
            CacheConfiguration<TestKey, String> cfg = new CacheConfiguration<>("test");

            cfg.setEvictionPolicy(new TestEvictionPolicy());
            cfg.setOnheapCacheEnabled(true);

            IgniteCache<TestKey, String> cache = ignite.createCache(cfg);

            for (int i = 0; i < 10; i++)
                cache.put(new TestKey(0), "val" + i);
        }
    }

    private static class TestEvictionPolicy implements EvictionPolicy<TestKey, String>, Serializable {
        private final Collection<EvictableEntry> entries = new ArrayList<>();

        @Override public synchronized void onEntryAccessed(boolean rmv, EvictableEntry<TestKey, String> e) {
            for (EvictableEntry e0 : entries)
                assertTrue(e0.equals(e));

            entries.add(e);
        }
    }

    private static class TestKey {
        private final int key;

        public TestKey(int key) {
            this.key = key;
        }

        @Override public boolean equals(Object other) {
            if (this == other)
                return true;

            if (other == null || getClass() != other.getClass())
                return false;

            TestKey testKey = (TestKey)other;

            return key == testKey.key;

        }

        @Override public int hashCode() {
            return key;
        }
    }
}
