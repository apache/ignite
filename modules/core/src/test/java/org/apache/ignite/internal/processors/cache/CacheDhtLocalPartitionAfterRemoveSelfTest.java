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

package org.apache.ignite.internal.processors.cache;

import org.apache.ignite.*;
import org.apache.ignite.cache.*;
import org.apache.ignite.configuration.*;
import org.apache.ignite.internal.processors.cache.distributed.dht.*;
import org.apache.ignite.testframework.junits.common.*;

/**
 * Test for remove operation.
 */
public class CacheDhtLocalPartitionAfterRemoveSelfTest extends GridCommonAbstractTest {
    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String gridName) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(gridName);

        CacheConfiguration ccfg = new CacheConfiguration();

        ccfg.setAtomicityMode(CacheAtomicityMode.TRANSACTIONAL);
        ccfg.setEvictSynchronized(false);
        ccfg.setNearConfiguration(null);

        cfg.setCacheConfiguration(ccfg);

        return cfg;
    }

    /** {@inheritDoc} */
    @Override protected void beforeTestsStarted() throws Exception {
        startGrids(1);
    }

    /** {@inheritDoc} */
    @Override protected void afterTestsStopped() throws Exception {
        stopAllGrids();
    }

    /**
     * @throws Exception If failed.
     */
    public void testMemoryUsage() throws Exception {
        IgniteCache<TestKey, Integer> cache = grid(0).cache(null);

        for (int i = 0; i < 1000; ++i)
            cache.put(new TestKey("" + i), i);

        for (int i = 0; i < 1000; ++i)
            assert cache.getAndRemove(new TestKey("" + i)).equals(i);

        assertEquals(0, cache.size());

        int size = 0;

        for (GridDhtLocalPartition p : dht(cache).topology().localPartitions()) {
            int pSize = p.size();

            size += pSize;
        }

        System.out.println("All size: " + size);
    }

    /**
     * Test key.
     */
    private static class TestKey {
        /** Key. */
        private String key;

        /**
         * @param key Key.
         */
        public TestKey(String key) {
            this.key = key;
        }

        /** {@inheritDoc} */
        @Override public int hashCode() {
            return key.hashCode();
        }

        /** {@inheritDoc} */
        @Override public boolean equals(Object obj) {
            if (obj == null || !(obj instanceof TestKey))
                return false;

            return key.equals(((TestKey)obj).key);
        }
    }
}
