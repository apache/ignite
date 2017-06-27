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
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.cache.CacheAtomicityMode;
import org.apache.ignite.cache.eviction.EvictableEntry;
import org.apache.ignite.cache.eviction.EvictionPolicy;
import org.apache.ignite.cache.eviction.fifo.FifoEvictionPolicy;
import org.apache.ignite.cache.eviction.lru.LruEvictionPolicy;
import org.apache.ignite.cache.eviction.sorted.SortedEvictionPolicy;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;

/**
 * Tests eviction policy notification in case of {@link IgniteCache#removeAll} call.
 */
public class EvictionPolicyRemoveAllNotificationSelfTest extends GridCommonAbstractTest {
    /** {@inheritDoc} */
    @Override protected void beforeTestsStarted() throws Exception {
        startGrid(0);
    }

    /** {@inheritDoc} */
    @Override protected void afterTestsStopped() throws Exception {
        stopAllGrids();
    }

    /**
     * @throws Exception If fails.
     */
    public void testNotifyPolicy() throws Exception {
        int max = 5;

        doTestNotifyPolicy(CacheAtomicityMode.TRANSACTIONAL, new LruEvictionPolicy<>(max));
        doTestNotifyPolicy(CacheAtomicityMode.TRANSACTIONAL, new FifoEvictionPolicy<>(max));
        doTestNotifyPolicy(CacheAtomicityMode.TRANSACTIONAL, new SortedEvictionPolicy<>(max));

        doTestNotifyPolicy(CacheAtomicityMode.ATOMIC, new LruEvictionPolicy<>(max));
        doTestNotifyPolicy(CacheAtomicityMode.ATOMIC, new FifoEvictionPolicy<>(max));
        doTestNotifyPolicy(CacheAtomicityMode.ATOMIC, new SortedEvictionPolicy<>(max));
    }

    /**
     * @param mode Mode.
     * @param targetPlc Target policy.
     */
    private <K, V> void doTestNotifyPolicy(CacheAtomicityMode mode, EvictionPolicy<K, V> targetPlc) {
        CacheConfiguration ccfg = new CacheConfiguration();
        ccfg.setAtomicityMode(mode);

        EvictionPolicyWrapper plc = new EvictionPolicyWrapper<>(targetPlc);

        ccfg.setEvictionPolicy(plc);

        IgniteEx ignite = grid(0);

        IgniteCache<Integer, String> cache = ignite.getOrCreateCache(ccfg);

        try {
            cache.put(1, "1");
            cache.put(2, "2");
            cache.put(3, "3");
            cache.put(4, "4");
            cache.put(5, "5");

            cache.removeAll();

            assertEquals(5, plc.getRemoveCount());
        }
        finally {
            if (cache != null)
                cache.destroy();
        }
    }

    /**
     *
     */
    private static class EvictionPolicyWrapper<K, V> implements EvictionPolicy<K, V>, Serializable {
        /** Policy. */
        private final EvictionPolicy<K, V> plc;

        /** Remove count. */
        private final AtomicInteger rmvCnt = new AtomicInteger();

        /**
         * @param plc Policy.
         */
        public EvictionPolicyWrapper(EvictionPolicy<K, V> plc) {
            this.plc = plc;
        }

        /** {@inheritDoc} */
        @Override public void onEntryAccessed(boolean rmv, EvictableEntry<K, V> entry) {
            plc.onEntryAccessed(rmv, entry);

            if (rmv)
                rmvCnt.incrementAndGet();
        }

        /**
         * @return remove notifications count.
         */
        public int getRemoveCount() {
            return rmvCnt.get();
        }
    }
}
