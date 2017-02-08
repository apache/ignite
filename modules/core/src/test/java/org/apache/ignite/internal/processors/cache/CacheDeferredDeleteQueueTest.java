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

import java.util.Collection;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.cache.CacheAtomicityMode;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.NearCacheConfiguration;
import org.apache.ignite.internal.IgniteKernal;
import org.apache.ignite.internal.processors.cache.distributed.dht.GridDhtLocalPartition;
import org.apache.ignite.internal.processors.cache.distributed.dht.GridDhtPartitionTopology;
import org.apache.ignite.internal.util.lang.GridAbsPredicate;
import org.apache.ignite.testframework.GridTestUtils;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;

import static org.apache.ignite.IgniteSystemProperties.IGNITE_CACHE_REMOVED_ENTRIES_TTL;
import static org.apache.ignite.cache.CacheAtomicityMode.ATOMIC;
import static org.apache.ignite.cache.CacheAtomicityMode.TRANSACTIONAL;
import static org.apache.ignite.cache.CacheMode.PARTITIONED;
import static org.apache.ignite.cache.CacheWriteSynchronizationMode.FULL_SYNC;

/**
 *
 */
public class CacheDeferredDeleteQueueTest extends GridCommonAbstractTest {
    /** */
    private static String ttlProp;

    /** */
    private static int NODES = 2;

    /** {@inheritDoc} */
    @Override protected void beforeTestsStarted() throws Exception {
        ttlProp = System.getProperty(IGNITE_CACHE_REMOVED_ENTRIES_TTL);

        System.setProperty(IGNITE_CACHE_REMOVED_ENTRIES_TTL, "1000");

        startGridsMultiThreaded(NODES);
    }

    /** {@inheritDoc} */
    @Override protected void afterTestsStopped() throws Exception {
        if (ttlProp != null)
            System.setProperty(IGNITE_CACHE_REMOVED_ENTRIES_TTL, ttlProp);
        else
            System.clearProperty(IGNITE_CACHE_REMOVED_ENTRIES_TTL);

        stopAllGrids();

        super.afterTestsStopped();
    }

    /**
     * @throws Exception If failed.
     */
    public void testDeferredDeleteQueue() throws Exception {
        testQueue(ATOMIC, false);

        testQueue(TRANSACTIONAL, false);

        testQueue(ATOMIC, true);

        testQueue(TRANSACTIONAL, true);
    }

    /**
     * @param atomicityMode Cache atomicity mode.
     * @param nearCache {@code True} if need create near cache.
     *
     * @throws Exception If failed.
     */
    private void testQueue(CacheAtomicityMode atomicityMode, boolean nearCache) throws Exception {
        CacheConfiguration<Integer, Integer> ccfg = new CacheConfiguration<>();

        ccfg.setCacheMode(PARTITIONED);
        ccfg.setAtomicityMode(atomicityMode);
        ccfg.setWriteSynchronizationMode(FULL_SYNC);
        ccfg.setBackups(1);

        if (nearCache)
            ccfg.setNearConfiguration(new NearCacheConfiguration<Integer, Integer>());

        IgniteCache<Integer, Integer> cache = ignite(0).createCache(ccfg);

        try {
            final int KEYS = cache.getConfiguration(CacheConfiguration.class).getAffinity().partitions() * 3;

            for (int i = 0; i < KEYS; i++)
                cache.put(i, i);

            for (int i = 0; i < KEYS; i++)
                cache.remove(i);

            boolean wait = GridTestUtils.waitForCondition(new GridAbsPredicate() {
                @Override public boolean apply() {
                    for (int i = 0; i < NODES; i++) {
                        final GridDhtPartitionTopology top =
                            ((IgniteKernal)ignite(i)).context().cache().cache(null).context().topology();

                        for (GridDhtLocalPartition p : top.currentLocalPartitions()) {
                            Collection<Object> rmvQueue = GridTestUtils.getFieldValue(p, "rmvQueue");

                            if (!rmvQueue.isEmpty() || p.size() != 0)
                                return false;
                        }
                    }

                    return true;
                }
            }, 5000);

            assertTrue("Failed to wait for rmvQueue cleanup.", wait);
        }
        finally {
            ignite(0).destroyCache(ccfg.getName());
        }
    }
}
