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
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.Lock;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.cache.CacheMode;
import org.apache.ignite.cache.CacheWriteSynchronizationMode;
import org.apache.ignite.cache.eviction.EvictableEntry;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.configuration.NearCacheConfiguration;
import org.apache.ignite.events.Event;
import org.apache.ignite.events.EventType;
import org.apache.ignite.lang.IgnitePredicate;
import org.apache.ignite.testframework.MvccFeatureChecker;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.junit.Test;

import static java.util.concurrent.TimeUnit.SECONDS;
import static org.apache.ignite.cache.CacheAtomicityMode.TRANSACTIONAL;
import static org.apache.ignite.cache.CacheMode.LOCAL;
import static org.apache.ignite.cache.CacheMode.PARTITIONED;
import static org.apache.ignite.cache.CacheMode.REPLICATED;
import static org.apache.ignite.events.EventType.EVT_CACHE_ENTRY_EVICTED;

/**
 *
 */
public class GridCacheEvictionLockUnlockSelfTest extends GridCommonAbstractTest {
    /** Evict latch. */
    private static CountDownLatch evictLatch;

    /** Evict counter. */
    private static final AtomicInteger evictCnt = new AtomicInteger();

    /** Touch counter. */
    private static final AtomicInteger touchCnt = new AtomicInteger();

    /** Cache mode. */
    private CacheMode mode;

    /** Number of grids to start. */
    private int gridCnt;

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String igniteInstanceName) throws Exception {
        MvccFeatureChecker.skipIfNotSupported(MvccFeatureChecker.Feature.LOCAL_CACHE);
        MvccFeatureChecker.skipIfNotSupported(MvccFeatureChecker.Feature.ENTRY_LOCK);

        IgniteConfiguration c = super.getConfiguration(igniteInstanceName);

        CacheConfiguration cc = defaultCacheConfiguration();

        cc.setCacheMode(mode);
        cc.setWriteSynchronizationMode(CacheWriteSynchronizationMode.FULL_SYNC);
        cc.setEvictionPolicy(new EvictionPolicy());
        cc.setOnheapCacheEnabled(true);
        cc.setAtomicityMode(TRANSACTIONAL);

        NearCacheConfiguration nearCfg = new NearCacheConfiguration();

        nearCfg.setNearEvictionPolicy(new EvictionPolicy());
        cc.setNearConfiguration(nearCfg);

        if (mode == PARTITIONED)
            cc.setBackups(1);

        c.setCacheConfiguration(cc);

        c.setIncludeEventTypes(EventType.EVTS_ALL);

        return c;
    }

    /** {@inheritDoc} */
    @Override protected void beforeTestsStarted() throws Exception {
        MvccFeatureChecker.skipIfNotSupported(MvccFeatureChecker.Feature.LOCAL_CACHE);
        MvccFeatureChecker.skipIfNotSupported(MvccFeatureChecker.Feature.ENTRY_LOCK);

        super.beforeTestsStarted();
    }

    /** @throws Exception If failed. */
    @Test
    public void testLocal() throws Exception {
        mode = LOCAL;
        gridCnt = 1;

        doTest();
    }

    /** @throws Exception If failed. */
    @Test
    public void testReplicated() throws Exception {
        mode = REPLICATED;
        gridCnt = 3;

        doTest();
    }

    /** @throws Exception If failed. */
    @Test
    public void testPartitioned() throws Exception {
        mode = PARTITIONED;
        gridCnt = 3;

        doTest();
    }

    /** @throws Exception If failed. */
    private void doTest() throws Exception {
        try {
            startGridsMultiThreaded(gridCnt);

            for (int i = 0; i < gridCnt; i++)
                grid(i).events().localListen(new EvictListener(), EVT_CACHE_ENTRY_EVICTED);

            for (int i = 0; i < gridCnt; i++) {
                reset();

                IgniteCache<Object, Object> cache = jcache(i);

                Lock lock = cache.lock("key");

                lock.lock();
                lock.unlock();

                assertTrue(evictLatch.await(3, SECONDS));

                assertEquals(gridCnt, evictCnt.get());
                assertEquals(gridCnt, touchCnt.get());

                for (int j = 0; j < gridCnt; j++)
                    assertEquals(0, jcache(j).size());
            }
        }
        finally {
            stopAllGrids();
        }
    }

    /** @throws Exception If failed. */
    private void reset() throws Exception {
        evictLatch = new CountDownLatch(gridCnt);

        evictCnt.set(0);
        touchCnt.set(0);
    }

    /** Eviction event listener. */
    private static class EvictListener implements IgnitePredicate<Event> {
        /** {@inheritDoc} */
        @Override public boolean apply(Event evt) {
            assert evt.type() == EVT_CACHE_ENTRY_EVICTED;

            evictCnt.incrementAndGet();

            evictLatch.countDown();

            return true;
        }
    }

    /** Eviction policy. */
    private static class EvictionPolicy implements org.apache.ignite.cache.eviction.EvictionPolicy<Object, Object>, Serializable {
        /** {@inheritDoc} */
        @Override public void onEntryAccessed(boolean rmv, EvictableEntry<Object, Object> entry) {
            touchCnt.incrementAndGet();

            entry.evict();
        }
    }
}
