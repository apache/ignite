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

import java.util.concurrent.atomic.AtomicInteger;
import javax.cache.configuration.CacheEntryListenerConfiguration;
import javax.cache.configuration.Factory;
import javax.cache.configuration.MutableCacheEntryListenerConfiguration;
import javax.cache.event.CacheEntryEvent;
import javax.cache.event.CacheEntryExpiredListener;
import javax.cache.event.CacheEntryListener;
import javax.cache.expiry.Duration;
import javax.cache.expiry.ModifiedExpiryPolicy;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.cache.CacheAtomicityMode;
import org.apache.ignite.cache.CacheMode;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.internal.util.lang.GridAbsPredicate;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.testframework.GridTestUtils;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.junit.Test;

import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static org.apache.ignite.cache.CacheAtomicityMode.ATOMIC;
import static org.apache.ignite.cache.CacheAtomicityMode.TRANSACTIONAL;
import static org.apache.ignite.cache.CacheMode.PARTITIONED;
import static org.apache.ignite.cache.CacheWriteSynchronizationMode.FULL_SYNC;

/**
 *
 */
public class IgniteCacheEntryListenerExpiredEventsTest extends GridCommonAbstractTest {
    /** */
    private static AtomicInteger evtCntr;

    /** {@inheritDoc} */
    @Override protected void beforeTestsStarted() throws Exception {
        super.beforeTestsStarted();

        startGrid(0);
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testExpiredEventAtomic() throws Exception {
        checkExpiredEvents(cacheConfiguration(PARTITIONED, ATOMIC));
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testExpiredEventTx() throws Exception {
        checkExpiredEvents(cacheConfiguration(PARTITIONED, TRANSACTIONAL));
    }

    /**
     * @param ccfg Cache configuration.
     * @throws Exception If failed.
     */
    private void checkExpiredEvents(CacheConfiguration<Object, Object> ccfg) throws Exception {
        IgniteCache<Object, Object> cache = ignite(0).createCache(ccfg);

        try {
            evtCntr = new AtomicInteger();

            CacheEntryListenerConfiguration<Object, Object> lsnrCfg = new MutableCacheEntryListenerConfiguration<>(
                new ExpiredListenerFactory(),
                null,
                true,
                false
            );

            cache.registerCacheEntryListener(lsnrCfg);

            IgniteCache<Object, Object> expiryCache =
                cache.withExpiryPolicy(new ModifiedExpiryPolicy(new Duration(MILLISECONDS, 500)));

            expiryCache.put(1, 1);

            for (int i = 0; i < 10; i++)
                cache.get(i);

            boolean wait = GridTestUtils.waitForCondition(new GridAbsPredicate() {
                @Override public boolean apply() {
                    return evtCntr.get() > 0;
                }
            }, 5000);

            assertTrue(wait);

            U.sleep(100);

            assertEquals(1, evtCntr.get());
        }
        finally {
            ignite(0).destroyCache(cache.getName());
        }
    }

    /**
     *
     * @param cacheMode Cache mode.
     * @param atomicityMode Cache atomicity mode.
     * @return Cache configuration.
     */
    private CacheConfiguration<Object, Object> cacheConfiguration(
        CacheMode cacheMode,
        CacheAtomicityMode atomicityMode) {
        CacheConfiguration<Object, Object> ccfg = new CacheConfiguration<>(DEFAULT_CACHE_NAME);

        ccfg.setAtomicityMode(atomicityMode);
        ccfg.setCacheMode(cacheMode);
        ccfg.setWriteSynchronizationMode(FULL_SYNC);

        if (cacheMode == PARTITIONED)
            ccfg.setBackups(1);

        return ccfg;
    }

    /**
     *
     */
    private static class ExpiredListenerFactory implements Factory<CacheEntryListener<Object, Object>> {
        /** {@inheritDoc} */
        @Override public CacheEntryListener<Object, Object> create() {
            return new ExpiredListener();
        }
    }

    /**
     *
     */
    private static class ExpiredListener implements CacheEntryExpiredListener<Object, Object> {
        /** {@inheritDoc} */
        @Override public void onExpired(Iterable<CacheEntryEvent<?, ?>> evts) {
            for (CacheEntryEvent<?, ?> evt : evts)
                evtCntr.incrementAndGet();
        }
    }
}
