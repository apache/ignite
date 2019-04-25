/*
 * Copyright 2019 GridGain Systems, Inc. and Contributors.
 *
 * Licensed under the GridGain Community Edition License (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     https://www.gridgain.com/products/software/community-edition/gridgain-community-edition-license
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.ignite.internal.processors.cache;

import java.util.concurrent.locks.Lock;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.testframework.MvccFeatureChecker;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.junit.Before;
import org.junit.Test;

import static org.apache.ignite.cache.CacheAtomicityMode.TRANSACTIONAL;

/**
 *
 */
public class CrossCacheLockTest extends GridCommonAbstractTest {
    /** */
    private static final int GRID_CNT = 4;

    /** */
    private static final String CACHE1 = "cache1";

    /** */
    private static final String CACHE2 = "cache2";

    /** */
    @Before
    public void beforeCrossCacheLockTest() {
        MvccFeatureChecker.skipIfNotSupported(MvccFeatureChecker.Feature.ENTRY_LOCK);
    }

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String igniteInstanceName) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(igniteInstanceName);

        if (igniteInstanceName.equals(getTestIgniteInstanceName(GRID_CNT - 1)))
            cfg.setClientMode(true);

        CacheConfiguration ccfg1 = new CacheConfiguration(DEFAULT_CACHE_NAME);
        ccfg1.setName(CACHE1);
        ccfg1.setBackups(1);
        ccfg1.setAtomicityMode(TRANSACTIONAL);

        CacheConfiguration ccfg2 = new CacheConfiguration(DEFAULT_CACHE_NAME);
        ccfg2.setName(CACHE2);
        ccfg2.setAtomicityMode(TRANSACTIONAL);

        cfg.setCacheConfiguration(ccfg1, ccfg2);

        return cfg;
    }

    /** {@inheritDoc} */
    @Override protected void beforeTestsStarted() throws Exception {
        super.beforeTestsStarted();

        startGridsMultiThreaded(GRID_CNT - 1);

        startGrid(GRID_CNT - 1);
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testLockUnlock() throws Exception {
        for (int i = 0; i < GRID_CNT; i++) {
            Ignite ignite = ignite(i);

            log.info("Check node: " + ignite.name());

            IgniteCache<Integer, Integer> cache1 = ignite.cache(CACHE1);
            IgniteCache<Integer, Integer> cache2 = ignite.cache(CACHE2);

            for (int k = 0; k < 1000; k++) {
                Lock lock1 = null;
                Lock lock2 = null;

                try {
                    lock1 = cache1.lock(k);

                    assertTrue(lock1.tryLock());

                    assertTrue(cache1.isLocalLocked(k, true));
                    assertFalse(cache2.isLocalLocked(k, true));

                    lock2 = cache2.lock(k);

                    assertTrue(lock2.tryLock());

                    assertTrue(cache1.isLocalLocked(k, true));
                    assertTrue(cache2.isLocalLocked(k, true));

                    lock2.unlock();

                    lock2 = null;

                    assertTrue(cache1.isLocalLocked(k, true));
                    assertFalse(cache2.isLocalLocked(k, true));

                    lock1.unlock();

                    lock1 = null;

                    assertFalse(cache1.isLocalLocked(k, true));
                    assertFalse(cache2.isLocalLocked(k, true));
                }
                finally {
                    if (lock1 != null)
                        lock1.unlock();

                    if (lock2 != null)
                        lock2.unlock();
                }
            }
        }
    }
}
