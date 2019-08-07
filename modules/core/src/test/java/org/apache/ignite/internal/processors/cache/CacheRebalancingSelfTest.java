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
 *
 */

package org.apache.ignite.internal.processors.cache;

import java.util.HashSet;
import java.util.Random;
import java.util.Set;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.cache.CacheMode;
import org.apache.ignite.cache.CachePeekMode;
import org.apache.ignite.cache.CacheWriteSynchronizationMode;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.internal.IgniteInternalFuture;
import org.apache.ignite.internal.IgniteInterruptedCheckedException;
import org.apache.ignite.internal.util.future.IgniteFutureImpl;
import org.apache.ignite.internal.util.lang.GridAbsPredicate;
import org.apache.ignite.lang.IgniteFuture;
import org.apache.ignite.testframework.GridTestUtils;
import org.apache.ignite.testframework.MvccFeatureChecker;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.junit.Test;

/**
 * Test for rebalancing.
 */
public class CacheRebalancingSelfTest extends GridCommonAbstractTest {
    /** Cache name with one backups */
    private static final String REBALANCE_TEST_CACHE_NAME = "rebalanceCache";

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String igniteInstanceName) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(igniteInstanceName);

        CacheConfiguration<Integer,Integer> ccfg = new CacheConfiguration<>();
        ccfg.setBackups(1);
        ccfg.setName(REBALANCE_TEST_CACHE_NAME);
        ccfg.setWriteSynchronizationMode(CacheWriteSynchronizationMode.FULL_SYNC);

        cfg.setCacheConfiguration(new CacheConfiguration(DEFAULT_CACHE_NAME), ccfg);

        return cfg;
    }

    /** {@inheritDoc} */
    @Override protected void afterTest() throws Exception {
        stopAllGrids();

        super.afterTest();
    }

    /**
     * @throws Exception If fails.
     */
    @Test
    public void testRebalanceLocalCacheFuture() throws Exception {
        MvccFeatureChecker.skipIfNotSupported(MvccFeatureChecker.Feature.LOCAL_CACHE);

        startGrid(
            getTestIgniteInstanceName(0),
            getConfiguration(getTestIgniteInstanceName(0))
                .setCacheConfiguration(
                    new CacheConfiguration<Integer, Integer>(DEFAULT_CACHE_NAME),
                    new CacheConfiguration<Integer, Integer>(REBALANCE_TEST_CACHE_NAME)
                        .setCacheMode(CacheMode.LOCAL))
        );

        IgniteCache<Integer, Integer> cache = ignite(0).cache(DEFAULT_CACHE_NAME);

        assertTrue(cache.rebalance().get());
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testRebalanceFuture() throws Exception {
        IgniteEx ig0 = startGrid(0);

        startGrid(1);

        IgniteCache<Object, Object> cache = ig0.cache(DEFAULT_CACHE_NAME);

        IgniteFuture fut1 = cache.rebalance();

        fut1.get();

        startGrid(2);

        IgniteFuture fut2 = cache.rebalance();

        assert internalFuture(fut2) != internalFuture(fut1);

        fut2.get();
    }

    /**
     * @param fut Future.
     * @return Internal future.
     */
    private static IgniteInternalFuture internalFuture(IgniteFuture fut) {
        assertTrue(fut.toString(), fut instanceof IgniteFutureImpl);

        return ((IgniteFutureImpl) fut).internalFuture();
    }

    /**
     * Test local cache size with and without rebalancing in case or topology change.
     *
     * @throws Exception If failed.
     */
    @Test
    public void testDisableRebalancing() throws Exception {
        IgniteEx ig0 = startGrid(0);
        IgniteEx ig1 = startGrid(1);
        startGrid(2);

        ig1.rebalanceEnabled(false);

        Random r = new Random();

        int totalKeysCnt = 10240;

        final Set<Integer> keys = new HashSet<>();
        while (keys.size() < totalKeysCnt)
            keys.add(r.nextInt());

        IgniteCache<Integer, Integer> cache = ig0.getOrCreateCache(REBALANCE_TEST_CACHE_NAME);

        for (Integer next : keys)
            cache.put(next, 1);

        testLocalCacheSize(ig0, 0, totalKeysCnt);
        int before_ig1 = testLocalCacheSize(ig1, 0, totalKeysCnt);

        stopGrid(2);

        testLocalCacheSize(ig0, totalKeysCnt, null);
        testLocalCacheSize(ig1, before_ig1, null);

        ig1.rebalanceEnabled(true);

        testLocalCacheSize(ig0, totalKeysCnt, null);
        testLocalCacheSize(ig1, totalKeysCnt, null);
    }

    /**
     * Test if test cache in specified node have correct local size. Waits size to became correct for some time.
     *
     * @param ignite node to test.
     * @param expFrom left bound, or exact value if {@code expTo} is {@code null}.
     * @param expTo right bound (or {@code null}).
     * @return actual local cache size.
     * @throws IgniteInterruptedCheckedException if interrupted.
     */
    private int testLocalCacheSize(Ignite ignite, final Integer expFrom, final Integer expTo) throws IgniteInterruptedCheckedException {
        final IgniteCache cache = ignite.cache(REBALANCE_TEST_CACHE_NAME);

        boolean isOk = GridTestUtils.waitForCondition(new GridAbsPredicate() {
            @Override public boolean apply() {
                Integer actualSize = cache.localSize(CachePeekMode.ALL);

                return expTo == null ? expFrom.equals(actualSize) : expFrom <= actualSize && actualSize <= expTo;
            }
        }, 10_000);

        int rslt = cache.localSize(CachePeekMode.ALL);

        assertTrue(ignite.configuration().getIgniteInstanceName() + " cache local size = "
            + rslt + " not " + (expTo == null ? "equal " + expFrom : "in " + expFrom + "-" + expTo), isOk);

        return rslt;
    }
}
