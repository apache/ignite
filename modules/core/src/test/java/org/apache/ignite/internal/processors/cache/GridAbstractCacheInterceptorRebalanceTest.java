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

import org.apache.ignite.IgniteCache;
import org.apache.ignite.cache.CacheAtomicityMode;
import org.apache.ignite.cache.CacheInterceptor;
import org.apache.ignite.cache.CacheMode;
import org.apache.ignite.cache.CacheRebalanceMode;
import org.apache.ignite.cache.CacheWriteSynchronizationMode;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.internal.IgniteInternalFuture;
import org.apache.ignite.lang.IgniteBiTuple;
import org.apache.ignite.testframework.GridTestUtils;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.jetbrains.annotations.Nullable;

import javax.cache.Cache;
import java.util.concurrent.Callable;
import java.util.concurrent.CountDownLatch;

/**
 *
 */
public abstract class GridAbstractCacheInterceptorRebalanceTest extends GridCommonAbstractTest {
    /** */
    private static final String CACHE_NAME = "test_cache";

    /** */
    private static final int CNT = 10_000;

    /** */
    private static volatile boolean inited;

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(final String gridName) throws Exception {
        final IgniteConfiguration cfg = super.getConfiguration(gridName);

        final CacheConfiguration<Integer, Integer> ccfg = new CacheConfiguration<>(CACHE_NAME);

        ccfg.setInterceptor(new RebalanceInterceptor());
        ccfg.setAtomicityMode(atomicityMode());
        ccfg.setWriteSynchronizationMode(CacheWriteSynchronizationMode.FULL_SYNC);
        ccfg.setCacheMode(CacheMode.REPLICATED);
        ccfg.setRebalanceMode(CacheRebalanceMode.SYNC);
        ccfg.setBackups(2);

        cfg.setCacheConfiguration(ccfg);
        cfg.setLateAffinityAssignment(true);

        return cfg;
    }

    /**
     * @return Cache atomicity mode.
     */
    protected abstract CacheAtomicityMode atomicityMode();

    /** {@inheritDoc} */
    @Override protected void beforeTest() throws Exception {
        inited = false;
    }

    /** {@inheritDoc} */
    @Override protected void afterTest() throws Exception {
        stopAllGrids();
    }

    /**
     * @throws Exception If fail.
     */
    public void testRebalanceUpdate() throws Exception {
        final IgniteEx ignite = startGrid(1);

        final IgniteCache<Integer, Integer> cache = ignite.cache(CACHE_NAME);

        for (int i = 0; i < CNT; i++)
            cache.put(i, i);

        inited = true;

        final CountDownLatch latch = new CountDownLatch(1);

        final IgniteInternalFuture<Object> updFut = GridTestUtils.runAsync(new Callable<Object>() {
            @Override public Object call() throws Exception {
                latch.await();

                for (int j = 1; j <= 3; j++) {
                    for (int i = 0; i < CNT; i++)
                        cache.put(i, i + j);
                }

                return null;
            }
        });

        final IgniteInternalFuture<Object> rebFut = GridTestUtils.runAsync(new Callable<Object>() {
            @Override public Object call() throws Exception {
                latch.await();

                for (int i = 2; i < 5; i++)
                    startGrid(i);

                return null;
            }
        });

        latch.countDown();

        updFut.get();
        rebFut.get();

        stopAllGrids();
    }

    /**
     *
     */
    private static class RebalanceInterceptor implements CacheInterceptor<Integer, Integer> {
        /** */
        private static final long serialVersionUID = 0L;

        /** {@inheritDoc} */
        @Nullable @Override public Integer onBeforePut(final Cache.Entry entry, final Integer newVal) {
            if (inited && entry.getValue() == null) {
                fail("Null old value [newVal=" + newVal + "]");

                return newVal;
            }

            final int old = inited ? (Integer) entry.getValue() : 0;

            if (!inited)
                assert entry.getValue() == null;
            else
                assertEquals(newVal.intValue(), old + 1);

            return newVal;
        }

        /** {@inheritDoc} */
        @Override public void onAfterPut(final Cache.Entry entry) {

        }

        /** {@inheritDoc} */
        @Nullable @Override public Integer onGet(final Integer key, @Nullable final Integer val) {
            return val;
        }

        /** {@inheritDoc} */
        @Nullable @Override public IgniteBiTuple<Boolean, Integer> onBeforeRemove(
            final Cache.Entry<Integer, Integer> entry) {
            return null;
        }

        /** {@inheritDoc} */
        @Override public void onAfterRemove(final Cache.Entry<Integer, Integer> entry) {

        }
    }
}
