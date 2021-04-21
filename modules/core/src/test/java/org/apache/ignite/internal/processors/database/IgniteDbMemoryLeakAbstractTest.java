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

package org.apache.ignite.internal.processors.database;

import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.configuration.DataRegionConfiguration;
import org.apache.ignite.configuration.DataStorageConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.internal.processors.cache.IgniteCacheProxy;
import org.apache.ignite.internal.processors.cache.persistence.DataStructure;
import org.junit.Test;

import static org.apache.ignite.IgniteSystemProperties.getInteger;

/**
 * Base class for memory leaks tests.
 */
public abstract class IgniteDbMemoryLeakAbstractTest extends IgniteDbAbstractTest {
    /** */
    private static final int CONCURRENCY_LEVEL = 16;

    /** */
    private static final int MIN_PAGE_CACHE_SIZE = 1048576 * CONCURRENCY_LEVEL;

    /** */
    private volatile Exception ex;

    /** */
    private long warmUpEndTime;

    /** */
    private long endTime;

    /** */
    private long loadedPages;

    /** */
    private long delta;

    /** */
    private long probeCnt;

    /** {@inheritDoc} */
    @Override protected void beforeTest() throws Exception {
        super.beforeTest();

        DataStructure.rnd = null;

        long startTime = System.nanoTime();

        warmUpEndTime = startTime + TimeUnit.SECONDS.toNanos(warmUp());

        endTime = warmUpEndTime + TimeUnit.SECONDS.toNanos(duration());
    }

    /** {@inheritDoc} */
    @Override protected void configure(IgniteConfiguration cfg) {
        cfg.setMetricsLogFrequency(5000);
    }

    /** {@inheritDoc} */
    @Override protected void configure(DataStorageConfiguration mCfg) {
        mCfg.setConcurrencyLevel(CONCURRENCY_LEVEL);

        long size = (1024 * (isLargePage() ? 16 : 4) + 24) * pagesMax();

        mCfg.setDefaultDataRegionConfiguration(
            new DataRegionConfiguration().setMaxSize(Math.max(size, MIN_PAGE_CACHE_SIZE)).setName("default"));
    }

    /**
     * @return Test duration in seconds.
     */
    protected int duration() {
        return getInteger("IGNITE_MEMORY_LEAKS_TEST_DURATION", 300);
    }

    /**
     * @return Warm up duration in seconds.
     */
    @SuppressWarnings("WeakerAccess")
    protected int warmUp() {
        return getInteger("IGNITE_MEMORY_LEAKS_TEST_WARM_UP", 450);
    }

    /** {@inheritDoc} */
    @Override protected int gridCount() {
        return 1;
    }

    /** {@inheritDoc} */
    @Override protected boolean indexingEnabled() {
        return false;
    }

    /** {@inheritDoc} */
    @Override protected long getTestTimeout() {
        return (warmUp() + duration() + 10) * 1000; // Extra seconds to stop all threads.
    }

    /**
     * @param ig Ignite instance.
     * @return IgniteCache.
     */
    protected abstract IgniteCache<Object, Object> cache(IgniteEx ig);

    /**
     * @return Cache key to perform an operation.
     */
    protected abstract Object key();

    /**
     * @param key Cache key to perform an operation.
     * @return Cache value to perform an operation.
     */
    protected abstract Object value(Object key);

    /**
     * @param cache IgniteCache.
     */
    protected void operation(IgniteCache<Object, Object> cache) {
        Object key = key();
        Object val = value(key);

        switch (nextInt(3)) {
            case 0:
                cache.getAndPut(key, val);

                break;

            case 1:
                cache.get(key);

                break;

            case 2:
                cache.getAndRemove(key);
        }
    }

    /**
     * @param bound Upper bound (exclusive). Must be positive.
     * @return Random int value.
     */
    protected static int nextInt(int bound) {
        return ThreadLocalRandom.current().nextInt(bound);
    }

    /**
     * @return Random int value.
     */
    protected static int nextInt() {
        return ThreadLocalRandom.current().nextInt();
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testMemoryLeak() throws Exception {
        final IgniteEx ignite = grid(0);

        final IgniteCache<Object, Object> cache = cache(ignite);

        Runnable target = new Runnable() {
            @Override public void run() {
                while (ex == null && System.nanoTime() < endTime) {
                    try {
                        operation(cache);
                    }
                    catch (Exception e) {
                        ex = e;

                        break;
                    }
                }
            }
        };

        Thread[] threads = new Thread[CONCURRENCY_LEVEL];

        info("Warming up is started.");

        for (int i = 0; i < threads.length; i++) {
            threads[i] = new Thread(target);
            threads[i].start();
        }

        while (ex == null && System.nanoTime() < warmUpEndTime)
            Thread.sleep(100);

        if (ex != null)
            throw ex;

        info("Warming up is ended.");

        while (ex == null && System.nanoTime() < endTime) {
            try {
                check(cache);
            }
            catch (Exception e) {
                ex = e;

                break;
            }

            Thread.sleep(TimeUnit.SECONDS.toMillis(5));
        }

        if (ex != null)
            throw ex;
    }

    /**
     * Callback to check the current state.
     *
     * @param cache Cache instance.
     * @throws Exception If failed.
     */
    protected final void check(IgniteCache cache) throws Exception {
        long pagesActual = ((IgniteCacheProxy)cache).context().dataRegion().pageMemory().loadedPages();

        if (loadedPages > 0) {
            delta += pagesActual - loadedPages;

            int allowedDelta = pagesDelta();

            if (probeCnt++ > 12) { // We need some statistic first. Minimal statistic is taken for a minute.
                long actualDelta = delta / probeCnt;

                assertTrue(
                    "Average growth pages in the number is more than expected [allowed=" + allowedDelta + ", actual=" + actualDelta + "]",
                    actualDelta <= allowedDelta);
            }
        }

        long pagesAllowed = pagesMax();

        assertTrue("Allocated pages count is more than expected [allowed=" + pagesAllowed + ", actual=" + pagesActual + "]", pagesActual < pagesAllowed);

        loadedPages = pagesActual;
    }

    /**
     * @return Maximal allowed pages number.
     */
    protected abstract long pagesMax();

    /**
     * @return Expected average number of pages, on which their total number can grow per 5 seconds.
     */
    @SuppressWarnings("WeakerAccess")
    protected int pagesDelta() {
        return 3;
    }
}
