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

import java.util.Random;
import java.util.concurrent.TimeUnit;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.configuration.MemoryConfiguration;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.internal.util.GridRandom;
import org.jetbrains.annotations.NotNull;

/**
 * Base class for memory leaks tests.
 */
public abstract class IgniteDbMemoryLeakAbstractTest extends IgniteDbAbstractTest {
    /** */
    @SuppressWarnings("WeakerAccess") protected static final int CONCURRENCY_LEVEL = 8;
    /** */
    private volatile Exception ex = null;

    /** */
    private long warmUpEndTime;

    /** */
    private long endTime;

    /** */
    private long loadedPages = 0;

    /** */
    private long delta = 0;

    /** */
    private long probeCnt = 0;

    /** */
    private static final ThreadLocal<Random> THREAD_LOCAL_RANDOM = new ThreadLocal<>();

    @Override protected void beforeTest() throws Exception {
        super.beforeTest();

        long startTime = System.nanoTime();
        warmUpEndTime = startTime + TimeUnit.SECONDS.toNanos(warmUp());
        endTime = warmUpEndTime + TimeUnit.SECONDS.toNanos(duration());
    }

    /** {@inheritDoc} */
    @Override protected void configure(IgniteConfiguration cfg) {
        cfg.setMetricsLogFrequency(5000);
    }

    /** {@inheritDoc} */
    @Override protected void configure(MemoryConfiguration mCfg) {
        mCfg.setConcurrencyLevel(CONCURRENCY_LEVEL);
    }

    /**
     * @return Test duration in seconds.
     */
    protected int duration() {
        return 300;
    }

    /**
     * @return Warm up duration.
     */
    @SuppressWarnings("WeakerAccess") protected int warmUp() {
        return 300;
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
        return (warmUp() + duration() + 1) * 1000; // One extra second to stop all threads
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
        Object value = value(key);

        switch (getRandom().nextInt(3)) {
            case 0:
                cache.getAndPut(key, value);

                break;
            case 1:
                cache.get(key);

                break;
            case 2:
                cache.getAndRemove(key);
        }
    }

    /**
     * @return Random.
     */
    @NotNull protected static Random getRandom() {
        Random rnd = THREAD_LOCAL_RANDOM.get();

        if (rnd == null) {
            rnd = new GridRandom();
            THREAD_LOCAL_RANDOM.set(rnd);
        }

        return rnd;
    }

    /**
     * @throws Exception If failed.
     */
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

        Thread.sleep(TimeUnit.NANOSECONDS.toMillis(warmUpEndTime - System.nanoTime()));

        info("Warming up is ended.");

        while (System.nanoTime() < endTime) {
            try {
                check(ignite);
            }
            catch (Exception e) {
                ex = e;

                break;
            }

            Thread.sleep(TimeUnit.SECONDS.toMillis(5));
        }

        for (Thread thread : threads)
            thread.join();

        if (ex != null)
            throw ex;
    }

    /**
     * Callback to check the current state.
     *
     * @param ig Ignite instance.
     * @throws Exception If failed.
     */
    protected void check(IgniteEx ig) throws Exception {
        long pagesActual = ig.context().cache().context().database().pageMemory().loadedPages();
        long pagesMax = pagesMax();

        assertTrue(
            "Maximal allowed pages number is exceeded. [allowed=" + pagesMax + "; actual= " + pagesActual + "]",
            pagesActual <= pagesMax);

        if (loadedPages > 0) {
            delta += pagesActual - loadedPages;
            int allowedDelta = pagesDelta();

            if(probeCnt++ > 12) { // we need some statistic first. Minimal statistic is taken for a minute.
                long actualDelta = delta / probeCnt;

                assertTrue(
                    "Average growth pages in the number is more than expected. [allowed=" + allowedDelta + "; actual=" + actualDelta + "]",
                    actualDelta <= allowedDelta);
            }
        }

        loadedPages = pagesActual;
    }

    /**
     * @return Maximal allowed pages number.
     */
    protected abstract long pagesMax();

    /**
     * @return Expected average number of pages, on which their total number can grow per 5 seconds.
     */
    @SuppressWarnings("WeakerAccess") protected int pagesDelta() {
        return 5;
    }
}
