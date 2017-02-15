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
    private volatile Exception ex = null;

    /** */
    private static final ThreadLocal<Random> THREAD_LOCAL_RANDOM = new ThreadLocal<>();

    /** {@inheritDoc} */
    @Override protected void configure(IgniteConfiguration cfg) {
        cfg.setMetricsLogFrequency(5000);
    }

    /** {@inheritDoc} */
    @Override protected void configure(MemoryConfiguration mCfg) {
        int concLvl = Runtime.getRuntime().availableProcessors();

        mCfg.setConcurrencyLevel(concLvl);
        mCfg.setPageCacheSize(1024 * 1024 * concLvl); //minimal possible value
    }

    /**
     * @return Test duration in seconds.
     */
    protected int duration() {
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
        return (duration() + 1) * 1000;
    }

    /**
     * @param ig Ignite instance.
     * @return IgniteCache.
     */
    protected abstract IgniteCache<Object,Object> cache(IgniteEx ig);

    /**
     * @return Cache key to perform an operation.
     */
    protected abstract Object key();

    /**
     * @return Cache value to perform an operation.
     * @param key Cache key to perform an operation.
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

        if(rnd == null){
            rnd = new GridRandom();
            THREAD_LOCAL_RANDOM.set(rnd);
        }

        return rnd;
    }

    /**
     * @throws Exception If failed.
     */
    public void testMemoryLeak() throws Exception {
        final long end = System.nanoTime() + TimeUnit.SECONDS.toNanos(duration());

        final IgniteEx ignite = grid(0);
        final IgniteCache<Object, Object> cache = cache(ignite);

        Runnable target = new Runnable() {
            @Override public void run() {
                while (ex == null && System.nanoTime() < end) {
                    try {
                        operation(cache);
                        check(ignite);
                    }
                    catch (Exception e) {
                        ex = e;

                        break;
                    }
                }
            }
        };

        Thread[] threads = new Thread[Runtime.getRuntime().availableProcessors()];

        for (int i = 0; i < threads.length; i++) {
            threads[i] = new Thread(target);
            threads[i].start();
        }

        for (Thread thread : threads) {
            thread.join();
        }

        if(ex != null){
            throw ex;
        }
    }

    /**
     * Callback to check the current state
     *
     * @param ig Ignite instance
     * @throws Exception If failed.
     */
    protected void check(IgniteEx ig) throws Exception {
    }
}
