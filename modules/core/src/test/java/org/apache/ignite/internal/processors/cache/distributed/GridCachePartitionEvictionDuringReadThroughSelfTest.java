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

package org.apache.ignite.internal.processors.cache.distributed;

import java.util.LinkedHashSet;
import java.util.Set;
import java.util.concurrent.Callable;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import javax.cache.Cache;
import javax.cache.configuration.Factory;
import javax.cache.expiry.CreatedExpiryPolicy;
import javax.cache.expiry.Duration;
import javax.cache.integration.CacheLoaderException;
import javax.cache.integration.CacheWriterException;
import org.apache.ignite.cache.CacheAtomicityMode;
import org.apache.ignite.cache.eviction.lru.LruEvictionPolicy;
import org.apache.ignite.cache.store.CacheStore;
import org.apache.ignite.cache.store.CacheStoreAdapter;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.configuration.NearCacheConfiguration;
import org.apache.ignite.internal.IgniteInternalFuture;
import org.apache.ignite.internal.IgniteInterruptedCheckedException;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.testframework.GridTestUtils;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.junit.Test;

/**
 *
 */
public class GridCachePartitionEvictionDuringReadThroughSelfTest extends GridCommonAbstractTest {
    /** Failing key. */
    private static final int FAILING_KEY = 3;

    /** Data read grid index. */
    private static final int DATA_READ_GRID_IDX = 0;

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String gridName) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(gridName);

        CacheConfiguration<Integer, Integer> ccfg =
            new CacheConfiguration<Integer, Integer>()
                .setName("config")
                .setAtomicityMode(CacheAtomicityMode.ATOMIC)
                .setBackups(0) // No need for backup, just load from the store if needed
                .setCacheStoreFactory(new CacheStoreFactory())
                .setOnheapCacheEnabled(true)
                .setEvictionPolicy(new LruEvictionPolicy(100))
                .setNearConfiguration(new NearCacheConfiguration<Integer, Integer>()
                .setNearEvictionPolicy(new LruEvictionPolicy<Integer, Integer>()));

        ccfg.setExpiryPolicyFactory(CreatedExpiryPolicy.factoryOf(new Duration(TimeUnit.MINUTES, 1)))
            .setReadThrough(true)
            .setWriteThrough(false);

        cfg.setCacheConfiguration(ccfg);

        return cfg;
    }

    /** {@inheritDoc} */
    @Override protected void afterTest() throws Exception {
        stopAllGrids();
    }

    /**
     * @throws Exception if failed.
     */
    @Test
    public void testPartitionRent() throws Exception {
        startGrid(DATA_READ_GRID_IDX);

        final AtomicBoolean done = new AtomicBoolean();

        IgniteInternalFuture<Long> gridAndCacheAccessFut = GridTestUtils.runMultiThreadedAsync(new Callable<Integer>() {
            @Override public Integer call() throws Exception {
                final Set<Integer> keysSet = new LinkedHashSet<>();

                keysSet.add(1);
                keysSet.add(2);
                keysSet.add(FAILING_KEY);
                keysSet.add(4);
                keysSet.add(5);

                while (!done.get()) {
                    try {
                        grid(DATA_READ_GRID_IDX).<Integer, Integer>cache("config").getAll(keysSet);
                    }
                    catch (Throwable ignore) {
                        // No-op.
                    }
                    if (Thread.currentThread().isInterrupted())
                        throw new IgniteInterruptedCheckedException("Execution of [" + Thread.currentThread().getName() + "] Interrupted. Test is probably timed out");
                }

                return null;
            }
        }, 4, "loader");

        IgniteInternalFuture<Void> startFut = GridTestUtils.runAsync(new Callable<Void>() {
            @Override public Void call() throws Exception {
                for (int i = 1; i < 5; i++) {
                    startGrid(i);

                    awaitPartitionMapExchange();
                }

                return null;
            }
        });

        try {
            startFut.get();
        }
        catch (Exception e) {
            gridAndCacheAccessFut.cancel();
            U.error(log, e);
            throw e;
        }

        done.set(true);

        gridAndCacheAccessFut.get();
    }

    /**
     *
     */
    private static class CacheStoreFactory implements Factory<CacheStore<Integer, Integer>> {
        /** {@inheritDoc} */
        @Override public CacheStore<Integer, Integer> create() {
            return new HangingCacheStore();
        }
    }

    /**
     *
     */
    private static class HangingCacheStore extends CacheStoreAdapter<Integer, Integer> {
        /** {@inheritDoc} */
        @Override public Integer load(Integer key) throws CacheLoaderException {
            if (key == FAILING_KEY)
                throw new TestCacheLoaderExpectedException();

            return key;
        }

        /** {@inheritDoc} */
        @Override public void write(Cache.Entry<? extends Integer, ? extends Integer> entry) throws CacheWriterException {

        }

        /** {@inheritDoc} */
        @Override public void delete(Object key) throws CacheWriterException {

        }
    }

    /**
     *
     */
    private static class TestCacheLoaderExpectedException extends CacheLoaderException {
        /** {@inheritDoc} to reduce amount of logging, trace is not filled */
        @Override public synchronized Throwable fillInStackTrace() {
            return this;
        }
    }
}
