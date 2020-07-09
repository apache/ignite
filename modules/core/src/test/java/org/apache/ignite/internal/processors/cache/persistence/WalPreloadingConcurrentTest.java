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

package org.apache.ignite.internal.processors.cache.persistence;

import java.util.Collections;
import java.util.concurrent.Callable;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.atomic.AtomicBoolean;
import org.apache.ignite.cache.CacheAtomicityMode;
import org.apache.ignite.cache.CacheMode;
import org.apache.ignite.cache.CacheRebalanceMode;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.DataRegionConfiguration;
import org.apache.ignite.configuration.DataStorageConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.configuration.WALMode;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.internal.IgniteInternalFuture;
import org.apache.ignite.internal.processors.cache.IgniteInternalCache;
import org.apache.ignite.internal.util.typedef.T2;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.junit.Test;

/**
 * Tests if preloading operations for WAL work correctly for concurrent invocations.
 */
public class WalPreloadingConcurrentTest extends GridCommonAbstractTest {
    /** Threads number */
    private static final int THREADS = 8;

    /**
     * {@inheritDoc}
     */
    @Override protected IgniteConfiguration getConfiguration(String gridName) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(gridName);

        cfg.setConsistentId(gridName);

        CacheConfiguration<Object, Object> ccfg = new CacheConfiguration<>(DEFAULT_CACHE_NAME)
            .setAtomicityMode(CacheAtomicityMode.TRANSACTIONAL)
            .setRebalanceMode(CacheRebalanceMode.ASYNC)
            .setCacheMode(CacheMode.REPLICATED);

        cfg.setCacheConfiguration(ccfg);

        DataStorageConfiguration dbCfg = new DataStorageConfiguration()
            .setWalMode(WALMode.LOG_ONLY)
            .setDefaultDataRegionConfiguration(
                new DataRegionConfiguration()
                    .setPersistenceEnabled(true)
            );

        cfg.setDataStorageConfiguration(dbCfg);

        return cfg;
    }

    /**
     * {@inheritDoc}
     */
    @Override protected void beforeTest() throws Exception {
        stopAllGrids();
        cleanPersistenceDir();
    }

    /**
     * {@inheritDoc}
     */
    @Override protected void afterTest() throws Exception {
        stopAllGrids();
        cleanPersistenceDir();
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public final void testConcurrentReserveReleaseHistoryForPreloading() throws Exception {
        IgniteEx ig = startGrid(0);

        final int entryCnt = 3200;

        ig.cluster().active(true);

        IgniteInternalCache<Object, Object> cache = ig.cachex(DEFAULT_CACHE_NAME);

        for (int k = 0; k < entryCnt; k++)
            cache.put(k, k);

        forceCheckpoint();

        GridCacheDatabaseSharedManager db = (GridCacheDatabaseSharedManager)ig.context().cache().context().database();

        AtomicBoolean stop = new AtomicBoolean(false);

        //Millis
        long duration = 5000L;

        IgniteInternalFuture fut1 = multithreadedAsync(new Callable<Object>() {
            @Override public Object call() {
                int randomPart = ThreadLocalRandom.current().nextInt(cache.configuration().getAffinity().partitions());

                while (!stop.get()) {
                    db.reserveHistoryForPreloading(Collections.singletonMap(
                        new T2<Integer, Integer>(cache.context().groupId(), randomPart),
                        0L
                    ));
                }

                return null;
            }
        }, THREADS, "reserve-history-thread");

        IgniteInternalFuture fut2 = multithreadedAsync(new Callable<Object>() {
            @Override public Object call() {
                while (!stop.get())
                    db.releaseHistoryForPreloading();

                return null;
            }
        }, THREADS, "release-history-thread");

        Thread.sleep(duration);

        stop.set(true);

        fut1.get();

        fut2.get();
    }
}
