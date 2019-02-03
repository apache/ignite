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

import java.util.ArrayList;
import java.util.Collection;
import java.util.concurrent.Callable;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.atomic.AtomicReferenceArray;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.cache.CacheAtomicityMode;
import org.apache.ignite.cache.CacheMode;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.IgniteInternalFuture;
import org.apache.ignite.internal.binary.BinaryMarshaller;
import org.apache.ignite.testframework.GridTestUtils;
import org.apache.ignite.testframework.MvccFeatureChecker;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.junit.Test;

/**
 *
 */
public class IgniteCacheCreatePutMultiNodeSelfTest extends GridCommonAbstractTest {
    /** Grid count. */
    private static final int GRID_CNT = 4;

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String igniteInstanceName) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(igniteInstanceName);

        cfg.setMarshaller(new BinaryMarshaller());

        return cfg;
    }

    /** {@inheritDoc} */
    @Override protected long getTestTimeout() {
        return 6 * 60 * 1000L;
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testStartNodes() throws Exception {
        try {
            Collection<IgniteInternalFuture<?>> futs = new ArrayList<>(GRID_CNT);
            int scale = 3;

            final CyclicBarrier barrier = new CyclicBarrier(GRID_CNT * scale);
            final AtomicReferenceArray<Exception> err = new AtomicReferenceArray<>(GRID_CNT * scale);

            for (int i = 0; i < GRID_CNT * scale; i++) {
                if (i < GRID_CNT)
                    startGrid(i);

                final int idx = i;

                IgniteInternalFuture<Void> fut = GridTestUtils.runAsync(new Callable<Void>() {
                    @Override public Void call() throws Exception {
                        Ignite ignite = ignite(idx % GRID_CNT);

                        try {
                            for (int k = 0; k < 50; k++) {
                                barrier.await();

                                String cacheName = "cache-" + k;

                                IgniteCache<Integer, Integer> cache = getCache(ignite, cacheName);

                                for (int i = 0; i < 100; i++) {
                                    while (true) {
                                        try {
                                            cache.getAndPut(i, i);

                                            break;
                                        }
                                        catch (Exception e) {
                                            MvccFeatureChecker.assertMvccWriteConflict(e);
                                        }
                                    }
                                }

                                barrier.await();

                                ignite.destroyCache(cacheName);
                            }
                        }
                        catch (Exception e) {
                            err.set(idx, e);
                        }

                        return null;
                    }
                });

                futs.add(fut);
            }

            for (IgniteInternalFuture<?> fut : futs)
                fut.get(getTestTimeout());

            info("Errors: " + err);

            for (int i = 0; i < err.length(); i++) {
                Exception ex = err.get(i);

                if (ex != null)
                    throw ex;
            }
        }
        finally {
            stopAllGrids();
        }
    }

    /**
     * @param grid Grid.
     * @param cacheName Cache name.
     * @return Cache.
     */
    private IgniteCache<Integer, Integer> getCache(Ignite grid, String cacheName) {
        CacheConfiguration<Integer, Integer> ccfg = new CacheConfiguration<>(cacheName);

        ccfg.setCacheMode(CacheMode.PARTITIONED);
        ccfg.setAtomicityMode(CacheAtomicityMode.TRANSACTIONAL);
        ccfg.setBackups(1);
        ccfg.setNearConfiguration(null);

        return grid.getOrCreateCache(ccfg);
    }
}
