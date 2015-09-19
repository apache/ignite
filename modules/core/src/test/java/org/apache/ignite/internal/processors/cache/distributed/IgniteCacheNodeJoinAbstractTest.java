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

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.Callable;
import java.util.concurrent.atomic.AtomicBoolean;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.cache.CacheMode;
import org.apache.ignite.cache.query.ScanQuery;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.internal.IgniteInternalFuture;
import org.apache.ignite.internal.processors.cache.IgniteCacheAbstractTest;
import org.apache.ignite.testframework.GridTestUtils;

import static org.apache.ignite.cache.CacheMode.PARTITIONED;

/**
 *
 */
public abstract class IgniteCacheNodeJoinAbstractTest extends IgniteCacheAbstractTest {
    /** {@inheritDoc} */
    @Override protected CacheMode cacheMode() {
        return PARTITIONED;
    }

    /** {@inheritDoc} */
    @Override protected int gridCount() {
        return 1;
    }

    /** {@inheritDoc} */
    @Override protected CacheConfiguration cacheConfiguration(String gridName) throws Exception {
        CacheConfiguration cfg = super.cacheConfiguration(gridName);

        cfg.setReadFromBackup(false); // Force remote 'get'.

        return cfg;
    }

    /**
     * @throws Exception If failed.
     */
    public void testGet() throws Exception {
        final IgniteCache<Integer, Integer> cache = jcache(0);

        final int KEYS = 1000;

        Map<Integer, Integer> map = new HashMap<>();

        for (int i = 0; i < KEYS; i++)
            map.put(i, i);

        for (int i = 0; i < 10; i++) {
            log.info("Iteration: " + i);

            cache.putAll(map);

            final IgniteInternalFuture fut = GridTestUtils.runAsync(new Callable<Void>() {
                @Override public Void call() throws Exception {
                    startGrid(1);

                    return null;
                }
            });

            final AtomicBoolean stop = new AtomicBoolean();

            GridTestUtils.runMultiThreaded(new Callable<Void>() {
                @Override public Void call() throws Exception {
                    while (!stop.get() && !fut.isDone()) {
                        for (int key = 0; key < KEYS; key++) {
                            assertNotNull(cache.get(key));

                            if (key % 100 == 0 && fut.isDone())
                                break;
                        }
                    }

                    return null;
                }
            }, 10, "test-get");

            try {
                fut.get(60_000);
            }
            finally {
                stop.set(true);
            }

            stopGrid(1);
        }
    }

    /**
     * @throws Exception If failed.
     */
    public void testScanQuery() throws Exception {
        final IgniteCache<Integer, Integer> cache = jcache(0);

        for (int i = 0; i < 5; i++) {
            log.info("Iteration: " + i);

            final IgniteInternalFuture fut = GridTestUtils.runAsync(new Callable<Void>() {
                @Override public Void call() throws Exception {
                    startGrid(1);

                    return null;
                }
            });

            final AtomicBoolean stop = new AtomicBoolean();

            GridTestUtils.runMultiThreaded(new Callable<Void>() {
                @Override public Void call() throws Exception {
                    ScanQuery qry = new ScanQuery();

                    while (!stop.get() && !fut.isDone())
                        cache.query(qry).getAll();

                    return null;
                }
            }, 10, "test-qry");

            try {
                fut.get(60_000);
            }
            finally {
                stop.set(true);
            }

            stopGrid(1);
        }
    }
}