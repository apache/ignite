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

package org.apache.ignite.internal.processors.cache.query.continuous;

import java.util.Iterator;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicReference;
import javax.cache.Cache;
import javax.cache.event.CacheEntryEvent;
import javax.cache.event.CacheEntryUpdatedListener;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.cache.CacheMode;
import org.apache.ignite.cache.query.ContinuousQuery;
import org.apache.ignite.cache.query.QueryCursor;

import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static org.apache.ignite.cache.CacheMode.REPLICATED;

/**
 * Continuous queries tests for replicated cache.
 */
public class GridCacheContinuousQueryReplicatedSelfTest extends GridCacheContinuousQueryAbstractSelfTest {
    /** {@inheritDoc} */
    @Override protected CacheMode cacheMode() {
        return REPLICATED;
    }

    /** {@inheritDoc} */
    @Override protected int gridCount() {
        return 3;
    }

    /**
     * @throws Exception If failed.
     */
    public void testRemoteNodeCallback() throws Exception {
        IgniteCache<Integer, Integer> cache1 = grid(0).cache(null);
        IgniteCache<Integer, Integer> cache2 = grid(1).cache(null);

        ContinuousQuery<Integer, Integer> qry = new ContinuousQuery<>();

        final AtomicReference<Integer> val = new AtomicReference<>();
        final CountDownLatch latch = new CountDownLatch(1);

        qry.setLocalListener(new CacheEntryUpdatedListener<Integer, Integer>() {
            @Override public void onUpdated(Iterable<CacheEntryEvent<? extends Integer, ? extends Integer>> evts) {
                Iterator<CacheEntryEvent<? extends Integer, ? extends Integer>> it = evts.iterator();

                CacheEntryEvent<? extends Integer, ? extends Integer> e = it.next();

                assert !it.hasNext();

                log.info("Event: " + e);

                val.set(e.getValue());

                latch.countDown();
            }
        });

        try (QueryCursor<Cache.Entry<Integer, Integer>> ignored = cache2.query(qry)) {
            cache1.put(1, 10);

            latch.await(LATCH_TIMEOUT, MILLISECONDS);

            assertEquals(10, val.get().intValue());
        }
    }

    /**
     * Ensure that every node see every update.
     *
     * @throws Exception If failed.
     */
    public void testCrossCallback() throws Exception {
        // Prepare.
        IgniteCache<Integer, Integer> cache1 = grid(0).cache(null);
        IgniteCache<Integer, Integer> cache2 = grid(1).cache(null);

        final int key1 = primaryKey(cache1);
        final int key2 = primaryKey(cache2);

        final CountDownLatch latch1 = new CountDownLatch(2);
        final CountDownLatch latch2 = new CountDownLatch(2);

        ContinuousQuery<Integer, Integer> qry1 = new ContinuousQuery<>();

        qry1.setLocalListener(new CacheEntryUpdatedListener<Integer, Integer>() {
            @Override public void onUpdated(Iterable<CacheEntryEvent<? extends Integer, ? extends Integer>> evts) {
                for (CacheEntryEvent<? extends Integer, ? extends Integer> evt : evts) {
                    log.info("Update in cache 1: " + evt);

                    if (evt.getKey() == key1 || evt.getKey() == key2) latch1.countDown();
                }
            }
        });

        ContinuousQuery<Integer, Integer> qry2 = new ContinuousQuery<>();

        qry2.setLocalListener(new CacheEntryUpdatedListener<Integer, Integer>() {
            @Override public void onUpdated(Iterable<CacheEntryEvent<? extends Integer, ? extends Integer>> evts) {
                for (CacheEntryEvent<? extends Integer, ? extends Integer> evt : evts) {
                    log.info("Update in cache 2: " + evt);

                    if (evt.getKey() == key1 || evt.getKey() == key2)
                        latch2.countDown();
                }
            }
        });

        try (QueryCursor<Cache.Entry<Integer, Integer>> ignored = cache2.query(qry1);
             QueryCursor<Cache.Entry<Integer, Integer>> ignore = cache2.query(qry2)) {
            cache1.put(key1, key1);
            cache1.put(key2, key2);

            assert latch1.await(LATCH_TIMEOUT, MILLISECONDS);
            assert latch2.await(LATCH_TIMEOUT, MILLISECONDS);
        }
    }
}