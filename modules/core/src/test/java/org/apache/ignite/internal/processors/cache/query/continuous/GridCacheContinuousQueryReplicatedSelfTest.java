/*
 *                   GridGain Community Edition Licensing
 *                   Copyright 2019 GridGain Systems, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License") modified with Commons Clause
 * Restriction; you may not use this file except in compliance with the License. You may obtain a
 * copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the
 * License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied. See the License for the specific language governing permissions
 * and limitations under the License.
 *
 * Commons Clause Restriction
 *
 * The Software is provided to you by the Licensor under the License, as defined below, subject to
 * the following condition.
 *
 * Without limiting other conditions in the License, the grant of rights under the License will not
 * include, and the License does not grant to you, the right to Sell the Software.
 * For purposes of the foregoing, “Sell” means practicing any or all of the rights granted to you
 * under the License to provide to third parties, for a fee or other consideration (including without
 * limitation fees for hosting or consulting/ support services related to the Software), a product or
 * service whose value derives, entirely or substantially, from the functionality of the Software.
 * Any license notice or attribution required by the License must also include this Commons Clause
 * License Condition notice.
 *
 * For purposes of the clause above, the “Licensor” is Copyright 2019 GridGain Systems, Inc.,
 * the “License” is the Apache License, Version 2.0, and the Software is the GridGain Community
 * Edition software provided with this notice.
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
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static org.apache.ignite.cache.CacheMode.REPLICATED;

/**
 * Continuous queries tests for replicated cache.
 */
@RunWith(JUnit4.class)
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
    @Test
    public void testRemoteNodeCallback() throws Exception {
        IgniteCache<Integer, Integer> cache1 = grid(0).cache(DEFAULT_CACHE_NAME);
        IgniteCache<Integer, Integer> cache2 = grid(1).cache(DEFAULT_CACHE_NAME);

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
    @Test
    public void testCrossCallback() throws Exception {
        // Prepare.
        IgniteCache<Integer, Integer> cache1 = grid(0).cache(DEFAULT_CACHE_NAME);
        IgniteCache<Integer, Integer> cache2 = grid(1).cache(DEFAULT_CACHE_NAME);

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
