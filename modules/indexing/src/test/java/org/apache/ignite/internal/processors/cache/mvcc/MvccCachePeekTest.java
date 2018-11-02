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

package org.apache.ignite.internal.processors.cache.mvcc;

import java.util.concurrent.CountDownLatch;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.cache.CacheMode;
import org.apache.ignite.cache.CachePeekMode;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.internal.IgniteInternalFuture;
import org.apache.ignite.testframework.GridTestUtils;
import org.apache.ignite.transactions.Transaction;

import static org.apache.ignite.cache.CacheAtomicityMode.TRANSACTIONAL_SNAPSHOT;
import static org.apache.ignite.transactions.TransactionConcurrency.PESSIMISTIC;
import static org.apache.ignite.transactions.TransactionIsolation.REPEATABLE_READ;

public class MvccCachePeekTest extends CacheMvccAbstractTest {
    private IgniteCache<Object, Object> cache;

    @Override protected CacheMode cacheMode() {
        return CacheMode.PARTITIONED;
    }

    @Override protected void beforeTest() throws Exception {
        super.beforeTest();

        startGrid(0);

        cache = grid(0).getOrCreateCache(new CacheConfiguration<>(DEFAULT_CACHE_NAME)
            .setAtomicityMode(TRANSACTIONAL_SNAPSHOT)
            .setCacheMode(cacheMode()));
    }

    @Override protected void afterTest() throws Exception {
        cache.destroy();

        super.afterTest();
    }

    /**
     * @throws Exception if failed.
     */
    public void testPeek() throws Exception {
        assertNull(cache.localPeek(1));

        cache.put(1, 1);

        assertEquals(1, cache.localPeek(1));

        cache.put(1, 2);

        assertEquals(2, cache.localPeek(1));
    }

    /**
     * @throws Exception if failed.
     */
    public void testPeekDoesNotSeeAbortedVersions() throws Exception {
        cache.put(1, 1);

        try (Transaction tx = grid(0).transactions().txStart(PESSIMISTIC, REPEATABLE_READ)) {
            cache.put(1, 2);

            tx.rollback();
        }

        assertEquals(1, cache.localPeek(1));
    }

    /**
     * @throws Exception if failed.
     */
    public void testPeekDoesNotSeeActiveVersions() throws Exception {
        cache.put(1, 1);

        CountDownLatch writeCompleted = new CountDownLatch(1);
        CountDownLatch checkCompleted = new CountDownLatch(1);

        IgniteInternalFuture<Object> fut = GridTestUtils.runAsync(() -> {
            try (Transaction tx = grid(0).transactions().txStart(PESSIMISTIC, REPEATABLE_READ)) {
                cache.put(1, 2);

                writeCompleted.countDown();
                checkCompleted.await();

                tx.commit();
            }

            return null;
        });

        writeCompleted.await();

        assertEquals(1, cache.localPeek(1));

        checkCompleted.countDown();

        fut.get();
    }

    /**
     * @throws Exception if failed.
     */
    public void testPeekOnheap() throws Exception {
        cache.put(1, 1);

        assertNull(cache.localPeek(1, CachePeekMode.ONHEAP));
    }
}
