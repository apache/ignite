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

package org.apache.ignite.internal.processors.cache;

import java.util.Collection;
import java.util.Collections;
import java.util.concurrent.Callable;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.Lock;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.configuration.NearCacheConfiguration;
import org.apache.ignite.internal.IgniteInternalFuture;
import org.apache.ignite.internal.IgniteKernal;
import org.apache.ignite.internal.processors.affinity.AffinityTopologyVersion;
import org.apache.ignite.internal.util.typedef.CI1;
import org.apache.ignite.internal.util.typedef.F;
import org.apache.ignite.testframework.GridTestUtils;
import org.apache.ignite.transactions.Transaction;

import static org.apache.ignite.cache.CacheAtomicityMode.TRANSACTIONAL;
import static org.apache.ignite.cache.CacheMode.PARTITIONED;
import static org.apache.ignite.transactions.TransactionConcurrency.PESSIMISTIC;
import static org.apache.ignite.transactions.TransactionIsolation.REPEATABLE_READ;

/**
 * Abstract class for cache tests.
 */
public class GridCacheFinishPartitionsSelfTest extends GridCacheAbstractSelfTest {
    /** */
    private static final int GRID_CNT = 1;

    /** Grid kernal. */
    private IgniteKernal grid;

    /** {@inheritDoc} */
    @Override protected int gridCount() {
        return GRID_CNT;
    }

    /** {@inheritDoc} */
    @Override protected void beforeTest() throws Exception {
        grid = (IgniteKernal)grid(0);
    }

    /** {@inheritDoc} */
    @Override protected void afterTest() throws Exception {
        grid = null;
    }

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String gridName) throws Exception {
        IgniteConfiguration c = super.getConfiguration(gridName);

        CacheConfiguration cc = defaultCacheConfiguration();

        cc.setCacheMode(PARTITIONED);
        cc.setBackups(1);
        cc.setAtomicityMode(TRANSACTIONAL);
        cc.setNearConfiguration(new NearCacheConfiguration());

        c.setCacheConfiguration(cc);

        return c;
    }

    /**
     * @throws Exception If failed.
     */
    public void testTxFinishPartitions() throws Exception {
        String key = "key";
        String val = "value";

        IgniteCache<String, String> cache = grid.cache(null);

        int keyPart = grid.<String, String>internalCache().context().affinity().partition(key);

        cache.put(key, val);

        // Wait for tx-enlisted partition.
        long waitTime = runTransactions(key, keyPart, F.asList(keyPart));

        info("Wait time, ms: " + waitTime);

        // Wait for not enlisted partition.
        waitTime = runTransactions(key, keyPart, F.asList(keyPart + 1));

        info("Wait time, ms: " + waitTime);

        // Wait for both partitions.
        waitTime = runTransactions(key, keyPart, F.asList(keyPart, keyPart + 1));

        info("Wait time, ms: " + waitTime);
    }

    /**
     * @param key Key.
     * @param keyPart Key partition.
     * @param waitParts Partitions to wait.
     * @return Wait time.
     * @throws Exception If failed.
     */
    private long runTransactions(final String key, final int keyPart, final Collection<Integer> waitParts)
        throws Exception {
        int threadNum = 1;

        final CyclicBarrier barrier = new CyclicBarrier(threadNum);
        final CountDownLatch latch = new CountDownLatch(threadNum);

        final AtomicLong start = new AtomicLong();

        GridTestUtils.runMultiThreaded(new Callable() {
            @Override public Object call() throws Exception {
                if (barrier.await() == 0)
                    start.set(System.currentTimeMillis());

                IgniteCache<String, String> cache = grid(0).cache(null);

                Transaction tx = grid(0).transactions().txStart(PESSIMISTIC, REPEATABLE_READ);

                cache.get(key);

                IgniteInternalFuture<?> fut = grid.context().cache().context().partitionReleaseFuture(
                    new AffinityTopologyVersion(GRID_CNT + 1));

                fut.listen(new CI1<IgniteInternalFuture<?>>() {
                    @Override public void apply(IgniteInternalFuture<?> e) {
                        latch.countDown();
                    }
                });

                assert !fut.isDone() : "Failed waiting for locks " +
                    "[keyPart=" + keyPart + ", waitParts=" + waitParts + ", done=" + fut.isDone() + ']';

                tx.commit();

                return null;
            }
        }, threadNum, "test-finish-partitions-thread");

        latch.await();

        return System.currentTimeMillis() - start.get();
    }

    /**
     * Tests method {@link GridCacheMvccManager#finishLocks(org.apache.ignite.lang.IgnitePredicate,
     * AffinityTopologyVersion)}.
     *
     * @throws Exception If failed.
     */
    public void testMvccFinishPartitions() throws Exception {
        String key = "key";

        int keyPart = grid.internalCache().context().affinity().partition(key);

        // Wait for tx-enlisted partition.
        long waitTime = runLock(key, keyPart, F.asList(keyPart));

        info("Wait time, ms: " + waitTime);

        // Wait for not enlisted partition.
        waitTime = runLock(key, keyPart, F.asList(keyPart + 1));

        info("Wait time, ms: " + waitTime);

        // Wait for both partitions.
        waitTime = runLock(key, keyPart, F.asList(keyPart, keyPart + 1));

        info("Wait time, ms: " + waitTime);
    }

    /**
     * Tests finish future for particular set of keys.
     *
     * @throws Exception If failed.
     */
    public void testMvccFinishKeys() throws Exception {
        IgniteCache<String, Integer> cache = grid(0).cache(null);

        try (Transaction tx = grid(0).transactions().txStart(PESSIMISTIC, REPEATABLE_READ)) {
            final String key = "key";

            cache.get(key);

            GridCacheAdapter<String, Integer> internal = grid.internalCache();

            KeyCacheObject cacheKey = internal.context().toCacheKeyObject(key);

            IgniteInternalFuture<?> nearFut = internal.context().mvcc().finishKeys(Collections.singletonList(cacheKey),
                new AffinityTopologyVersion(2));

            IgniteInternalFuture<?> dhtFut = internal.context().near().dht().context().mvcc().finishKeys(
                Collections.singletonList(cacheKey), new AffinityTopologyVersion(2));

            assert !nearFut.isDone();
            assert !dhtFut.isDone();

            tx.commit();
        }
    }

    /**
     * Tests chained locks and partitions release future.
     *
     * @throws Exception If failed.
     */
    public void testMvccFinishPartitionsContinuousLockAcquireRelease() throws Exception {
        int key = 1;

        GridCacheSharedContext<Object, Object> ctx = grid.context().cache().context();

        final AtomicLong end = new AtomicLong(0);

        final CountDownLatch latch = new CountDownLatch(1);

        IgniteCache<Integer, String> cache = grid.cache(null);

        Lock lock = cache.lock(key);

        lock.lock();

        long start = System.currentTimeMillis();

        info("Start time: " + start);

        IgniteInternalFuture<?> fut = ctx.partitionReleaseFuture(new AffinityTopologyVersion(GRID_CNT + 1));

        assert fut != null;

        fut.listen(new CI1<IgniteInternalFuture<?>>() {
            @Override public void apply(IgniteInternalFuture<?> e) {
                end.set(System.currentTimeMillis());

                latch.countDown();

                info("End time: " + end.get());
            }
        });

        Lock lock1 = cache.lock(key + 1);

        lock1.lock();

        lock.unlock();

        Lock lock2 = cache.lock(key + 2);

        lock2.lock();

        lock1.unlock();

        assert !fut.isDone() : "Failed waiting for locks";

        lock2.unlock();

        latch.await();
    }

    /**
     * @param key Key.
     * @param keyPart Key partition.
     * @param waitParts Partitions to wait.
     * @return Wait time.
     * @throws Exception If failed.
     */
    private long runLock(String key, int keyPart, Collection<Integer> waitParts) throws Exception {

        GridCacheSharedContext<Object, Object> ctx = grid.context().cache().context();

        final AtomicLong end = new AtomicLong(0);

        final CountDownLatch latch = new CountDownLatch(1);

        IgniteCache<String, String> cache = grid.cache(null);

        Lock lock = cache.lock(key);

        lock.lock();

        long start;
        try {
            start = System.currentTimeMillis();

            info("Start time: " + start);

            IgniteInternalFuture<?> fut = ctx.partitionReleaseFuture(new AffinityTopologyVersion(GRID_CNT + 1));

            assert fut != null;

            fut.listen(new CI1<IgniteInternalFuture<?>>() {
                @Override public void apply(IgniteInternalFuture<?> e) {
                    end.set(System.currentTimeMillis());

                    latch.countDown();

                    info("End time: " + end.get());
                }
            });

            assert !fut.isDone() : "Failed waiting for locks [keyPart=" + keyPart + ", waitParts=" + waitParts + ", done="
                + fut.isDone() + ']';
        }
        finally {
            lock.unlock();
        }

        latch.await();

        return end.get() - start;
    }
}