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

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import javax.cache.CacheException;
import javax.cache.event.CacheEntryEvent;
import javax.cache.event.CacheEntryUpdatedListener;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.IgniteTransactions;
import org.apache.ignite.cache.CacheAtomicityMode;
import org.apache.ignite.cache.affinity.Affinity;
import org.apache.ignite.cache.query.ContinuousQuery;
import org.apache.ignite.cache.query.QueryCursor;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.internal.IgniteInternalFuture;
import org.apache.ignite.internal.util.lang.GridAbsPredicate;
import org.apache.ignite.internal.util.typedef.T2;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.testframework.GridTestUtils;
import org.apache.ignite.testframework.GridTestUtils.SF;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.apache.ignite.transactions.Transaction;
import org.apache.ignite.transactions.TransactionSerializationException;
import org.junit.Test;

import static org.apache.ignite.cache.CacheAtomicityMode.ATOMIC;
import static org.apache.ignite.cache.CacheAtomicityMode.TRANSACTIONAL;
import static org.apache.ignite.cache.CacheAtomicityMode.TRANSACTIONAL_SNAPSHOT;
import static org.apache.ignite.cache.CacheWriteSynchronizationMode.FULL_SYNC;
import static org.apache.ignite.transactions.TransactionConcurrency.PESSIMISTIC;
import static org.apache.ignite.transactions.TransactionIsolation.REPEATABLE_READ;

/**
 *
 */
public class CacheContinuousQueryConcurrentPartitionUpdateTest extends GridCommonAbstractTest {
    /** {@inheritDoc} */
    @Override protected void afterTest() throws Exception {
        stopAllGrids();

        super.afterTest();
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testConcurrentUpdatePartitionAtomic() throws Exception {
        concurrentUpdatePartition(ATOMIC, false);
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testConcurrentUpdatePartitionTx() throws Exception {
        concurrentUpdatePartition(TRANSACTIONAL, false);
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testConcurrentUpdatePartitionMvccTx() throws Exception {
        concurrentUpdatePartition(TRANSACTIONAL_SNAPSHOT, false);
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testConcurrentUpdatePartitionAtomicCacheGroup() throws Exception {
        concurrentUpdatePartition(ATOMIC, true);
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testConcurrentUpdatePartitionTxCacheGroup() throws Exception {
        concurrentUpdatePartition(TRANSACTIONAL, true);
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testConcurrentUpdatePartitionMvccTxCacheGroup() throws Exception {
        concurrentUpdatePartition(TRANSACTIONAL_SNAPSHOT, true);
    }

    /**
     * @param atomicityMode Cache atomicity mode.
     * @param cacheGrp {@code True} if test cache multiple caches in the same group.
     * @throws Exception If failed.
     */
    private void concurrentUpdatePartition(CacheAtomicityMode atomicityMode, boolean cacheGrp) throws Exception {
        Ignite srv = startGrid(0);
        Ignite client = startClientGrid(1);

        List<AtomicInteger> cntrs = new ArrayList<>();
        List<String> caches = new ArrayList<>();

        if (cacheGrp) {
            for (int i = 0; i < 3; i++) {
                CacheConfiguration<Object, Object> ccfg = new CacheConfiguration<>(DEFAULT_CACHE_NAME + i);

                ccfg.setGroupName("testGroup");
                ccfg.setWriteSynchronizationMode(FULL_SYNC);
                ccfg.setAtomicityMode(atomicityMode);

                IgniteCache<Object, Object> cache = client.createCache(ccfg);

                caches.add(cache.getName());

                cntrs.add(startListener(cache).get1());
            }
        }
        else {
            CacheConfiguration<Object, Object> ccfg = new CacheConfiguration<>(DEFAULT_CACHE_NAME);

            ccfg.setWriteSynchronizationMode(FULL_SYNC);
            ccfg.setAtomicityMode(atomicityMode);

            IgniteCache<Object, Object> cache = client.createCache(ccfg);

            caches.add(cache.getName());

            cntrs.add(startListener(cache).get1());
        }

        Affinity<Integer> aff = srv.affinity(caches.get(0));

        final List<Integer> keys = new ArrayList<>();

        final int KEYS = 10;

        for (int i = 0; i < 100_000; i++) {
            if (aff.partition(i) == 0) {
                keys.add(i);

                if (keys.size() == KEYS)
                    break;
            }
        }

        assertEquals(KEYS, keys.size());

        final int THREADS = SF.applyLB(10, 4);
        final int UPDATES = SF.applyLB(1000, 100);

        final List<IgniteCache<Object, Object>> srvCaches = new ArrayList<>();

        for (String cacheName : caches)
            srvCaches.add(srv.cache(cacheName));

        for (int i = 0; i < SF.applyLB(15, 5); i++) {
            log.info("Iteration: " + i);

            GridTestUtils.runMultiThreaded(new Callable<Void>() {
                @Override public Void call() throws Exception {
                    ThreadLocalRandom rnd = ThreadLocalRandom.current();

                    for (int i = 0; i < UPDATES; i++) {
                        for (int c = 0; c < srvCaches.size(); c++) {
                            if (atomicityMode == ATOMIC)
                                srvCaches.get(c).put(keys.get(rnd.nextInt(KEYS)), i);
                            else {
                                IgniteCache<Object, Object> cache0 = srvCaches.get(c);
                                IgniteTransactions txs = cache0.unwrap(Ignite.class).transactions();

                                boolean committed = false;

                                while (!committed) {
                                    try (Transaction tx = txs.txStart(PESSIMISTIC, REPEATABLE_READ)) {
                                        cache0.put(keys.get(rnd.nextInt(KEYS)), i);

                                        tx.commit();

                                        committed = true;
                                    }
                                    catch (CacheException e) {
                                        assertTrue(e.getCause() instanceof TransactionSerializationException);
                                        assertEquals(atomicityMode, TRANSACTIONAL_SNAPSHOT);
                                    }
                                }
                            }
                        }
                    }

                    return null;
                }
            }, THREADS, "update");

            for (final AtomicInteger evtCnt : cntrs) {
                GridTestUtils.waitForCondition(new GridAbsPredicate() {
                    @Override public boolean apply() {
                        log.info("Events: " + evtCnt.get());

                        return evtCnt.get() >= THREADS * UPDATES;
                    }
                }, 5000);

                assertEquals(THREADS * UPDATES, evtCnt.get());

                evtCnt.set(0);
            }
        }
    }

    /**
     * @param cache Cache.
     * @return Event counter.
     */
    private T2<AtomicInteger, QueryCursor> startListener(IgniteCache<Object, Object> cache) {
        final AtomicInteger evtCnt = new AtomicInteger();

        ContinuousQuery<Object, Object> qry = new ContinuousQuery<>();

        qry.setLocalListener(new CacheEntryUpdatedListener<Object, Object>() {
            @Override public void onUpdated(Iterable<CacheEntryEvent<?, ?>> evts) {
                for (CacheEntryEvent evt : evts) {
                    assertNotNull(evt.getKey());
                    assertNotNull(evt.getValue());

                    if ((Integer)evt.getValue() >= 0)
                        evtCnt.incrementAndGet();
                }
            }
        });

        QueryCursor cur = cache.query(qry);

        return new T2<>(evtCnt, cur);
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testConcurrentUpdatesAndQueryStartAtomic() throws Exception {
        concurrentUpdatesAndQueryStart(ATOMIC, false);
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testConcurrentUpdatesAndQueryStartTx() throws Exception {
        concurrentUpdatesAndQueryStart(TRANSACTIONAL, false);
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testConcurrentUpdatesAndQueryStartMvccTx() throws Exception {
        concurrentUpdatesAndQueryStart(TRANSACTIONAL_SNAPSHOT, false);
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testConcurrentUpdatesAndQueryStartAtomicCacheGroup() throws Exception {
        concurrentUpdatesAndQueryStart(ATOMIC, true);
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testConcurrentUpdatesAndQueryStartTxCacheGroup() throws Exception {
        concurrentUpdatesAndQueryStart(TRANSACTIONAL, true);
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testConcurrentUpdatesAndQueryStartMvccTxCacheGroup() throws Exception {
        concurrentUpdatesAndQueryStart(TRANSACTIONAL_SNAPSHOT, true);
    }

    /**
     * @param atomicityMode Cache atomicity mode.
     * @param cacheGrp {@code True} if test cache multiple caches in the same group.
     * @throws Exception If failed.
     */
    private void concurrentUpdatesAndQueryStart(CacheAtomicityMode atomicityMode, boolean cacheGrp) throws Exception {
        Ignite srv = startGrid(0);
        Ignite client = startClientGrid(1);

        List<String> caches = new ArrayList<>();

        if (cacheGrp) {
            for (int i = 0; i < 3; i++) {
                CacheConfiguration<?, ?> ccfg = new CacheConfiguration<>(DEFAULT_CACHE_NAME + i);

                ccfg.setGroupName("testGroup");
                ccfg.setWriteSynchronizationMode(FULL_SYNC);
                ccfg.setAtomicityMode(atomicityMode);

                IgniteCache<?, ?> cache = client.createCache(ccfg);

                caches.add(cache.getName());
            }
        }
        else {
            CacheConfiguration<?, ?> ccfg = new CacheConfiguration<>(DEFAULT_CACHE_NAME);

            ccfg.setWriteSynchronizationMode(FULL_SYNC);
            ccfg.setAtomicityMode(atomicityMode);

            IgniteCache<?, ?> cache = client.createCache(ccfg);

            caches.add(cache.getName());
        }

        Affinity<Integer> aff = srv.affinity(caches.get(0));

        final List<Integer> keys = new ArrayList<>();

        final int KEYS = 10;

        for (int i = 0; i < 100_000; i++) {
            if (aff.partition(i) == 0) {
                keys.add(i);

                if (keys.size() == KEYS)
                    break;
            }
        }

        assertEquals(KEYS, keys.size());

        final int THREADS = 10;
        final int UPDATES = 1000;

        final List<IgniteCache<Object, Object>> srvCaches = new ArrayList<>();

        for (String cacheName : caches)
            srvCaches.add(srv.cache(cacheName));

        for (int i = 0; i < SF.applyLB(5, 2); i++) {
            log.info("Iteration: " + i);

            final AtomicBoolean stop = new AtomicBoolean();

            List<T2<AtomicInteger, QueryCursor>> qrys = new ArrayList<>();

            try {
                IgniteInternalFuture fut = GridTestUtils.runMultiThreadedAsync(new Callable<Void>() {
                    @Override public Void call() throws Exception {
                        ThreadLocalRandom rnd = ThreadLocalRandom.current();

                        while (!stop.get()) {
                            for (IgniteCache<Object, Object> srvCache : srvCaches) {
                                if (atomicityMode == ATOMIC)
                                    srvCache.put(keys.get(rnd.nextInt(KEYS)), rnd.nextInt(100) - 200);
                                else {
                                    IgniteTransactions txs = srvCache.unwrap(Ignite.class).transactions();

                                    boolean committed = false;

                                    while (!committed) {
                                        try (Transaction tx = txs.txStart(PESSIMISTIC, REPEATABLE_READ)) {
                                            srvCache.put(keys.get(rnd.nextInt(KEYS)), rnd.nextInt(100) - 200);

                                            tx.commit();

                                            committed = true;
                                        }
                                        catch (CacheException e) {
                                            assertTrue(e.getCause() instanceof TransactionSerializationException);
                                            assertEquals(atomicityMode, TRANSACTIONAL_SNAPSHOT);
                                        }
                                    }
                                }
                            }
                        }

                        return null;
                    }
                }, THREADS, "update");

                U.sleep(1000);

                for (String cache : caches)
                    for (int l = 0; l < 10; l++)
                        qrys.add(startListener(client.cache(cache)));

                U.sleep(1000);

                stop.set(true);

                fut.get();
            }
            finally {
                stop.set(true);
            }

            GridTestUtils.runMultiThreadedAsync(new Callable<Void>() {
                @Override public Void call() throws Exception {
                    ThreadLocalRandom rnd = ThreadLocalRandom.current();

                    for (int i = 0; i < UPDATES; i++) {
                        for (IgniteCache<Object, Object> srvCache : srvCaches) {
                            if (atomicityMode == ATOMIC)
                                srvCache.put(keys.get(rnd.nextInt(KEYS)), i);
                            else {
                                IgniteTransactions txs = srvCache.unwrap(Ignite.class).transactions();

                                boolean committed = false;

                                while (!committed) {
                                    try (Transaction tx = txs.txStart(PESSIMISTIC, REPEATABLE_READ)) {
                                        srvCache.put(keys.get(rnd.nextInt(KEYS)), i);

                                        tx.commit();

                                        committed = true;
                                    }
                                    catch (CacheException e) {
                                        assertTrue(e.getCause() instanceof TransactionSerializationException);
                                        assertEquals(atomicityMode, TRANSACTIONAL_SNAPSHOT);
                                    }
                                }
                            }
                        }
                    }

                    return null;
                }
            }, THREADS, "update");

            for (T2<AtomicInteger, QueryCursor> qry : qrys) {
                final AtomicInteger evtCnt = qry.get1();

                GridTestUtils.waitForCondition(new GridAbsPredicate() {
                    @Override public boolean apply() {
                        log.info("Events: " + evtCnt.get());

                        return evtCnt.get() >= THREADS * UPDATES;
                    }
                }, 30000);

                assertEquals(THREADS * UPDATES, evtCnt.get());

                qry.get2().close();
            }
        }
    }
}
