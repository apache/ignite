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

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.cache.query.SqlFieldsQuery;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.configuration.TransactionConfiguration;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.internal.IgniteInternalFuture;
import org.apache.ignite.internal.TestRecordingCommunicationSpi;
import org.apache.ignite.internal.processors.cache.distributed.near.GridNearTxLocal;
import org.apache.ignite.internal.processors.cache.transactions.TransactionProxyImpl;
import org.apache.ignite.internal.transactions.IgniteTxRollbackCheckedException;
import org.apache.ignite.internal.util.typedef.X;
import org.apache.ignite.testframework.GridTestUtils;
import org.apache.ignite.testframework.GridTestUtils.SF;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.apache.ignite.transactions.Transaction;
import org.apache.ignite.transactions.TransactionRollbackException;
import org.junit.After;
import org.junit.Test;

import static org.apache.ignite.cache.CacheAtomicityMode.TRANSACTIONAL_SNAPSHOT;
import static org.apache.ignite.transactions.TransactionConcurrency.PESSIMISTIC;
import static org.apache.ignite.transactions.TransactionIsolation.REPEATABLE_READ;

/** */
public class MvccDeadlockDetectionTest extends GridCommonAbstractTest {
    /** */
    private IgniteEx client;

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String igniteInstanceName) throws Exception {
        return super.getConfiguration(igniteInstanceName)
            .setCommunicationSpi(new TestRecordingCommunicationSpi())
            .setTransactionConfiguration(new TransactionConfiguration().setDeadlockTimeout(1));
    }

    /** */
    private void setUpGrids(int n, boolean indexed) throws Exception {
        Ignite ign = startGridsMultiThreaded(n);

        CacheConfiguration<Object, Object> ccfg = new CacheConfiguration<>(DEFAULT_CACHE_NAME)
            .setAtomicityMode(TRANSACTIONAL_SNAPSHOT);

        if (indexed)
            ccfg.setIndexedTypes(Integer.class, Integer.class);

        ign.getOrCreateCache(ccfg);

        client = startClientGrid(n);
    }

    /**
     * @throws Exception If failed.
     */
    @After
    public void cleanupTest() throws Exception {
        stopAllGrids();
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void detectSimpleDeadlock() throws Exception {
        setUpGrids(2, false);

        Integer key0 = primaryKey(grid(0).cache(DEFAULT_CACHE_NAME));
        Integer key1 = primaryKey(grid(1).cache(DEFAULT_CACHE_NAME));

        IgniteCache<Object, Object> cache = client.cache(DEFAULT_CACHE_NAME);

        assert client.configuration().isClientMode();

        CyclicBarrier b = new CyclicBarrier(2);

        IgniteInternalFuture<Object> fut0 = GridTestUtils.runAsync(() -> {
            try (Transaction tx = client.transactions().txStart(PESSIMISTIC, REPEATABLE_READ)) {
                cache.put(key0, 0);
                b.await();
                cache.put(key1, 1);

                tx.commit();
            }

            return null;
        });

        IgniteInternalFuture<Object> fut1 = GridTestUtils.runAsync(() -> {
            try (Transaction tx = client.transactions().txStart(PESSIMISTIC, REPEATABLE_READ)) {
                cache.put(key1, 1);
                b.await();
                cache.put(key0, 0);

                tx.commit();
            }

            return null;
        });

        assertExactlyOneAbortedDueDeadlock(fut0, fut1);
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void detectSimpleDeadlockFastUpdate() throws Exception {
        setUpGrids(2, true);

        IgniteCache<Object, Object> cache = client.cache(DEFAULT_CACHE_NAME);

        Integer key0 = primaryKey(grid(0).cache(DEFAULT_CACHE_NAME));
        Integer key1 = primaryKey(grid(1).cache(DEFAULT_CACHE_NAME));

        cache.query(new SqlFieldsQuery("insert into Integer(_key, _val) values(?, ?)").setArgs(key0, -1));
        cache.query(new SqlFieldsQuery("insert into Integer(_key, _val) values(?, ?)").setArgs(key1, -1));

        assert client.configuration().isClientMode();

        CyclicBarrier b = new CyclicBarrier(2);

        IgniteInternalFuture<Object> fut0 = GridTestUtils.runAsync(() -> {
            try (Transaction tx = client.transactions().txStart(PESSIMISTIC, REPEATABLE_READ)) {
                cache.query(new SqlFieldsQuery("update Integer set _val = 0 where _key = ?").setArgs(key0));
                b.await();
                cache.query(new SqlFieldsQuery("update Integer set _val = 0 where _key = ?").setArgs(key1));

                tx.commit();
            }

            return null;
        });

        IgniteInternalFuture<Object> fut1 = GridTestUtils.runAsync(() -> {
            try (Transaction tx = client.transactions().txStart(PESSIMISTIC, REPEATABLE_READ)) {
                cache.query(new SqlFieldsQuery("update Integer set _val = 1 where _key = ?").setArgs(key1));
                b.await();
                cache.query(new SqlFieldsQuery("update Integer set _val = 1 where _key = ?").setArgs(key0));

                tx.commit();
            }

            return null;
        });

        assertExactlyOneAbortedDueDeadlock(fut0, fut1);
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void detect3Deadlock() throws Exception {
        setUpGrids(3, false);

        Integer key0 = primaryKey(grid(0).cache(DEFAULT_CACHE_NAME));
        Integer key1 = primaryKey(grid(1).cache(DEFAULT_CACHE_NAME));
        Integer key2 = primaryKey(grid(2).cache(DEFAULT_CACHE_NAME));

        IgniteCache<Object, Object> cache = client.cache(DEFAULT_CACHE_NAME);

        assert client.configuration().isClientMode();

        CyclicBarrier b = new CyclicBarrier(3);

        IgniteInternalFuture<Object> fut0 = GridTestUtils.runAsync(() -> {
            try (Transaction tx = client.transactions().txStart(PESSIMISTIC, REPEATABLE_READ)) {
                cache.put(key0, 0);
                b.await();
                cache.put(key1, 1);

                tx.rollback();
            }

            return null;
        });

        IgniteInternalFuture<Object> fut1 = GridTestUtils.runAsync(() -> {
            try (Transaction tx = client.transactions().txStart(PESSIMISTIC, REPEATABLE_READ)) {
                cache.put(key1, 0);
                b.await();
                cache.put(key2, 1);

                tx.rollback();
            }

            return null;
        });

        IgniteInternalFuture<Object> fut2 = GridTestUtils.runAsync(() -> {
            try (Transaction tx = client.transactions().txStart(PESSIMISTIC, REPEATABLE_READ)) {
                cache.put(key2, 1);
                b.await();
                cache.put(key0, 0);

                tx.rollback();
            }

            return null;
        });

        assertExactlyOneAbortedDueDeadlock(fut0, fut1, fut2);
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void detectMultipleLockWaitDeadlock() throws Exception {
        // T0 -> T1
        //  \-> T2 -> T0
        setUpGrids(3, true);

        IgniteCache<Object, Object> cache = client.cache(DEFAULT_CACHE_NAME);

        Integer key0 = primaryKey(grid(0).cache(DEFAULT_CACHE_NAME));
        Integer key1 = primaryKey(grid(1).cache(DEFAULT_CACHE_NAME));
        Integer key2 = primaryKey(grid(2).cache(DEFAULT_CACHE_NAME));

        cache.query(new SqlFieldsQuery("insert into Integer(_key, _val) values(?, ?)").setArgs(key0, -1));
        cache.query(new SqlFieldsQuery("insert into Integer(_key, _val) values(?, ?)").setArgs(key1, -1));
        cache.query(new SqlFieldsQuery("insert into Integer(_key, _val) values(?, ?)").setArgs(key2, -1));

        CyclicBarrier b = new CyclicBarrier(3);

        IgniteInternalFuture<Object> fut2 = GridTestUtils.runAsync(() -> {
            try (Transaction tx = client.transactions().txStart(PESSIMISTIC, REPEATABLE_READ)) {
                cache.query(new SqlFieldsQuery("update Integer set _val = 2 where _key = ?").setArgs(key2));
                b.await();
                cache.query(new SqlFieldsQuery("update Integer set _val = 2 where _key = ?").setArgs(key0));

                // rollback to prevent waiting tx abort due write conflict
                tx.rollback();
            }

            return null;
        });

        IgniteInternalFuture<Object> fut1 = GridTestUtils.runAsync(() -> {
            try (Transaction tx = client.transactions().txStart(PESSIMISTIC, REPEATABLE_READ)) {
                cache.query(new SqlFieldsQuery("update Integer set _val = 1 where _key = ?").setArgs(key1));
                b.await();
                GridTestUtils.waitForCondition(fut2::isDone, 1000);

                // rollback to prevent waiting tx abort due write conflict
                tx.rollback();
            }

            return null;
        });

        IgniteInternalFuture<Object> fut0 = GridTestUtils.runAsync(() -> {
            try (Transaction tx = client.transactions().txStart(PESSIMISTIC, REPEATABLE_READ)) {
                cache.query(new SqlFieldsQuery("update Integer set _val = 0 where _key = ?").setArgs(key0));
                b.await();
                cache.query(
                    new SqlFieldsQuery("update Integer set _val = 0 where _key = ? or _key = ?").setArgs(key2, key1));

                tx.commit();
            }

            return null;
        });

        fut1.get(10, TimeUnit.SECONDS);

        assertExactlyOneAbortedDueDeadlock(fut0, fut2);
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void detectDeadlockLocalEntriesEnlistFuture() throws Exception {
        setUpGrids(1, false);

        List<Integer> keys = primaryKeys(grid(0).cache(DEFAULT_CACHE_NAME), 2);

        IgniteCache<Object, Object> cache = client.cache(DEFAULT_CACHE_NAME);

        assert client.configuration().isClientMode();

        CyclicBarrier b = new CyclicBarrier(2);

        IgniteInternalFuture<Object> fut0 = GridTestUtils.runAsync(() -> {
            try (Transaction tx = client.transactions().txStart(PESSIMISTIC, REPEATABLE_READ)) {
                cache.put(keys.get(0), 11);
                b.await();
                cache.put(keys.get(1), 11);

                tx.commit();
            }

            return null;
        });

        IgniteInternalFuture<Object> fut1 = GridTestUtils.runAsync(() -> {
            try (Transaction tx = client.transactions().txStart(PESSIMISTIC, REPEATABLE_READ)) {
                cache.put(keys.get(1), 22);
                b.await();
                cache.put(keys.get(0), 22);

                tx.commit();
            }

            return null;
        });

        assertExactlyOneAbortedDueDeadlock(fut0, fut1);
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void detectDeadlockLocalPrimary() throws Exception {
        // Checks that case when near tx does local on enlist on the same node and no dht tx is created

        setUpGrids(2, false);

        IgniteCache<Object, Object> cache0 = grid(0).cache(DEFAULT_CACHE_NAME);
        IgniteCache<Object, Object> cache1 = grid(1).cache(DEFAULT_CACHE_NAME);

        int key0 = primaryKey(cache0);
        int key1 = primaryKey(cache1);

        CyclicBarrier b = new CyclicBarrier(2);

        IgniteInternalFuture<Object> fut0 = GridTestUtils.runAsync(() -> {
            try (Transaction tx = grid(0).transactions().txStart(PESSIMISTIC, REPEATABLE_READ)) {
                cache0.put(key1, 11);
                b.await();
                cache0.put(key0, 11);

                tx.commit();
            }

            return null;
        });

        IgniteInternalFuture<Object> fut1 = GridTestUtils.runAsync(() -> {
            try (Transaction tx = grid(1).transactions().txStart(PESSIMISTIC, REPEATABLE_READ)) {
                cache1.put(key0, 22);
                b.await();
                cache1.put(key1, 22);

                tx.commit();
            }

            return null;
        });

        assertExactlyOneAbortedDueDeadlock(fut0, fut1);
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void detectDeadlockLocalQueryEnlistFuture() throws Exception {
        setUpGrids(1, true);

        List<Integer> keys = primaryKeys(grid(0).cache(DEFAULT_CACHE_NAME), 2);

        Collections.sort(keys);

        Integer key0 = keys.get(0), key1 = keys.get(1);

        IgniteCache<Object, Object> cache = client.cache(DEFAULT_CACHE_NAME);

        assert client.configuration().isClientMode();

        cache.query(new SqlFieldsQuery("insert into Integer(_key, _val) values(?, ?)").setArgs(key0, -1));
        cache.query(new SqlFieldsQuery("insert into Integer(_key, _val) values(?, ?)").setArgs(key1, -1));

        CyclicBarrier b = new CyclicBarrier(2);

        IgniteInternalFuture<Object> fut0 = GridTestUtils.runAsync(() -> {
            try (Transaction tx = client.transactions().txStart(PESSIMISTIC, REPEATABLE_READ)) {
                cache.query(new SqlFieldsQuery("update Integer set _val = 0 where _key <= ?").setArgs(key0));
                b.await();
                cache.query(new SqlFieldsQuery("update Integer set _val = 0 where _key >= ?").setArgs(key1));
                TimeUnit.SECONDS.sleep(2);

                tx.commit();
            }

            return null;
        });

        IgniteInternalFuture<Object> fut1 = GridTestUtils.runAsync(() -> {
            try (Transaction tx = client.transactions().txStart(PESSIMISTIC, REPEATABLE_READ)) {
                cache.query(new SqlFieldsQuery("update Integer set _val = 1 where _key >= ?").setArgs(key1));
                b.await();
                cache.query(new SqlFieldsQuery("update Integer set _val = 1 where _key <= ?").setArgs(key0));

                tx.commit();
            }

            return null;
        });

        assertExactlyOneAbortedDueDeadlock(fut0, fut1);
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void nonDeadlockedTxDetectsDeadlock1() throws Exception {
        setUpGrids(2, false);

        Integer key0 = primaryKey(grid(0).cache(DEFAULT_CACHE_NAME));
        Integer key1 = primaryKey(grid(1).cache(DEFAULT_CACHE_NAME));

        IgniteCache<Object, Object> cache = client.cache(DEFAULT_CACHE_NAME);

        assert client.configuration().isClientMode();

        CyclicBarrier b = new CyclicBarrier(3);

        IgniteInternalFuture<Object> fut0 = GridTestUtils.runAsync(() -> {
            try (Transaction tx = client.transactions().txStart(PESSIMISTIC, REPEATABLE_READ)) {
                blockProbe(grid(1), tx);

                cache.put(key0, 0);
                b.await();
                cache.put(key1, 1);

                tx.rollback();
            }

            return null;
        });

        IgniteInternalFuture<Object> fut1 = GridTestUtils.runAsync(() -> {
            try (Transaction tx = client.transactions().txStart(PESSIMISTIC, REPEATABLE_READ)) {
                blockProbe(grid(0), tx);

                cache.put(key1, 1);
                b.await();
                cache.put(key0, 0);

                tx.rollback();
            }

            return null;
        });

        b.await();
        tryPutRepeatedly(cache, key0);

        assertExactlyOneAbortedDueDeadlock(fut0, fut1);
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void nonDeadlockedTxDetectsDeadlock2() throws Exception {
        setUpGrids(2, false);

        List<Integer> keys0 = primaryKeys(grid(0).cache(DEFAULT_CACHE_NAME), 2);
        Integer key00 = keys0.get(0);
        Integer key01 = keys0.get(1);
        Integer key1 = primaryKey(grid(1).cache(DEFAULT_CACHE_NAME));

        IgniteCache<Object, Object> cache = client.cache(DEFAULT_CACHE_NAME);

        assert client.configuration().isClientMode();

        CyclicBarrier b = new CyclicBarrier(3);

        IgniteInternalFuture<Object> fut0 = GridTestUtils.runAsync(() -> {
            try (Transaction tx = client.transactions().txStart(PESSIMISTIC, REPEATABLE_READ)) {
                blockProbe(grid(1), tx);

                cache.put(key00, 0);
                b.await();
                cache.put(key1, 1);

                tx.rollback();
            }

            return null;
        });

        IgniteInternalFuture<Object> fut1 = GridTestUtils.runAsync(() -> {
            try (Transaction tx = client.transactions().txStart(PESSIMISTIC, REPEATABLE_READ)) {
                blockProbe(grid(0), tx);

                cache.put(key1, 1);
                cache.put(key01, 0);
                b.await();
                cache.put(key00, 0);

                tx.rollback();
            }

            return null;
        });

        b.await();
        tryPutRepeatedly(cache, key01);

        assertExactlyOneAbortedDueDeadlock(fut0, fut1);
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void randomizedPuts() throws Exception {
        int gridCnt = SF.applyLB(10, 2);
        int opsByWorker = SF.applyLB(1000, 10);

        setUpGrids(gridCnt, false);

        List<Integer> keys = new ArrayList<>();
        for (int i = 0; i < gridCnt; i++)
            keys.addAll(primaryKeys(grid(i).cache(DEFAULT_CACHE_NAME), 3));

        AtomicInteger aborted = new AtomicInteger();

        List<IgniteInternalFuture<?>> futs = new ArrayList<>();
        for (int i = 0; i < gridCnt * 2; i++) {
            IgniteEx ign = grid(i % gridCnt);
            IgniteCache<Object, Object> cache = ign.cache(DEFAULT_CACHE_NAME);

            IgniteInternalFuture fut = GridTestUtils.runAsync(() -> {
                for (int k = 0; k < opsByWorker; k++) {
                    try (Transaction tx = ign.transactions().txStart(PESSIMISTIC, REPEATABLE_READ)) {
                        ArrayList<Integer> keys0 = new ArrayList<>(keys);
                        Collections.shuffle(keys0);
                        int nkeys = ThreadLocalRandom.current().nextInt(8) + 5;
                        for (int j = 0; j < nkeys; j++)
                            cache.put(keys0.get(j), j);

                        tx.rollback();
                    }
                    catch (Exception e) {
                        if (X.hasCause(e, IgniteTxRollbackCheckedException.class))
                            aborted.incrementAndGet();
                    }
                }
            });
            futs.add(fut);
        }

        for (IgniteInternalFuture<?> fut : futs)
            fut.get(10, TimeUnit.MINUTES);

        log.info("Number of txs aborted: " + aborted);
    }

    /** */
    private static void blockProbe(IgniteEx ign, Transaction tx) {
        ((TestRecordingCommunicationSpi)ign.configuration().getCommunicationSpi())
            .blockMessages((node, msg) -> {
                if (msg instanceof DeadlockProbe) {
                    DeadlockProbe msg0 = (DeadlockProbe)msg;
                    GridNearTxLocal tx0 = ((TransactionProxyImpl)tx).tx();
                    return msg0.initiatorVersion().equals(tx0.xidVersion());
                }

                return false;
            });
    }

    /** */
    private void assertExactlyOneAbortedDueDeadlock(IgniteInternalFuture<?>... futs) throws IgniteCheckedException {
        assert futs.length > 0;

        int aborted = 0;

        for (IgniteInternalFuture<?> fut : futs) {
            try {
                fut.get(10, TimeUnit.SECONDS);
            }
            catch (IgniteCheckedException e) {
                if (X.hasCause(e, TransactionRollbackException.class))
                    aborted++;
                else
                    throw e;
            }
        }

        if (aborted != 1)
            fail("Exactly one tx is expected to be aborted, but was " + aborted);
    }

    /** */
    private void tryPutRepeatedly(IgniteCache<Object, Object> cache, Integer key0) {
        for (int i = 0; i < 100; i++) {
            try (Transaction tx = client.transactions().txStart(PESSIMISTIC, REPEATABLE_READ, 200, 1)) {
                cache.put(key0, 33);

                break;
            }
            catch (Exception ignored) {
            }
        }
    }
}
