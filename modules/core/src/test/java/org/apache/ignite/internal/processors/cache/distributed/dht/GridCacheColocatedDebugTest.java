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

package org.apache.ignite.internal.processors.cache.distributed.dht;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.Lock;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.cache.affinity.rendezvous.RendezvousAffinityFunction;
import org.apache.ignite.cache.store.CacheStore;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.IgniteInternalFuture;
import org.apache.ignite.internal.IgniteKernal;
import org.apache.ignite.internal.processors.cache.GridCacheAdapter;
import org.apache.ignite.internal.processors.cache.GridCacheContext;
import org.apache.ignite.internal.processors.cache.GridCacheEntryEx;
import org.apache.ignite.internal.processors.cache.GridCacheMvccCandidate;
import org.apache.ignite.internal.processors.cache.GridCacheTestStore;
import org.apache.ignite.internal.util.typedef.F;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.spi.discovery.tcp.TcpDiscoverySpi;
import org.apache.ignite.spi.discovery.tcp.ipfinder.TcpDiscoveryIpFinder;
import org.apache.ignite.spi.discovery.tcp.ipfinder.vm.TcpDiscoveryVmIpFinder;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.apache.ignite.transactions.Transaction;
import org.apache.ignite.transactions.TransactionConcurrency;
import org.apache.ignite.transactions.TransactionIsolation;

import static org.apache.ignite.cache.CacheMode.PARTITIONED;
import static org.apache.ignite.cache.CacheWriteSynchronizationMode.FULL_SYNC;
import static org.apache.ignite.transactions.TransactionConcurrency.OPTIMISTIC;
import static org.apache.ignite.transactions.TransactionConcurrency.PESSIMISTIC;
import static org.apache.ignite.transactions.TransactionIsolation.READ_COMMITTED;
import static org.apache.ignite.transactions.TransactionIsolation.REPEATABLE_READ;

/**
 * Tests for colocated cache.
 */
public class GridCacheColocatedDebugTest extends GridCommonAbstractTest {
    /** IP finder. */
    private static final TcpDiscoveryIpFinder ipFinder = new TcpDiscoveryVmIpFinder(true);

    /** Test thread count. */
    private static final int THREAD_CNT = 10;

    /** Store enable flag. */
    private boolean storeEnabled;

    /** {@inheritDoc} */
    @SuppressWarnings("unchecked")
    @Override protected IgniteConfiguration getConfiguration(String gridName) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(gridName);

        TcpDiscoverySpi spi = new TcpDiscoverySpi();

        spi.setIpFinder(ipFinder);

        cfg.setDiscoverySpi(spi);

        CacheConfiguration cacheCfg = defaultCacheConfiguration();

        cacheCfg.setCacheMode(PARTITIONED);
        cacheCfg.setNearConfiguration(null);
        cacheCfg.setAffinity(new RendezvousAffinityFunction(false, 30));
        cacheCfg.setBackups(1);
        cacheCfg.setWriteSynchronizationMode(FULL_SYNC);
        cacheCfg.setSwapEnabled(false);

        if (storeEnabled) {
            cacheCfg.setCacheStoreFactory(singletonFactory(new GridCacheTestStore()));
            cacheCfg.setReadThrough(true);
            cacheCfg.setWriteThrough(true);
            cacheCfg.setLoadPreviousValue(true);
        }
        else
            cacheCfg.setCacheStoreFactory(null);

        cfg.setCacheConfiguration(cacheCfg);

        return cfg;
    }

    /**
     * @throws Exception If failed.
     */
    public void testSimplestPessimistic() throws Exception {
        checkSinglePut(false, PESSIMISTIC, REPEATABLE_READ);
    }

    /**
     * @throws Exception If failed.
     */
    public void testSimpleOptimistic() throws Exception {
        checkSinglePut(true, OPTIMISTIC, REPEATABLE_READ);
    }

    /**
     * @throws Exception If failed.
     */
    public void testReentry() throws Exception {
        checkReentry(PESSIMISTIC, REPEATABLE_READ);
    }

    /**
     * @throws Exception If failed.
     */
    public void testDistributedInTxSeparatePessimistic() throws Exception {
        checkDistributedPut(true, true, PESSIMISTIC, REPEATABLE_READ);
    }

    /**
     * @throws Exception If failed.
     */
    public void testDistributedInTxPessimistic() throws Exception {
        checkDistributedPut(true, false, PESSIMISTIC, REPEATABLE_READ);
    }

    /**
     * @throws Exception If failed.
     */
    public void testDistributedSeparatePessimistic() throws Exception {
        checkDistributedPut(false, true, PESSIMISTIC, REPEATABLE_READ);
    }

    /**
     * @throws Exception If failed.
     */
    public void testDistributedPessimistic() throws Exception {
        checkDistributedPut(false, false, PESSIMISTIC, REPEATABLE_READ);
    }

    /**
     * @throws Exception If failed.
     */
    public void testDistributedNonLocalInTxSeparatePessimistic() throws Exception {
        checkNonLocalPuts(true, true, PESSIMISTIC, REPEATABLE_READ);
    }

    /**
     * @throws Exception If failed.
     */
    public void testDistributedNonLocalInTxPessimistic() throws Exception {
        checkNonLocalPuts(true, false, PESSIMISTIC, REPEATABLE_READ);
    }

    /**
     * @throws Exception If failed.
     */
    public void testDistributedNonLocalSeparatePessimistic() throws Exception {
        checkNonLocalPuts(false, true, PESSIMISTIC, REPEATABLE_READ);
    }

    /**
     * @throws Exception If failed.
     */
    public void testDistributedNonLocalPessimistic() throws Exception {
        checkNonLocalPuts(false, false, PESSIMISTIC, REPEATABLE_READ);
    }

    /**
     * @throws Exception If failed.
     */
    public void testRollbackSeparatePessimistic() throws Exception {
        checkRollback(true, PESSIMISTIC, REPEATABLE_READ);
    }

    /**
     * @throws Exception If failed.
     */
    public void testDistributedInTxSeparateOptimistic() throws Exception {
        checkDistributedPut(true, true, OPTIMISTIC, REPEATABLE_READ);
    }

    /**
     * @throws Exception If failed.
     */
    public void testDistributedInTxOptimistic() throws Exception {
        checkDistributedPut(true, false, OPTIMISTIC, REPEATABLE_READ);
    }

    /**
     * @throws Exception If failed.
     */
    public void testDistributedNonLocalInTxSeparateOptimistic() throws Exception {
        checkNonLocalPuts(true, true, OPTIMISTIC, REPEATABLE_READ);
    }

    /**
     * @throws Exception If failed.
     */
    public void testDistributedNonLocalInTxOptimistic() throws Exception {
        checkNonLocalPuts(true, false, OPTIMISTIC, REPEATABLE_READ);
    }

    /**
     * @throws Exception If failed.
     */
    public void testRollbackSeparateOptimistic() throws Exception {
        checkRollback(true, OPTIMISTIC, REPEATABLE_READ);
    }

    /**
     * @throws Exception If failed.
     */
    public void testRollback() throws Exception {
        checkRollback(false, PESSIMISTIC, REPEATABLE_READ);
    }

    /**
     * @throws Exception If failed.
     */
    public void testPutsMultithreadedColocated() throws Exception {
        checkPutsMultithreaded(true, false, 100000);
    }

    /**
     * @throws Exception If failed.
     */
    public void testPutsMultithreadedRemote() throws Exception {
       checkPutsMultithreaded(false, true, 100000);
    }

    /**
     * @throws Exception If failed.
     */
    public void testPutsMultithreadedMixed() throws Exception {
        checkPutsMultithreaded(true, true, 100000);
    }

    /**
     * @param loc Local puts.
     * @param remote Remote puts.
     * @param maxIterCnt Number of iterations.
     * @throws Exception If failed.
     */
    public void checkPutsMultithreaded(boolean loc, boolean remote, final long maxIterCnt) throws Exception {
        storeEnabled = false;

        assert loc || remote;

        startGridsMultiThreaded(3);

        try {
            final Ignite g0 = grid(0);
            Ignite g1 = grid(1);

            final Collection<Integer> keys = new ConcurrentLinkedQueue<>();

            if (loc) {
                Integer key = -1;

                for (int i = 0; i < 20; i++) {
                    key = forPrimary(g0, key);

                    keys.add(key);
                }
            }

            if (remote) {
                Integer key = -1;

                for (int i = 0; i < 20; i++) {
                    key = forPrimary(g1, key);

                    keys.add(key);
                }
            }

            final AtomicLong iterCnt = new AtomicLong();

            final int keysCnt = 10;

            IgniteInternalFuture<?> fut = multithreadedAsync(new Runnable() {
                @Override public void run() {
                    // Make thread-local copy to shuffle keys.
                    List<Integer> threadKeys = new ArrayList<>(keys);

                    long threadId = Thread.currentThread().getId();

                    long itNum;

                    while ((itNum = iterCnt.getAndIncrement()) < maxIterCnt) {
                        Collections.shuffle(threadKeys);

                        List<Integer> iterKeys = threadKeys.subList(0, keysCnt);

                        Collections.sort(iterKeys);

                        Map<Integer, String> vals = U.newLinkedHashMap(keysCnt);

                        for (Integer key : iterKeys)
                            vals.put(key, String.valueOf(key) + threadId);

                        jcache(0).putAll(vals);

                        if (itNum > 0 && itNum % 5000 == 0)
                            info(">>> " + itNum + " iterations completed.");
                    }
                }
            }, THREAD_CNT);

            fut.get();

            Thread.sleep(1000);
            // Check that all transactions are committed.
            for (int i = 0; i < 3; i++) {
                GridCacheAdapter<Object, Object> cache = ((IgniteKernal)grid(i)).internalCache();

                for (Integer key : keys) {
                    GridCacheEntryEx entry = cache.peekEx(key);

                    if (entry != null) {
                        Collection<GridCacheMvccCandidate> locCands = entry.localCandidates();
                        Collection<GridCacheMvccCandidate> rmtCands = entry.remoteMvccSnapshot();

                        assert locCands == null || locCands.isEmpty() : "Local candidates is not empty [idx=" + i +
                            ", entry=" + entry + ']';
                        assert rmtCands == null || rmtCands.isEmpty() : "Remote candidates is not empty [idx=" + i +
                            ", entry=" + entry + ']';
                    }
                }
            }
        }
        finally {
            stopAllGrids();
        }
    }

    /**
     * @throws Exception If failed.
     */
    public void testLockLockedLocal() throws Exception {
        checkLockLocked(true);
    }

    /**
     * @throws Exception If failed.
     */
    public void testLockLockedRemote() throws Exception {
        checkLockLocked(false);
    }

    /**
     *
     * @param loc Flag indicating local or remote key should be checked.
     * @throws Exception If failed.
     */
    private void checkLockLocked(boolean loc) throws Exception {
        storeEnabled = false;

        startGridsMultiThreaded(3);

        try {
            final Ignite g0 = grid(0);
            Ignite g1 = grid(1);

            final Integer key = forPrimary(loc ? g0 : g1);

            final CountDownLatch lockLatch = new CountDownLatch(1);
            final CountDownLatch unlockLatch = new CountDownLatch(1);

            final Lock lock = g0.cache(null).lock(key);

            IgniteInternalFuture<?> unlockFut = multithreadedAsync(new Runnable() {
                @Override public void run() {
                    try {
                        lock.lock();

                        try {
                            lockLatch.countDown();

                            U.await(unlockLatch);
                        }
                        finally {
                            lock.unlock();
                        }
                    }
                    catch (IgniteCheckedException e) {
                        fail("Unexpected exception: " + e);
                    }

                }
            }, 1);

            U.await(lockLatch);

            assert g0.cache(null).isLocalLocked(key, false);
            assert !g0.cache(null).isLocalLocked(key, true) : "Key can not be locked by current thread.";

            assert !lock.tryLock();

            assert g0.cache(null).isLocalLocked(key, false);
            assert !g0.cache(null).isLocalLocked(key, true) : "Key can not be locked by current thread.";

            unlockLatch.countDown();
            unlockFut.get();

            assert lock.tryLock();

            lock.unlock();
        }
        finally {
            stopAllGrids();
        }
    }

    /**
     * @throws Exception If failed.
     */
    public void testPessimisticGet() throws Exception {
        storeEnabled = false;

        startGridsMultiThreaded(3);

        Ignite g0 = grid(0);

        try {
            for (int i = 0; i < 100; i++)
                g0.cache(null).put(i, i);

            for (int i = 0; i < 100; i++) {
                try (Transaction tx = g0.transactions().txStart(PESSIMISTIC, REPEATABLE_READ)) {
                    Integer val = (Integer) g0.cache(null).get(i);

                    assertEquals((Integer) i, val);
                }
            }
        }
        finally {
            stopAllGrids();
        }
    }

    /**
     * @param explicitTx Whether or not start implicit tx.
     * @param concurrency Tx concurrency.
     * @param isolation Tx isolation.
     * @throws Exception If failed.
     */
    private void checkSinglePut(boolean explicitTx, TransactionConcurrency concurrency, TransactionIsolation isolation)
        throws Exception {
        startGrid();

        try {
            Transaction tx = explicitTx ? grid().transactions().txStart(concurrency, isolation) : null;

            try {
                IgniteCache<Object, Object> cache = jcache();

                cache.putAll(F.asMap(1, "Hello", 2, "World"));

                if (tx != null)
                    tx.commit();

                System.out.println(cache.metrics());

                assertEquals("Hello", cache.get(1));
                assertEquals("World", cache.get(2));
                assertNull(cache.get(3));
            }
            finally {
                if (tx != null)
                    tx.close();
            }
        }
        finally {
            stopAllGrids();
        }
    }

    /**
     * @param concurrency Tx concurrency.
     * @param isolation Tx isolation.
     * @throws Exception If failed.
     */
    private void checkReentry(TransactionConcurrency concurrency, TransactionIsolation isolation) throws Exception {
        startGrid();

        try {
            Transaction tx = grid().transactions().txStart(concurrency, isolation);

            try {
                IgniteCache<Object, Object> cache = jcache();

                String old = (String)cache.get(1);

                assert old == null;

                String replaced = (String)cache.getAndPut(1, "newVal");

                assert replaced == null;

                replaced = (String)cache.getAndPut(1, "newVal2");

                assertEquals("newVal", replaced);

                if (tx != null)
                    tx.commit();

                assertEquals("newVal2", cache.get(1));
                assertNull(cache.get(3));
            }
            finally {
                if (tx != null)
                    tx.close();
            }
        }
        finally {
            stopAllGrids();
        }
    }

    /**
     * @param explicitTx Use explicit transactions.
     * @param separate Use one-key puts instead of batch.
     * @param concurrency Transactions concurrency.
     * @param isolation Transaction isolation.
     * @throws Exception If failed.
     */
    @SuppressWarnings("AssertEqualsBetweenInconvertibleTypes")
    private void checkDistributedPut(boolean explicitTx, boolean separate, TransactionConcurrency concurrency,
        TransactionIsolation isolation) throws Exception {
        storeEnabled = false;

        startGridsMultiThreaded(3);

        Ignite g0 = grid(0);
        Ignite g1 = grid(1);
        Ignite g2 = grid(2);

        try {
            Integer k0 = forPrimary(g0);
            Integer k1 = forPrimary(g1);
            Integer k2 = forPrimary(g2);

            Map<Integer, String> map = F.asMap(k0, "val" + k0, k1, "val" + k1, k2, "val" + k2);

            Transaction tx = explicitTx ? g0.transactions().txStart(concurrency, isolation) : null;

            try {
                if (separate) {
                    g0.cache(null).put(k0, "val" + k0);
                    g0.cache(null).put(k1, "val" + k1);
                    g0.cache(null).put(k2, "val" + k2);
                }
                else
                    g0.cache(null).putAll(map);

                if (tx != null)
                    tx.commit();
            }
            finally {
                if (tx != null)
                    tx.close();
            }

            if (separate) {
                assertEquals("val" + k0, g0.cache(null).get(k0));
                assertEquals("val" + k1, g0.cache(null).get(k1));
                assertEquals("val" + k2, g0.cache(null).get(k2));
            }
            else {
                Map<Object, Object> res = g0.cache(null).getAll(map.keySet());

                assertEquals(map, res);
            }

            tx = explicitTx ? g0.transactions().txStart(concurrency, isolation) : null;

            try {
                if (separate) {
                    g0.cache(null).remove(k0);
                    g0.cache(null).remove(k1);
                    g0.cache(null).remove(k2);
                }
                else
                    g0.cache(null).removeAll(map.keySet());

                if (tx != null)
                    tx.commit();
            }
            finally {
                if (tx != null)
                    tx.close();
            }

            if (separate) {
                assertEquals(null, g0.cache(null).get(k0));
                assertEquals(null, g0.cache(null).get(k1));
                assertEquals(null, g0.cache(null).get(k2));
            }
            else {
                Map<Object, Object> res = g0.cache(null).getAll(map.keySet());

                assertTrue(res.isEmpty());
            }
        }
        finally {
            stopAllGrids();
        }
    }

    /**
     * @param explicitTx Use explicit transactions.
     * @param separate Use one-key puts instead of batch.
     * @param concurrency Transactions concurrency.
     * @param isolation Transaction isolation.
     * @throws Exception If failed.
     */
    @SuppressWarnings("AssertEqualsBetweenInconvertibleTypes")
    private void checkNonLocalPuts(boolean explicitTx, boolean separate, TransactionConcurrency concurrency,
        TransactionIsolation isolation) throws Exception {
        storeEnabled = false;

        startGridsMultiThreaded(3);

        Ignite g0 = grid(0);
        Ignite g1 = grid(1);
        Ignite g2 = grid(2);

        try {
            Integer k1 = forPrimary(g1);
            Integer k2 = forPrimary(g2);

            Map<Integer, String> map = F.asMap(k1, "val" + k1, k2, "val" + k2);

            Transaction tx = explicitTx ? g0.transactions().txStart(concurrency, isolation) : null;

            try {
                if (separate) {
                    g0.cache(null).put(k1, "val" + k1);
                    g0.cache(null).put(k2, "val" + k2);
                }
                else
                    g0.cache(null).putAll(map);

                if (tx != null)
                    tx.commit();
            }
            finally {
                if (tx != null)
                    tx.close();
            }

            if (separate) {
                assertEquals("val" + k1, g0.cache(null).get(k1));
                assertEquals("val" + k2, g0.cache(null).get(k2));
            }
            else {
                Map<Object, Object> res = g0.cache(null).getAll(map.keySet());

                assertEquals(map, res);
            }

            tx = explicitTx ? g0.transactions().txStart(concurrency, isolation) : null;

            try {
                if (separate) {
                    g0.cache(null).remove(k1);
                    g0.cache(null).remove(k2);
                }
                else
                    g0.cache(null).removeAll(map.keySet());

                if (tx != null)
                    tx.commit();
            }
            finally {
                if (tx != null)
                    tx.close();
            }

            if (separate) {
                assertEquals(null, g0.cache(null).get(k1));
                assertEquals(null, g0.cache(null).get(k2));
            }
            else {
                Map<Object, Object> res = g0.cache(null).getAll(map.keySet());

                assertTrue(res.isEmpty());
            }
        }
        finally {
            stopAllGrids();
        }
    }

    /**
     * @throws Exception If failed.
     */
    public void testWriteThrough() throws Exception {
        storeEnabled = true;

        startGridsMultiThreaded(3);

        Ignite g0 = grid(0);
        Ignite g1 = grid(1);
        Ignite g2 = grid(2);

        try {
            // Check local commit.
            int k0 = forPrimary(g0);
            int k1 = forPrimary(g0, k0);
            int k2 = forPrimary(g0, k1);

            checkStoreWithValues(F.asMap(k0, String.valueOf(k0), k1, String.valueOf(k1), k2, String.valueOf(k2)));

            // Reassign keys.
            k1 = forPrimary(g1);
            k2 = forPrimary(g2);

            checkStoreWithValues(F.asMap(k0, String.valueOf(k0), k1, String.valueOf(k1), k2, String.valueOf(k2)));

            // Check remote only.

            checkStoreWithValues(F.asMap(k1, String.valueOf(k1), k2, String.valueOf(k2)));
        }
        finally {
            stopAllGrids();
        }
    }

    /**
     * @param map Values to check.
     * @throws Exception If failed.
     */
    private void checkStoreWithValues(Map<Integer, String> map) throws Exception {
        Ignite g0 = grid(0);
        Ignite g1 = grid(1);
        Ignite g2 = grid(2);

        g0.cache(null).putAll(map);

        checkStore(g0, map);
        checkStore(g1, Collections.<Integer, String>emptyMap());
        checkStore(g2, Collections.<Integer, String>emptyMap());

        clearStores(3);

        try (Transaction tx = g0.transactions().txStart(OPTIMISTIC, READ_COMMITTED)) {
            g0.cache(null).putAll(map);

            tx.commit();

            checkStore(g0, map);
            checkStore(g1, Collections.<Integer, String>emptyMap());
            checkStore(g2, Collections.<Integer, String>emptyMap());

            clearStores(3);
        }
    }

    /**
     * @param ignite Grid to take store from.
     * @param map Expected values in store.
     * @throws Exception If failed.
     */
    private void checkStore(Ignite ignite, Map<Integer, String> map) throws Exception {
        String cacheName = ignite.configuration().getCacheConfiguration()[0].getName();

        GridCacheContext ctx = ((IgniteKernal)grid()).context().cache().internalCache(cacheName).context();

        CacheStore store = ctx.store().configuredStore();

        assertEquals(map, ((GridCacheTestStore)store).getMap());
    }

    /**
     * Clears all stores.
     *
     * @param cnt Grid count.
     */
    private void clearStores(int cnt) {
        for (int i = 0; i < cnt; i++) {
            String cacheName = grid(i).configuration().getCacheConfiguration()[0].getName();

            GridCacheContext ctx = ((IgniteKernal)grid()).context().cache().internalCache(cacheName).context();

            CacheStore store = ctx.store().configuredStore();

            ((GridCacheTestStore)store).reset();
        }
    }

    /**
     * @param separate Use one-key puts instead of batch.
     * @param concurrency Transactions concurrency.
     * @param isolation Transaction isolation.
     * @throws Exception If failed.
     */
    @SuppressWarnings("AssertEqualsBetweenInconvertibleTypes")
    private void checkRollback(boolean separate, TransactionConcurrency concurrency, TransactionIsolation isolation)
        throws Exception {
        storeEnabled = false;

        startGridsMultiThreaded(3);

        Ignite g0 = grid(0);
        Ignite g1 = grid(1);
        Ignite g2 = grid(2);

        try {
            Integer k0 = forPrimary(g0);
            Integer k1 = forPrimary(g1);
            Integer k2 = forPrimary(g2);

            Map<Integer, String> map0 = F.asMap(k0, "val" + k0, k1, "val" + k1, k2, "val" + k2);

            g0.cache(null).putAll(map0);

            Map<Integer, String> map = F.asMap(k0, "value" + k0, k1, "value" + k1, k2, "value" + k2);

            Transaction tx = g0.transactions().txStart(concurrency, isolation);

            try {
                if (separate) {
                    g0.cache(null).put(k0, "value" + k0);
                    g0.cache(null).put(k1, "value" + k1);
                    g0.cache(null).put(k2, "value" + k2);
                }
                else
                    g0.cache(null).putAll(map);

                tx.rollback();
            }
            finally {
                tx.close();
            }

            if (separate) {
                assertEquals("val" + k0, g0.cache(null).get(k0));
                assertEquals("val" + k1, g0.cache(null).get(k1));
                assertEquals("val" + k2, g0.cache(null).get(k2));
            }
            else {
                Map<Object, Object> res = g0.cache(null).getAll(map.keySet());

                assertEquals(map0, res);
            }

            tx = g0.transactions().txStart(concurrency, isolation);

            try {
                if (separate) {
                    g0.cache(null).remove(k0);
                    g0.cache(null).remove(k1);
                    g0.cache(null).remove(k2);
                }
                else
                    g0.cache(null).removeAll(map.keySet());

                tx.rollback();
            }
            finally {
                tx.close();
            }

            if (separate) {
                assertEquals("val" + k0, g0.cache(null).get(k0));
                assertEquals("val" + k1, g0.cache(null).get(k1));
                assertEquals("val" + k2, g0.cache(null).get(k2));
            }
            else {
                Map<Object, Object> res = g0.cache(null).getAll(map.keySet());

                assertEquals(map0, res);
            }
        }
        finally {
            stopAllGrids();
        }
    }

    /**
     * @throws Exception If failed.
     */
    public void testExplicitLocks() throws Exception {
        storeEnabled = false;

        startGrid();

        try {
            IgniteCache<Object, Object> cache = jcache();

            Lock lock = cache.lock(1);

            lock.lock();

            assertNull(cache.getAndPut(1, "key1"));
            assertEquals("key1", cache.getAndPut(1, "key2"));
            assertEquals("key2", cache.get(1));

            lock.unlock();
        }
        finally {
            stopAllGrids();
        }
    }

    /**
     * @throws Exception If failed.
     */
    public void testExplicitLocksDistributed() throws Exception {
        storeEnabled = false;

        startGridsMultiThreaded(3);

        Ignite g0 = grid(0);
        Ignite g1 = grid(1);
        Ignite g2 = grid(2);

        try {
            Integer k0 = forPrimary(g0);
            Integer k1 = forPrimary(g1);
            Integer k2 = forPrimary(g2);

            IgniteCache<Object, Object> cache = jcache(0);

            Lock lock0 = cache.lock(k0);
            Lock lock1 = cache.lock(k1);
            Lock lock2 = cache.lock(k2);

            lock0.lock();
            lock1.lock();
            lock2.lock();

            cache.put(k0, "val0");

            cache.putAll(F.asMap(k1, "val1", k2, "val2"));

            assertEquals("val0", cache.get(k0));
            assertEquals("val1", cache.get(k1));
            assertEquals("val2", cache.get(k2));

            lock0.unlock();
            lock1.unlock();
            lock2.unlock();
        }
        finally {
            stopAllGrids();
        }
    }

    /**
     * Gets key for which given node is primary.
     *
     * @param g Grid.
     * @return Key.
     */
    private static Integer forPrimary(Ignite g) {
        return forPrimary(g, -1);
    }

    /**
     * Gets next key for which given node is primary, starting with (prev + 1)
     *
     * @param g Grid.
     * @param prev Previous key.
     * @return Key.
     */
    private static Integer forPrimary(Ignite g, int prev) {
        for (int i = prev + 1; i < 10000; i++) {
            if (g.affinity(null).mapKeyToNode(i).id().equals(g.cluster().localNode().id()))
                return i;
        }

        throw new IllegalArgumentException("Can not find key being primary for node: " + g.cluster().localNode().id());
    }
}