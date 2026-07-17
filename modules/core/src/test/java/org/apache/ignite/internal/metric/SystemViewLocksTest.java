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

package org.apache.ignite.internal.metric;

import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.locks.Lock;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.client.Person;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.internal.IgniteInternalFuture;
import org.apache.ignite.internal.util.typedef.F;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.spi.systemview.view.CacheExplicitLockView;
import org.apache.ignite.spi.systemview.view.CacheLockView;
import org.apache.ignite.spi.systemview.view.SystemView;
import org.apache.ignite.spi.systemview.view.TransactionView;
import org.apache.ignite.testframework.GridTestUtils;
import org.apache.ignite.testframework.junits.WithSystemProperty;
import org.apache.ignite.transactions.Transaction;
import org.junit.Test;

import static org.apache.ignite.IgniteCommonsSystemProperties.IGNITE_TO_STRING_INCLUDE_SENSITIVE;
import static org.apache.ignite.cache.CacheAtomicityMode.TRANSACTIONAL;
import static org.apache.ignite.cache.CacheMode.PARTITIONED;
import static org.apache.ignite.internal.processors.cache.GridCacheMvccManager.CACHE_EXPLICIT_LOCKS_VIEW;
import static org.apache.ignite.internal.processors.cache.GridCacheMvccManager.CACHE_LOCKS_VIEW;
import static org.apache.ignite.internal.processors.cache.transactions.IgniteTxManager.TXS_MON_LIST;
import static org.apache.ignite.testframework.GridTestUtils.waitForCondition;
import static org.apache.ignite.transactions.TransactionConcurrency.PESSIMISTIC;
import static org.apache.ignite.transactions.TransactionIsolation.REPEATABLE_READ;

/** Tests for {@link SystemView} for locks. */
public class SystemViewLocksTest extends SystemViewAbstractTest {
    /** {@inheritDoc} */
    @Override protected void beforeTest() throws Exception {
        super.beforeTest();

        IgniteEx ignite = startGrids(3);

        ignite.getOrCreateCache(
            new CacheConfiguration<Integer, Integer>(DEFAULT_CACHE_NAME)
                .setAtomicityMode(TRANSACTIONAL)
                .setCacheMode(PARTITIONED)
                .setBackups(1)
        );
    }

    /** {@inheritDoc} */
    @Override protected void afterTest() throws Exception {
        stopAllGrids();

        super.afterTest();
    }

    /** */
    @Test
    public void testExplicitLocks() throws Exception {
        IgniteEx ignite0 = grid(0);
        IgniteEx ignite1 = grid(1);
        IgniteEx ignite2 = grid(2);

        CountDownLatch finishLatch = new CountDownLatch(1);

        try {
            Integer key0 = primaryKey(ignite0.cache(DEFAULT_CACHE_NAME));
            Integer key1 = primaryKey(ignite1.cache(DEFAULT_CACHE_NAME));
            Integer key2 = primaryKey(ignite2.cache(DEFAULT_CACHE_NAME));

            CountDownLatch firstNodeLatch = new CountDownLatch(1);

            // Block key0 and key1 from node0.
            Runnable task0 = explicitLockTask(ignite0, () -> {
                firstNodeLatch.countDown();
                U.awaitQuiet(finishLatch);
            }, key0, key1);

            // Block key2 and wait for key0 from node1.
            Runnable task1 = explicitLockTask(ignite1, () -> {}, key2, key0);

            // Wait for key0 from node2.
            Runnable task2a = explicitLockTask(ignite2, () -> {}, key0);

            // Wait for key1 from node2.
            Runnable task2b = explicitLockTask(ignite2, () -> {}, key1);

            IgniteInternalFuture<?> fut0 = GridTestUtils.runAsync(task0);
            firstNodeLatch.await();
            IgniteInternalFuture<?> fut1 = GridTestUtils.runAsync(task1);
            IgniteInternalFuture<?> fut2a = GridTestUtils.runAsync(task2a);
            IgniteInternalFuture<?> fut2b = GridTestUtils.runAsync(task2b);

            List<CacheExplicitLockView> explicitLocks0 = viewContent(ignite0, CACHE_EXPLICIT_LOCKS_VIEW, 2);
            List<CacheExplicitLockView> explicitLocks1 = viewContent(ignite1, CACHE_EXPLICIT_LOCKS_VIEW, 2);
            List<CacheExplicitLockView> explicitLocks2 = viewContent(ignite2, CACHE_EXPLICIT_LOCKS_VIEW, 2);

            List<CacheLockView> keyLocks0 = viewContent(ignite0, CACHE_LOCKS_VIEW, 3);
            List<CacheLockView> keyLocks1 = viewContent(ignite1, CACHE_LOCKS_VIEW, 2);
            List<CacheLockView> keyLocks2 = viewContent(ignite2, CACHE_LOCKS_VIEW, 1);

            // Check threads.
            assertEquals(explicitLocks0.get(0).threadId(), explicitLocks0.get(1).threadId());
            assertEquals(explicitLocks1.get(0).threadId(), explicitLocks1.get(1).threadId());
            assertNotSame(explicitLocks2.get(0).threadId(), explicitLocks2.get(1).threadId());

            // Check lock owners.
            CacheLockView owner0 = F.find(keyLocks0, null, CacheLockView::isOwner);
            CacheLockView owner1 = F.find(keyLocks1, null, CacheLockView::isOwner);
            CacheLockView owner2 = F.find(keyLocks2, null, CacheLockView::isOwner);

            assertNotNull(owner0);
            assertNotNull(owner1);
            assertNotNull(owner2);

            assertEquals(owner0.originatingNodeId(), ignite0.localNode().id());
            assertEquals(1, F.size(explicitLocks0, l -> l.xid().equals(owner0.originatingXid())));

            assertEquals(owner1.originatingNodeId(), ignite0.localNode().id());
            assertEquals(1, F.size(explicitLocks0, l -> l.xid().equals(owner1.originatingXid())));

            assertEquals(owner2.originatingNodeId(), ignite1.localNode().id());
            assertEquals(1, F.size(explicitLocks1, l -> l.xid().equals(owner2.originatingXid())));

            // Check waiting locks.
            assertEquals(1, F.size(keyLocks0,
                l -> !l.isOwner() && l.originatingNodeId().equals(ignite1.localNode().id())));
            assertEquals(1, F.size(keyLocks0,
                l -> !l.isOwner() && l.originatingNodeId().equals(ignite2.localNode().id())));
            assertEquals(1, F.size(keyLocks1,
                l -> !l.isOwner() && l.originatingNodeId().equals(ignite2.localNode().id())));

            finishLatch.countDown();

            fut0.get();
            fut1.get();
            fut2a.get();
            fut2b.get();
        }
        finally {
            finishLatch.countDown();
        }
    }

    /** */
    @Test
    public void testTxLocks() throws Exception {
        IgniteEx ignite0 = grid(0);
        IgniteEx ignite1 = grid(1);
        IgniteEx ignite2 = grid(2);

        CountDownLatch finishLatch = new CountDownLatch(1);

        try {
            Integer key0 = primaryKey(ignite0.cache(DEFAULT_CACHE_NAME));
            Integer key1 = primaryKey(ignite1.cache(DEFAULT_CACHE_NAME));
            Integer key2 = primaryKey(ignite2.cache(DEFAULT_CACHE_NAME));

            CountDownLatch firstNodeLatch = new CountDownLatch(1);

            // Block key0 and key1 from node0.
            Runnable task0 = txLockTask(ignite0, () -> {
                firstNodeLatch.countDown();
                U.awaitQuiet(finishLatch);
            }, key0, key1);

            // Block key2 and wait for key0 from node1.
            Runnable task1 = txLockTask(ignite1, () -> {}, key2, key0);

            // Wait for key0 from node2.
            Runnable task2a = txLockTask(ignite2, () -> {}, key0);

            // Wait for key1 from node2.
            Runnable task2b = txLockTask(ignite2, () -> {}, key1);

            IgniteInternalFuture<?> fut0 = GridTestUtils.runAsync(task0);
            firstNodeLatch.await();
            IgniteInternalFuture<?> fut1 = GridTestUtils.runAsync(task1);
            IgniteInternalFuture<?> fut2a = GridTestUtils.runAsync(task2a);
            IgniteInternalFuture<?> fut2b = GridTestUtils.runAsync(task2b);

            List<TransactionView> txs0 = viewContent(ignite0, TXS_MON_LIST, 3); // 1 near + 2 dht.
            List<TransactionView> txs1 = viewContent(ignite1, TXS_MON_LIST, 3); // 1 near + 2 dht
            List<TransactionView> txs2 = viewContent(ignite2, TXS_MON_LIST, 3); // 2 near + 1 dht

            List<CacheLockView> keyLocks0 = viewContent(ignite0, CACHE_LOCKS_VIEW, 3);
            List<CacheLockView> keyLocks1 = viewContent(ignite1, CACHE_LOCKS_VIEW, 2);
            List<CacheLockView> keyLocks2 = viewContent(ignite2, CACHE_LOCKS_VIEW, 1);

            // Check lock owners.
            CacheLockView owner0 = F.find(keyLocks0, null, CacheLockView::isOwner);
            CacheLockView owner1 = F.find(keyLocks1, null, CacheLockView::isOwner);
            CacheLockView owner2 = F.find(keyLocks2, null, CacheLockView::isOwner);

            assertNotNull(owner0);
            assertNotNull(owner1);
            assertNotNull(owner2);

            assertEquals(owner0.originatingNodeId(), ignite0.localNode().id());
            assertEquals(1, F.size(txs0, l -> l.xid().equals(owner0.originatingXid()) && l.xid().equals(owner0.xid())));

            assertEquals(owner1.originatingNodeId(), ignite0.localNode().id());
            assertEquals(1, F.size(txs0, l -> l.xid().equals(owner1.originatingXid()))); // Near.
            assertEquals(1, F.size(txs1, l -> l.xid().equals(owner1.xid()))); // Dht.

            assertEquals(owner2.originatingNodeId(), ignite1.localNode().id());
            assertEquals(1, F.size(txs1, l -> l.xid().equals(owner2.originatingXid()))); // Near.
            assertEquals(1, F.size(txs2, l -> l.xid().equals(owner2.xid()))); // Dht.

            // Check waiting locks.
            CacheLockView waiting1on0 = F.find(keyLocks0, null,
                l -> !l.isOwner() && l.originatingNodeId().equals(ignite1.localNode().id()));
            CacheLockView waiting2on0 = F.find(keyLocks0, null,
                l -> !l.isOwner() && l.originatingNodeId().equals(ignite2.localNode().id()));
            CacheLockView waiting2on1 = F.find(keyLocks1, null,
                l -> !l.isOwner() && l.originatingNodeId().equals(ignite2.localNode().id()));

            assertNotNull(waiting1on0);
            assertNotNull(waiting2on0);
            assertNotNull(waiting2on1);

            assertEquals(1, F.size(txs1, l -> l.xid().equals(waiting1on0.originatingXid()))); // Near.
            assertEquals(1, F.size(txs0, l -> l.xid().equals(waiting1on0.xid()))); // Dht.

            assertEquals(1, F.size(txs2, l -> l.xid().equals(waiting2on0.originatingXid()))); // Near.
            assertEquals(1, F.size(txs0, l -> l.xid().equals(waiting2on0.xid()))); // Dht.

            assertEquals(1, F.size(txs2, l -> l.xid().equals(waiting2on1.originatingXid()))); // Near.
            assertEquals(1, F.size(txs1, l -> l.xid().equals(waiting2on1.xid()))); // Dht.

            finishLatch.countDown();

            fut0.get();
            fut1.get();
            fut2a.get();
            fut2b.get();
        }
        finally {
            finishLatch.countDown();
        }
    }

    /** */
    @Test
    public void testMixedExplicitTxLocks() throws Exception {
        IgniteEx ignite0 = grid(0);
        IgniteEx ignite1 = grid(1);
        IgniteEx ignite2 = grid(2);

        CountDownLatch finishLatch = new CountDownLatch(1);

        try {
            Integer key1 = primaryKey(ignite1.cache(DEFAULT_CACHE_NAME));
            Integer key2 = primaryKey(ignite2.cache(DEFAULT_CACHE_NAME));

            CountDownLatch firstNodeLatch = new CountDownLatch(1);
            CountDownLatch secondNodeLatch = new CountDownLatch(1);

            // Block key1 from node0.
            Runnable task0 = explicitLockTask(ignite0, () -> {
                firstNodeLatch.countDown();
                U.awaitQuiet(finishLatch);
            }, key1);

            // Block key2 from node1.
            Runnable task1 = txLockTask(ignite1, () -> {
                secondNodeLatch.countDown();
                U.awaitQuiet(finishLatch);
            }, key2);

            // Wait for key2 from node2.
            Runnable task2a = explicitLockTask(ignite2, () -> {}, key2);

            // Wait for key1 from node2.
            Runnable task2b = txLockTask(ignite2, () -> {}, key1);

            IgniteInternalFuture<?> fut0 = GridTestUtils.runAsync(task0);
            firstNodeLatch.await();
            IgniteInternalFuture<?> fut1 = GridTestUtils.runAsync(task1);
            secondNodeLatch.await();
            IgniteInternalFuture<?> fut2a = GridTestUtils.runAsync(task2a);
            IgniteInternalFuture<?> fut2b = GridTestUtils.runAsync(task2b);

            List<CacheExplicitLockView> explicitLocks0 = viewContent(ignite0, CACHE_EXPLICIT_LOCKS_VIEW, 1);
            List<CacheExplicitLockView> explicitLocks2 = viewContent(ignite2, CACHE_EXPLICIT_LOCKS_VIEW, 1);
            List<TransactionView> txs1 = viewContent(ignite1, TXS_MON_LIST, 2); // 1 near + 1 dht
            List<TransactionView> txs2 = viewContent(ignite2, TXS_MON_LIST, 2); // 1 near + 1 dht

            List<CacheLockView> keyLocks1 = viewContent(ignite1, CACHE_LOCKS_VIEW, 2);
            List<CacheLockView> keyLocks2 = viewContent(ignite2, CACHE_LOCKS_VIEW, 2);

            // Check lock owners.
            CacheLockView owner1 = F.find(keyLocks1, null, CacheLockView::isOwner);
            CacheLockView owner2 = F.find(keyLocks2, null, CacheLockView::isOwner);

            assertNotNull(owner1);
            assertNotNull(owner2);

            assertEquals(owner1.originatingNodeId(), ignite0.localNode().id());
            assertEquals(1, F.size(explicitLocks0, l -> l.xid().equals(owner1.originatingXid())));

            assertEquals(owner2.originatingNodeId(), ignite1.localNode().id());
            assertEquals(1, F.size(txs1, l -> l.xid().equals(owner2.originatingXid()))); // Near.
            assertEquals(1, F.size(txs2, l -> l.xid().equals(owner2.xid()))); // Dht.

            // Check waiting locks.
            CacheLockView waiting2on1 = F.find(keyLocks1, null,
                l -> !l.isOwner() && l.originatingNodeId().equals(ignite2.localNode().id()));
            CacheLockView waiting2on2 = F.find(keyLocks2, null,
                l -> !l.isOwner() && l.originatingNodeId().equals(ignite2.localNode().id()));

            assertNotNull(waiting2on1);
            assertNotNull(waiting2on2);

            assertEquals(1, F.size(txs2, l -> l.xid().equals(waiting2on1.originatingXid()))); // Near.
            assertEquals(1, F.size(txs1, l -> l.xid().equals(waiting2on1.xid()))); // Dht.

            assertEquals(1, F.size(explicitLocks2, l -> l.xid().equals(waiting2on2.originatingXid())));

            finishLatch.countDown();

            fut0.get();
            fut1.get();
            fut2a.get();
            fut2b.get();
        }
        finally {
            finishLatch.countDown();
        }
    }

    /** */
    @Test
    public void testKeyTextIncludeSensitive() throws Exception {
        checkKeyText(true);
    }

    /** */
    @Test
    @WithSystemProperty(key = IGNITE_TO_STRING_INCLUDE_SENSITIVE, value = "false")
    public void testKeyTextExcludeSensitive() throws Exception {
        checkKeyText(false);
    }

    /** */
    public void checkKeyText(boolean includeSensitive) throws Exception {
        IgniteEx ignite = grid(0);
        IgniteCache<Object, Object> cache = ignite.cache(DEFAULT_CACHE_NAME);

        // Check primitive object.
        String key1 = "testKey";

        Lock lock1 = cache.lock(key1);

        lock1.lock();

        try {
            List<CacheExplicitLockView> explicitLocks = viewContent(ignite, CACHE_EXPLICIT_LOCKS_VIEW, 1);
            // Sensitive data included: KeyCacheObjectImpl [part=859, val=testKey, hasValBytes=false]
            // Sensitive data excluded: KeyCacheObject [hasValBytes=false]
            assertEquals(includeSensitive, explicitLocks.get(0).key().contains(key1));
        }
        finally {
            lock1.unlock();
        }

        // Check binary object.
        Person key2 = new Person(12345, "testName");

        Lock lock2 = cache.lock(key2);

        lock2.lock();

        try {
            List<CacheExplicitLockView> explicitLocks = viewContent(ignite, CACHE_EXPLICIT_LOCKS_VIEW, 1);
            // Sensitive data included: org.apache.ignite.client.Person [idHash=..., hash=-1202253851, id=12345, name=testName]
            // Sensitive data excluded: BinaryObject [idHash=..., hash=-1202253851]
            assertEquals(includeSensitive, explicitLocks.get(0).key().contains(key2.getClass().getName()));
            assertEquals(includeSensitive, explicitLocks.get(0).key().contains("id=" + key2.getId()));
            assertEquals(includeSensitive, explicitLocks.get(0).key().contains("name=" + key2.getName()));
        }
        finally {
            lock2.unlock();
        }
    }

    /** */
    private static Runnable explicitLockTask(IgniteEx ignite, Runnable body, int... keysToLock) {
        return () -> {
            Lock[] locks = new Lock[keysToLock.length];

            for (int i = 0; i < keysToLock.length; i++) {
                locks[i] = ignite.cache(DEFAULT_CACHE_NAME).lock(keysToLock[i]);
                locks[i].lock();
            }

            try {
                body.run();
            }
            finally {
                for (int i = keysToLock.length - 1; i >= 0; i--)
                    locks[i].unlock();
            }
        };
    }

    /** */
    private static Runnable txLockTask(IgniteEx ignite, Runnable body, int... keysToLock) {
        return () -> {
            try (Transaction tx = ignite.transactions().txStart(PESSIMISTIC, REPEATABLE_READ)) {
                for (int i = 0; i < keysToLock.length; i++)
                    ignite.cache(DEFAULT_CACHE_NAME).put(keysToLock[i], 0);

                body.run();

                tx.commit();
            }
        };
    }

    /** */
    private static <T> List<T> viewContent(IgniteEx ignite, String viewName, int expSize) throws Exception {
        SystemView<T> view = ignite.context().systemView().view(viewName);

        assertTrue("Failed to wait for view size [ignite=" + ignite.name() + ", viewName=" + viewName +
                ", expSize=" + expSize + ", actSize=" + F.size(view.iterator()),
            waitForCondition(() -> F.size(view.iterator()) == expSize, 1_000L));

        return U.arrayList(view.iterator(), expSize);
    }
}
