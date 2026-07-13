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

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.cache.CacheAtomicityMode;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.internal.util.typedef.F;
import org.apache.ignite.spi.systemview.view.SystemView;
import org.apache.ignite.spi.systemview.view.TransactionView;
import org.apache.ignite.testframework.GridTestUtils;
import org.apache.ignite.transactions.Transaction;
import org.junit.Test;

import static org.apache.ignite.internal.processors.cache.GridCacheUtils.cacheId;
import static org.apache.ignite.internal.processors.cache.transactions.IgniteTxManager.TXS_MON_LIST;
import static org.apache.ignite.internal.util.lang.GridFunc.alwaysTrue;
import static org.apache.ignite.testframework.GridTestUtils.waitForCondition;
import static org.apache.ignite.transactions.TransactionConcurrency.OPTIMISTIC;
import static org.apache.ignite.transactions.TransactionConcurrency.PESSIMISTIC;
import static org.apache.ignite.transactions.TransactionIsolation.REPEATABLE_READ;
import static org.apache.ignite.transactions.TransactionIsolation.SERIALIZABLE;
import static org.apache.ignite.transactions.TransactionState.ACTIVE;

/** Tests for {@link SystemView} for transactions. */
public class SystemViewTransactionsTest extends SystemViewAbstractTest {
    /** */
    @Test
    public void testTransactions() throws Exception {
        try (IgniteEx g = startGrid(0)) {
            IgniteCache<Integer, Integer> cache1 = g.createCache(new CacheConfiguration<Integer, Integer>("c1")
                .setAtomicityMode(CacheAtomicityMode.TRANSACTIONAL));

            IgniteCache<Integer, Integer> cache2 = g.createCache(new CacheConfiguration<Integer, Integer>("c2")
                .setAtomicityMode(CacheAtomicityMode.TRANSACTIONAL));

            SystemView<TransactionView> txs = g.context().systemView().view(TXS_MON_LIST);

            assertEquals(0, F.size(txs.iterator(), alwaysTrue()));

            CountDownLatch latch = new CountDownLatch(1);

            try {
                AtomicInteger cntr = new AtomicInteger();

                GridTestUtils.runMultiThreadedAsync(() -> {
                    try (Transaction tx = g.transactions().withLabel("test").txStart(PESSIMISTIC, REPEATABLE_READ)) {
                        cache1.put(cntr.incrementAndGet(), cntr.incrementAndGet());
                        cache1.put(cntr.incrementAndGet(), cntr.incrementAndGet());

                        latch.await();
                    }
                    catch (InterruptedException e) {
                        throw new RuntimeException(e);
                    }
                }, 5, "xxx");

                boolean res = waitForCondition(() -> txs.size() == 5, 10_000L);

                assertTrue(res);

                TransactionView txv = txs.iterator().next();

                assertEquals(g.localNode().id(), txv.localNodeId());
                assertEquals(txv.isolation(), REPEATABLE_READ);
                assertEquals(txv.concurrency(), PESSIMISTIC);
                assertEquals(txv.state(), ACTIVE);
                assertNotNull(txv.xid());
                assertFalse(txv.system());
                assertFalse(txv.implicit());
                assertFalse(txv.implicitSingle());
                assertTrue(txv.near());
                assertFalse(txv.dht());
                assertTrue(txv.colocated());
                assertTrue(txv.local());
                assertEquals("test", txv.label());
                assertFalse(txv.onePhaseCommit());
                assertFalse(txv.internal());
                assertEquals(0, txv.timeout());
                assertTrue(txv.startTime() <= System.currentTimeMillis());
                assertEquals(String.valueOf(cacheId(cache1.getName())), txv.cacheIds());

                GridTestUtils.runMultiThreadedAsync(() -> {
                    try (Transaction tx = g.transactions().txStart(OPTIMISTIC, SERIALIZABLE)) {
                        cache1.put(cntr.incrementAndGet(), cntr.incrementAndGet());
                        cache1.put(cntr.incrementAndGet(), cntr.incrementAndGet());
                        cache2.put(cntr.incrementAndGet(), cntr.incrementAndGet());

                        latch.await();
                    }
                    catch (InterruptedException e) {
                        throw new RuntimeException(e);
                    }
                }, 5, "xxx");

                res = waitForCondition(() -> txs.size() == 10, 10_000L);

                assertTrue(res);

                for (TransactionView tx : txs) {
                    if (PESSIMISTIC == tx.concurrency())
                        continue;

                    assertEquals(g.localNode().id(), tx.localNodeId());
                    assertEquals(tx.isolation(), SERIALIZABLE);
                    assertEquals(tx.concurrency(), OPTIMISTIC);
                    assertEquals(tx.state(), ACTIVE);
                    assertNotNull(tx.xid());
                    assertFalse(tx.system());
                    assertFalse(tx.implicit());
                    assertFalse(tx.implicitSingle());
                    assertTrue(tx.near());
                    assertFalse(tx.dht());
                    assertTrue(tx.colocated());
                    assertTrue(tx.local());
                    assertNull(tx.label());
                    assertFalse(tx.onePhaseCommit());
                    assertFalse(tx.internal());
                    assertEquals(0, tx.timeout());
                    assertTrue(tx.startTime() <= System.currentTimeMillis());

                    String s1 = cacheId(cache1.getName()) + "," + cacheId(cache2.getName());
                    String s2 = cacheId(cache2.getName()) + "," + cacheId(cache1.getName());

                    assertTrue(s1.equals(tx.cacheIds()) || s2.equals(tx.cacheIds()));
                }
            }
            finally {
                latch.countDown();
            }

            boolean res = waitForCondition(() -> txs.size() == 0, 10_000L);

            assertTrue(res);
        }
    }
}
