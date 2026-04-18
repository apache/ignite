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

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.locks.Lock;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.testframework.GridTestUtils;
import org.apache.ignite.transactions.Transaction;
import org.apache.ignite.transactions.TransactionConcurrency;
import org.apache.ignite.transactions.TransactionIsolation;
import org.junit.jupiter.api.Test;

import static org.apache.ignite.transactions.TransactionConcurrency.PESSIMISTIC;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 *
 */
public class IgnitePessimisticTxSuspendResumeTest extends IgniteAbstractTxSuspendResumeTest {
    /** {@inheritDoc} */
    @Override protected TransactionConcurrency transactionConcurrency() {
        return PESSIMISTIC;
    }

    /**
     * Test explicit locks, implicit transactions and suspend/resume of pessimistic transactions.
     *
     * @throws Exception If failed.
     */
    @Test
    public void testExplicitLockAndSuspendResume() throws Exception {
        executeTestForAllCaches(new CI2Exc<Ignite, IgniteCache<Integer, Integer>>() {
            @Override public void applyx(Ignite ignite, final IgniteCache<Integer, Integer> cache) throws Exception {
                for (TransactionIsolation isolation : TransactionIsolation.values()) {
                    List<Lock> locks = new ArrayList<>(10);

                    for (int j = 0; j < 10; j++) {
                        cache.put(j, j);

                        Lock lock = cache.lock(j);

                        locks.add(lock);

                        lock.lock();

                        // Re-enter.
                        if (j >= 5) {
                            lock = cache.lock(j);

                            locks.add(lock);

                            lock.lock();
                        }

                        cache.put(j, j);
                    }

                    final Transaction tx = ignite.transactions().txStart(transactionConcurrency(), isolation);

                    for (int j = 10; j < 20; j++)
                        cache.put(j, j);

                    tx.suspend();

                    assertNull(cache.get(10));

                    for (int j = 10; j < 20; j++)
                        assertFalse(cache.lock(j).tryLock(), "Locked key " + j);

                    for (int i = 0; i < 10; i++) {
                        final int key = i;

                        GridTestUtils.runAsync(() -> {
                            tx.resume();

                            cache.put(key + 10, key + 10);
                            cache.put(key + 20, key + 20);

                            tx.suspend();

                            assertFalse(cache.lock(key).tryLock(), "Locked key " + key);
                            assertFalse(cache.lock(key + 10).tryLock(), "Locked key " + (key + 10));
                            assertFalse(cache.lock(key + 20).tryLock(), "Locked key " + (key + 20));

                            cache.put(key + 30, key + 30);

                            Lock lock = cache.lock(key + 30);

                            assertTrue(lock.tryLock(), "Can't lock key " + (key + 30));

                            cache.put(key + 30, key + 30);

                            lock.unlock();

                            cache.put(key + 30, key + 30);
                        }).get(FUT_TIMEOUT);
                    }

                    for (int j = 10; j < 30; j++)
                        assertFalse(cache.lock(j).tryLock(), "Locked key " + j);

                    tx.resume();

                    tx.commit();

                    for (Lock lock : locks)
                        lock.unlock();

                    for (int i = 0; i < 30; i++)
                        assertEquals(i, (int)cache.get(i));

                    GridTestUtils.runAsync(() -> {
                        for (int j = 0; j < 40; j++) {
                            Lock lock = cache.lock(j);

                            assertTrue(lock.tryLock(), "Can't lock key " + j);

                            cache.put(j, j);

                            lock.unlock();

                            cache.put(j, j);
                        }
                    }).get(FUT_TIMEOUT);

                    cache.removeAll();
                }
            }
        });
    }
}
