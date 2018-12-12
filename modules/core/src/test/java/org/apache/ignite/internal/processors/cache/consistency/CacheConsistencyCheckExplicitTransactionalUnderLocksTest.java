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

package org.apache.ignite.internal.processors.cache.consistency;

import java.util.Collection;
import java.util.Map;
import java.util.Set;
import java.util.function.Consumer;
import javax.cache.CacheException;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.cache.CacheEntry;
import org.apache.ignite.internal.util.typedef.T3;
import org.apache.ignite.transactions.Transaction;
import org.apache.ignite.transactions.TransactionConcurrency;
import org.apache.ignite.transactions.TransactionIsolation;
import org.apache.ignite.transactions.TransactionRollbackException;

/**
 *
 */
public class CacheConsistencyCheckExplicitTransactionalUnderLocksTest
    extends CacheConsistencyCheckExplicitTransactionalAbstractTest {
    /** Get and check and fix. */
    private static final Consumer<T3<
        IgniteCache<Integer, Integer> /*initiator's cache*/,
        Map<Integer /*key*/, T3<Map<Ignite, Integer> /*mapping*/, Integer /*primary*/, Integer /*latest*/>>,
        Boolean /*raw*/>> GET_CHECK_AND_FIX =
        (t) -> {
            IgniteCache<Integer, Integer> cache = t.get1();
            Set<Integer> keys = t.get2().keySet();
            Boolean raw = t.get3();

            assert keys.size() == 1;

            for (Map.Entry<Integer, T3<Map<Ignite, Integer>, Integer, Integer>> entry : t.get2().entrySet()) { // Once.
                try {
                    Integer key = entry.getKey();
                    Integer latest = entry.getValue().get3();
                    Integer res;

                    res = raw ?
                        cache.withConsistencyCheck().getEntry(key).getValue() :
                        cache.withConsistencyCheck().get(key);

                    assertEquals(latest, res);
                }
                catch (CacheException e) {
                    fail("Should not happen.");
                }
            }
        };

    /** GetAll and check and fix. */
    private static final Consumer<T3<
        IgniteCache<Integer, Integer> /*initiator's cache*/,
        Map<Integer /*key*/, T3<Map<Ignite, Integer> /*mapping*/, Integer /*primary*/, Integer /*latest*/>>,
        Boolean /*raw*/>> GETALL_CHECK_AND_FIX =
        (t) -> {
            IgniteCache<Integer, Integer> cache = t.get1();
            Set<Integer> keys = t.get2().keySet();
            Boolean raw = t.get3();

            try {
                if (raw) {
                    Collection<CacheEntry<Integer, Integer>> res = cache.withConsistencyCheck().getEntries(keys);

                    for (CacheEntry<Integer, Integer> entry : res)
                        assertEquals(t.get2().get(entry.getKey()).get3(), entry.getValue());
                }
                else {
                    Map<Integer, Integer> res = cache.withConsistencyCheck().getAll(keys);

                    for (Map.Entry<Integer, Integer> entry : res.entrySet())
                        assertEquals(t.get2().get(entry.getKey()).get3(), entry.getValue());
                }
            }
            catch (CacheException e) {
                fail("Should not happen.");
            }
        };

    /** Get and check and make sure it fixed. */
    private static final Consumer<T3<
        IgniteCache<Integer, Integer> /*initiator's cache*/,
        Map<Integer /*key*/, T3<Map<Ignite, Integer> /*mapping*/, Integer /*primary*/, Integer /*latest*/>>,
        Boolean /*raw*/>> GET_AND_CHECK =
        (t) -> {
            IgniteCache<Integer, Integer> cache = t.get1();
            Boolean raw = t.get3();

            for (Map.Entry<Integer, T3<Map<Ignite, Integer>, Integer, Integer>> entry : t.get2().entrySet()) { // Once.
                try {
                    Integer key = entry.getKey();
                    Integer latest = entry.getValue().get3();
                    Integer res;

                    // Regular check.
                    res = raw ?
                        cache.getEntry(key).getValue() :
                        cache.get(key);

                    assertEquals(latest, res);

                    // Consistency check.
                    res = raw ?
                        cache.withConsistencyCheck().getEntry(key).getValue() :
                        cache.withConsistencyCheck().get(key);

                    assertEquals(latest, res);
                }
                catch (CacheException e) {
                    fail("Should not happen.");
                }
            }
        };

    /** {@inheritDoc} */
    @Override public void test() throws Exception {
        test(TransactionConcurrency.PESSIMISTIC, TransactionIsolation.REPEATABLE_READ);
        test(TransactionConcurrency.PESSIMISTIC, TransactionIsolation.SERIALIZABLE);

    }

    /** {@inheritDoc} */
    @Override protected void testGet(Ignite initiator, TransactionConcurrency concurrency,
        TransactionIsolation isolation, boolean raw) throws Exception {
        prepareAndCheck(initiator, 1,
            (T3<IgniteCache<Integer, Integer>, Map<Integer, T3<Map<Ignite, Integer>, Integer, Integer>>, Boolean> t) -> {
                try (Transaction tx = initiator.transactions().txStart(concurrency, isolation)) {
                    GET_CHECK_AND_FIX.accept(t); // Recovery (inside tx).
                    GET_AND_CHECK.accept(t); // Checks (inside tx).

                    try {
                        tx.commit();
                    }
                    catch (TransactionRollbackException e) {
                        fail("Should not happen. " + iterableKey);
                    }
                }

                GET_AND_CHECK.accept(t); // Checks (outside tx).
            }, raw);
    }

    /** {@inheritDoc} */
    @Override protected void testGetAll(Ignite initiator, TransactionConcurrency concurrency,
        TransactionIsolation isolation, Integer cnt, boolean raw) throws Exception {
        prepareAndCheck(initiator, cnt,
            (T3<IgniteCache<Integer, Integer>, Map<Integer, T3<Map<Ignite, Integer>, Integer, Integer>>, Boolean> t) -> {
                try (Transaction tx = initiator.transactions().txStart(concurrency, isolation)) {
                    GETALL_CHECK_AND_FIX.accept(t); // Recovery (inside tx).
                    GET_AND_CHECK.accept(t); // Checks (inside tx).

                    try {
                        tx.commit();
                    }
                    catch (TransactionRollbackException e) {
                        fail("Should not happen. " + iterableKey);
                    }
                }

                GET_AND_CHECK.accept(t); // Checks (outside tx).
            }, raw);
    }
}
