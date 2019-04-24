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

import java.util.Arrays;
import java.util.Collection;
import org.apache.ignite.Ignite;
import org.apache.ignite.internal.util.typedef.G;
import org.apache.ignite.transactions.Transaction;
import org.apache.ignite.transactions.TransactionConcurrency;
import org.apache.ignite.transactions.TransactionIsolation;
import org.apache.ignite.transactions.TransactionRollbackException;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

/**
 *
 */
@RunWith(Parameterized.class)
public class ExplicitTransactionalCacheConsistencyTest extends AbstractCacheConsistencyTest {
    /** Test parameters. */
    @Parameterized.Parameters(name = "concurrency={0}, isolation={1}, getEntry={2}")
    public static Collection parameters() {
        return Arrays.asList(new Object[][] {
            {TransactionConcurrency.OPTIMISTIC, TransactionIsolation.READ_COMMITTED, true},
            {TransactionConcurrency.OPTIMISTIC, TransactionIsolation.REPEATABLE_READ, true},
            {TransactionConcurrency.OPTIMISTIC, TransactionIsolation.SERIALIZABLE, true},

            {TransactionConcurrency.PESSIMISTIC, TransactionIsolation.READ_COMMITTED, true},
            {TransactionConcurrency.PESSIMISTIC, TransactionIsolation.REPEATABLE_READ, true},
            {TransactionConcurrency.PESSIMISTIC, TransactionIsolation.SERIALIZABLE, true},

            {TransactionConcurrency.OPTIMISTIC, TransactionIsolation.READ_COMMITTED, false},
            {TransactionConcurrency.OPTIMISTIC, TransactionIsolation.REPEATABLE_READ, false},
            {TransactionConcurrency.OPTIMISTIC, TransactionIsolation.SERIALIZABLE, false},

            {TransactionConcurrency.PESSIMISTIC, TransactionIsolation.READ_COMMITTED, false},
            {TransactionConcurrency.PESSIMISTIC, TransactionIsolation.REPEATABLE_READ, false},
            {TransactionConcurrency.PESSIMISTIC, TransactionIsolation.SERIALIZABLE, false}
        });
    }

    /** Concurrency. */
    @Parameterized.Parameter
    public TransactionConcurrency concurrency;

    /** Isolation. */
    @Parameterized.Parameter(1)
    public TransactionIsolation isolation;

    /** GetEntry or just get. */
    @Parameterized.Parameter(2)
    public boolean raw;

    /**
     *
     */
    @Test
    public void test() throws Exception {
        for (Ignite node : G.allGrids()) {
            testGet(node, concurrency, isolation, 1, raw, false);
            testGetAllVariations(node, concurrency, isolation, raw);
            testGetNull(node, concurrency, isolation, 1, raw);
        }
    }

    /**
     *
     */
    protected void testGet(
        Ignite initiator,
        TransactionConcurrency concurrency,
        TransactionIsolation isolation,
        Integer cnt,
        boolean raw,
        boolean all) throws Exception {
        prepareAndCheck(
            initiator,
            cnt,
            raw,
            (ConsistencyRecoveryData data) -> {
                try (Transaction tx = initiator.transactions().txStart(concurrency, isolation)) {
                    // Recovery (inside tx).
                    if (all)
                        GETALL_CHECK_AND_FIX.accept(data);
                    else
                        GET_CHECK_AND_FIX.accept(data);

                    ENSURE_FIXED.accept(data); // Checks (inside tx).

                    try {
                        tx.commit();
                    }
                    catch (TransactionRollbackException e) {
                        fail("Should not happen. " + e);
                    }
                }

                ENSURE_FIXED.accept(data); // Checks (outside tx).
            });
    }

    /**
     *
     */
    private void testGetAllVariations(
        Ignite initiator,
        TransactionConcurrency concurrency,
        TransactionIsolation isolation,
        boolean raw) throws Exception {
        testGet(initiator, concurrency, isolation, 1, raw, true); // 1 (all keys available at primary)
        testGet(initiator, concurrency, isolation, 2, raw, true); // less than backups
        testGet(initiator, concurrency, isolation, 3, raw, true); // equals to backups
        testGet(initiator, concurrency, isolation, 4, raw, true); // equals to backups + primary
        testGet(initiator, concurrency, isolation, 10, raw, true); // more than backups
    }

    /**
     *
     */
    private void testGetNull(Ignite initiator,
        TransactionConcurrency concurrency,
        TransactionIsolation isolation,
        Integer cnt,
        boolean raw) throws Exception {
        prepareAndCheck(
            initiator,
            cnt,
            raw,
            (ConsistencyRecoveryData data) -> {
                try (Transaction tx = initiator.transactions().txStart(concurrency, isolation)) {
                    GET_NULL.accept(data);

                    try {
                        tx.commit();
                    }
                    catch (TransactionRollbackException e) {
                        fail("Should not happen. " + e);
                    }
                }

                GET_NULL.accept(data); // Checks (outside tx).
            });
    }
}
