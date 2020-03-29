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

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import org.apache.ignite.Ignite;
import org.apache.ignite.transactions.Transaction;
import org.apache.ignite.transactions.TransactionConcurrency;
import org.apache.ignite.transactions.TransactionIsolation;
import org.apache.ignite.transactions.TransactionRollbackException;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

/**
 *
 */
@RunWith(Parameterized.class)
public class ExplicitTransactionalReadRepairTest extends AbstractFullSetReadRepairTest {
    /** Test parameters. */
    @Parameterized.Parameters(name = "concurrency={0}, isolation={1}, getEntry={2}, async={3}")
    public static Collection parameters() {
        List<Object[]> res = new ArrayList<>();

        for (TransactionConcurrency concurrency : TransactionConcurrency.values()) {
            for (TransactionIsolation isolation : TransactionIsolation.values()) {
                for (boolean raw : new boolean[] {false, true}) {
                    for (boolean async : new boolean[] {false, true})
                        res.add(new Object[] {concurrency, isolation, raw, async});
                }
            }
        }

        return res;
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

    /** Async. */
    @Parameterized.Parameter(3)
    public boolean async;

    /** {@inheritDoc} */
    @Override protected void testGet(Ignite initiator, Integer cnt, boolean all) throws Exception {
        prepareAndCheck(
            initiator,
            cnt,
            raw,
            async,
            (ReadRepairData data) -> {
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

    /** {@inheritDoc} */
    @Override protected void testGetNull(Ignite initiator) throws Exception {
        prepareAndCheck(
            initiator,
            1,
            raw,
            async,
            (ReadRepairData data) -> {
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

    /** {@inheritDoc} */
    @Override protected void testContains(Ignite initiator, Integer cnt, boolean all) throws Exception {
        prepareAndCheck(
            initiator,
            cnt,
            raw,
            async,
            (ReadRepairData data) -> {
                try (Transaction tx = initiator.transactions().txStart(concurrency, isolation)) {
                    // Recovery (inside tx).
                    if (all)
                        CONTAINS_ALL_CHECK_AND_FIX.accept(data);
                    else
                        CONTAINS_CHECK_AND_FIX.accept(data);

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
}
