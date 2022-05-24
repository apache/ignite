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

package org.apache.ignite.internal.processors.cache.consistency.inmem;

import java.util.ArrayList;
import java.util.List;
import java.util.function.Consumer;
import org.apache.ignite.Ignite;
import org.apache.ignite.internal.processors.cache.consistency.AbstractFullSetReadRepairTest;
import org.apache.ignite.internal.processors.cache.consistency.ReadRepairDataGenerator.ReadRepairData;
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
    @Parameterized.Parameters(name = "concurrency={0}, isolation={1}, getEntry={2}, async={3}, misses={4}, nulls={5}, binary={6}")
    public static Iterable<Object[]> parameters() {
        List<Object[]> res = new ArrayList<>();

        for (TransactionConcurrency concurrency : TransactionConcurrency.values()) {
            for (TransactionIsolation isolation : TransactionIsolation.values()) {
                for (boolean raw : new boolean[] {false, true}) {
                    for (boolean async : new boolean[] {false, true}) {
                        for (boolean misses : new boolean[] {false, true}) {
                            for (boolean nulls : new boolean[] {false, true}) {
                                for (boolean binary : new boolean[] {false, true})
                                    res.add(new Object[] {concurrency, isolation, raw, async, misses, nulls, binary});
                            }
                        }
                    }
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

    /** Misses. */
    @Parameterized.Parameter(4)
    public boolean misses;

    /** Nulls. */
    @Parameterized.Parameter(5)
    public boolean nulls;

    /** With binary. */
    @Parameterized.Parameter(6)
    public boolean binary;

    /** {@inheritDoc} */
    @Override protected void testGet(Ignite initiator, int cnt, boolean all) throws Exception {
        generateAndCheck(
            initiator,
            cnt,
            raw,
            async,
            misses,
            nulls,
            binary,
            (ReadRepairData rrd) -> repairIfRepairable.accept(rrd, () -> {
                boolean repairByOtherTx = concurrency == TransactionConcurrency.OPTIMISTIC ||
                    isolation == TransactionIsolation.READ_COMMITTED;

                testReadRepair(initiator, rrd, all ? GETALL_CHECK_AND_REPAIR : GET_CHECK_AND_REPAIR, repairByOtherTx, true);
            }));
    }

    /** {@inheritDoc} */
    @Override protected void testContains(Ignite initiator, int cnt, boolean all) throws Exception {
        generateAndCheck(
            initiator,
            cnt,
            raw,
            async,
            misses,
            nulls,
            binary,
            (ReadRepairData rrd) -> repairIfRepairable.accept(rrd, () -> {
                // "Contains" works like optimistic() || readCommitted() and always repaired by other tx.
                testReadRepair(initiator, rrd, all ? CONTAINS_ALL_CHECK_AND_REPAIR : CONTAINS_CHECK_AND_REPAIR, true, true);
            }));
    }

    /** {@inheritDoc} */
    @Override protected void testGetNull(Ignite initiator, int cnt, boolean all) throws Exception {
        generateAndCheck(
            initiator,
            cnt,
            raw,
            async,
            misses,
            nulls,
            binary,
            (ReadRepairData rrd) -> testReadRepair(initiator, rrd, all ? GET_ALL_NULL : GET_NULL, false, false));
    }

    /**
     *
     */
    private void testReadRepair(Ignite initiator, ReadRepairData rrd, Consumer<ReadRepairData> readOp,
        boolean repairByOtherTx, boolean hit) {
        try (Transaction tx = initiator.transactions().txStart(concurrency, isolation)) {
            // Recovery (inside tx).
            readOp.accept(rrd);

            if (hit) // Checks (inside tx).
                check(rrd, null, repairByOtherTx); // Hit.
            else
                checkEventMissed(); // Miss.

            try {
                tx.commit();
            }
            catch (TransactionRollbackException e) {
                fail("Should not happen. " + e);
            }
        }

        if (hit) // Checks (outside tx).
            check(rrd, null, !repairByOtherTx); // Hit.
        else
            checkEventMissed(); // Miss.
    }
}
