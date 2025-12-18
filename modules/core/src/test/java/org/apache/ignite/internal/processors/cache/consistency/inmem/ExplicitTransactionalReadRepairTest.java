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

import java.util.List;
import java.util.function.Consumer;
import java.util.stream.Stream;

import org.apache.ignite.Ignite;
import org.apache.ignite.internal.processors.cache.consistency.AbstractFullSetReadRepairTest;
import org.apache.ignite.internal.processors.cache.consistency.ReadRepairDataGenerator.ReadRepairData;
import org.apache.ignite.testframework.GridTestUtils;
import org.apache.ignite.transactions.Transaction;
import org.apache.ignite.transactions.TransactionConcurrency;
import org.apache.ignite.transactions.TransactionIsolation;
import org.apache.ignite.transactions.TransactionRollbackException;
import org.junit.jupiter.params.Parameter;
import org.junit.jupiter.params.ParameterizedClass;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

/**
 *
 */
@ParameterizedClass(name = "concurrency={0}, isolation={1}, getEntry={2}, async={3}, misses={4}, nulls={5}, binary={6}")
@MethodSource("allTypesArgs")
public class ExplicitTransactionalReadRepairTest extends AbstractFullSetReadRepairTest {
    /** Test parameters. */
    private static Stream<Arguments> allTypesArgs() {
        return GridTestUtils.cartesianProduct(
                List.of(TransactionConcurrency.values()),
                List.of(TransactionIsolation.values()),
                List.of(true, false), // raw
                List.of(true, false), // async
                List.of(true, false), // misses
                List.of(true, false), // nulls
                List.of(true, false)  // binary
        ).stream().map(Arguments::of);
    }

    /** Concurrency. */
    @Parameter(0)
    public TransactionConcurrency concurrency;

    /** Isolation. */
    @Parameter(1)
    public TransactionIsolation isolation;

    /** GetEntry or just get. */
    @Parameter(2)
    public boolean raw;

    /** Async. */
    @Parameter(3)
    public boolean async;

    /** Misses. */
    @Parameter(4)
    public boolean misses;

    /** Nulls. */
    @Parameter(5)
    public boolean nulls;

    /** With binary. */
    @Parameter(6)
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
