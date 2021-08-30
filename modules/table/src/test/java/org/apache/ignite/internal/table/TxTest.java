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

package org.apache.ignite.internal.table;

import java.util.concurrent.CompletableFuture;
import java.util.function.Consumer;
import org.apache.ignite.internal.schema.Column;
import org.apache.ignite.internal.schema.NativeTypes;
import org.apache.ignite.internal.schema.SchemaDescriptor;
import org.apache.ignite.internal.table.impl.DummyInternalTableImpl;
import org.apache.ignite.internal.table.impl.DummySchemaManagerImpl;
import org.apache.ignite.internal.util.Pair;
import org.apache.ignite.table.KeyValueBinaryView;
import org.apache.ignite.table.Table;
import org.apache.ignite.table.Tuple;
import org.apache.ignite.tx.IgniteTransactions;
import org.apache.ignite.tx.Transaction;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.junit.jupiter.MockitoExtension;
import org.mockito.junit.jupiter.MockitoSettings;
import org.mockito.quality.Strictness;

import static java.util.concurrent.CompletableFuture.allOf;
import static org.junit.jupiter.api.Assertions.assertEquals;

/** */
@ExtendWith(MockitoExtension.class)
@MockitoSettings(strictness = Strictness.LENIENT)
public class TxTest {
    /** Table ID test value. */
    public final java.util.UUID tableId = java.util.UUID.randomUUID();

    /** Accounts table. */
    private Table accounts;

    /** */
    public static final double BALANCE_1 = 500;

    /** */
    public static final double BALANCE_2 = 500;

    /** */
    public static final double DELTA = 500;

    /** */
    @Mock
    private IgniteTransactions igniteTransactions;

    /** */
    @Mock
    private Transaction tx;

    /**
     * Initialize the test state.
     */
    @BeforeEach
    public void before() {
        SchemaDescriptor schema = new SchemaDescriptor(
            tableId,
            1,
            new Column[]{new Column("accountNumber", NativeTypes.INT64, false)},
            new Column[]{new Column("balance", NativeTypes.DOUBLE, false)}
        );

        accounts = new TableImpl(new DummyInternalTableImpl(), new DummySchemaManagerImpl(schema), null, null);
        Tuple r1 = Tuple.create().set("accountNumber", 1L).set("balance", BALANCE_1);
        Tuple r2 = Tuple.create().set("accountNumber", 2L).set("balance", BALANCE_2);

        accounts.insert(r1);
        accounts.insert(r2);

        Mockito.doAnswer(invocation -> {
            Consumer<Transaction> argument = invocation.getArgument(0);

            argument.accept(tx);

            return null;
        }).when(igniteTransactions).runInTransaction(Mockito.any());

        Mockito.when(igniteTransactions.beginAsync()).thenReturn(CompletableFuture.completedFuture(tx));
    }

    /**
     * Tests a synchronous transaction.
     */
    @Test
    public void testTxSync() {
        igniteTransactions.runInTransaction(tx -> {
            Table txAcc = accounts.withTransaction(tx);

            CompletableFuture<Tuple> read1 = txAcc.getAsync(makeKey(1));
            CompletableFuture<Tuple> read2 = txAcc.getAsync(makeKey(2));

            txAcc.upsertAsync(makeRecord(1, read1.join().doubleValue("balance") - DELTA));
            txAcc.upsertAsync(makeRecord(2, read2.join().doubleValue("balance") + DELTA));

            tx.commit(); // Not necessary to wait for async ops expicitly before the commit.
        });

        Mockito.verify(tx).commit();

        assertEquals(BALANCE_1 - DELTA, accounts.get(makeKey(1)).doubleValue("balance"));
        assertEquals(BALANCE_2 + DELTA, accounts.get(makeKey(2)).doubleValue("balance"));
    }

    /**
     * Tests a synchronous transaction over key-value view.
     */
    @Test
    public void testTxSyncKeyValue() {
        igniteTransactions.runInTransaction(tx -> {
            KeyValueBinaryView txAcc = accounts.kvView().withTransaction(tx);

            CompletableFuture<Tuple> read1 = txAcc.getAsync(makeKey(1));
            CompletableFuture<Tuple> read2 = txAcc.getAsync(makeKey(2));

            txAcc.putAsync(makeKey(1), makeValue(read1.join().doubleValue("balance") - DELTA));
            txAcc.putAsync(makeKey(2), makeValue(read2.join().doubleValue("balance") + DELTA));

            tx.commit(); // Not necessary to wait for async ops expicitly before the commit.
        });

        Mockito.verify(tx).commit();

        assertEquals(BALANCE_1 - DELTA, accounts.get(makeKey(1)).doubleValue("balance"));
        assertEquals(BALANCE_2 + DELTA, accounts.get(makeKey(2)).doubleValue("balance"));
    }

    /**
     * Tests an asynchronous transaction.
     */
    @Test
    public void testTxAsync() {
        igniteTransactions.beginAsync().thenApply(tx -> accounts.withTransaction(tx)).
            thenCompose(txAcc -> txAcc.getAsync(makeKey(1))
            .thenCombine(txAcc.getAsync(makeKey(2)), (v1, v2) -> new Pair<>(v1, v2))
            .thenCompose(pair -> allOf(
                txAcc.upsertAsync(makeRecord(1, pair.getFirst().doubleValue("balance") - DELTA)),
                txAcc.upsertAsync(makeRecord(2, pair.getSecond().doubleValue("balance") + DELTA))
                )
            )
            .thenApply(ignore -> txAcc.transaction())
        ).thenCompose(Transaction::commitAsync);

        Mockito.verify(tx).commitAsync();

        assertEquals(BALANCE_1 - DELTA, accounts.get(makeKey(1)).doubleValue("balance"));
        assertEquals(BALANCE_2 + DELTA, accounts.get(makeKey(2)).doubleValue("balance"));
    }

    /**
     * Tests an asynchronous transaction over key-value view.
     */
    @Test
    public void testTxAsyncKeyValue() {
        igniteTransactions.beginAsync().thenApply(tx -> accounts.kvView().withTransaction(tx)).
            thenCompose(txAcc -> txAcc.getAsync(makeKey(1))
            .thenCombine(txAcc.getAsync(makeKey(2)), (v1, v2) -> new Pair<>(v1, v2))
            .thenCompose(pair -> allOf(
                txAcc.putAsync(makeKey(1), makeValue(pair.getFirst().doubleValue("balance") - DELTA)),
                txAcc.putAsync(makeKey(2), makeValue(pair.getSecond().doubleValue("balance") + DELTA))
                )
            )
            .thenApply(ignore -> txAcc.transaction())
        ).thenCompose(Transaction::commitAsync);

        Mockito.verify(tx).commitAsync();

        assertEquals(BALANCE_1 - DELTA, accounts.get(makeKey(1)).doubleValue("balance"));
        assertEquals(BALANCE_2 + DELTA, accounts.get(makeKey(2)).doubleValue("balance"));
    }

    /**
     * @param id The id.
     * @return The key tuple.
     */
    private Tuple makeKey(long id) {
        return Tuple.create().set("accountNumber", id);
    }

    /**
     * @param id The id.
     * @param balance The balance.
     * @return The value tuple.
     */
    private Tuple makeRecord(long id, double balance) {
        return Tuple.create().set("accountNumber", id).set("balance", balance);
    }

    /**
     * @param balance The balance.
     * @return The value tuple.
     */
    private Tuple makeValue(double balance) {
        return Tuple.create().set("balance", balance);
    }
}
