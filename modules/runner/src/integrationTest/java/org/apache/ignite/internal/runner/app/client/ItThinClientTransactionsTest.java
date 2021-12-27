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

package org.apache.ignite.internal.runner.app.client;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.startsWith;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.CompletableFuture;
import org.apache.ignite.client.IgniteClient;
import org.apache.ignite.client.IgniteClientException;
import org.apache.ignite.table.KeyValueView;
import org.apache.ignite.table.RecordView;
import org.apache.ignite.table.Table;
import org.apache.ignite.table.Tuple;
import org.apache.ignite.table.mapper.Mapper;
import org.apache.ignite.tx.Transaction;
import org.apache.ignite.tx.TransactionException;
import org.junit.jupiter.api.Test;

/**
 * Thin client transactions integration test.
 */
public class ItThinClientTransactionsTest extends ItAbstractThinClientTest {
    @Test
    void testKvViewOperations() {
        KeyValueView<Integer, String> kvView = kvView();

        int key = 1;
        kvView.put(null, key, "1");

        Transaction tx = client().transactions().begin();
        kvView.put(tx, key, "22");

        assertTrue(kvView.contains(tx, key));
        assertFalse(kvView.remove(tx, key, "1"));
        assertFalse(kvView.putIfAbsent(tx, key, "111"));
        assertEquals("22", kvView.get(tx, key));
        assertEquals("22", kvView.getAndPut(tx, key, "33"));
        assertEquals("33", kvView.getAndReplace(tx, key, "44"));
        assertTrue(kvView.replace(tx, key, "55"));
        assertEquals("55", kvView.getAndRemove(tx, key));
        assertFalse(kvView.contains(tx, key));
        assertFalse(kvView.remove(tx, key));

        kvView.putAll(tx, Map.of(key, "6", 2, "7"));
        assertEquals(2, kvView.getAll(tx, List.of(key, 2, 3)).size());

        tx.rollback();
        assertEquals("1", kvView.get(null, key));
    }

    @Test
    void testKvViewBinaryOperations() {
        KeyValueView<Tuple, Tuple> kvView = table().keyValueView();

        Tuple key = key(1);
        kvView.put(null, key, val("1"));

        Transaction tx = client().transactions().begin();
        kvView.put(tx, key, val("22"));

        assertTrue(kvView.contains(tx, key));
        assertFalse(kvView.remove(tx, key, val("1")));
        assertFalse(kvView.putIfAbsent(tx, key, val("111")));
        assertEquals(val("22"), kvView.get(tx, key));
        assertEquals(val("22"), kvView.getAndPut(tx, key, val("33")));
        assertEquals(val("33"), kvView.getAndReplace(tx, key, val("44")));
        assertTrue(kvView.replace(tx, key, val("55")));
        assertEquals(val("55"), kvView.getAndRemove(tx, key));
        assertFalse(kvView.contains(tx, key));
        assertFalse(kvView.remove(tx, key));

        kvView.putAll(tx, Map.of(key, val("6"), key(2), val("7")));
        assertEquals(2, kvView.getAll(tx, List.of(key, key(2), key(3))).size());

        tx.rollback();
        assertEquals(val("1"), kvView.get(null, key));
    }

    @Test
    void testRecordViewOperations() {
        RecordView<Rec> recordView = table().recordView(Mapper.of(Rec.class));
        Rec key = rec(1, null);
        recordView.upsert(null, rec(1, "1"));

        Transaction tx = client().transactions().begin();
        recordView.upsert(tx, rec(1, "22"));

        assertFalse(recordView.deleteExact(tx, rec(1, "1")));
        assertFalse(recordView.insert(tx, rec(1, "111")));
        assertEquals(rec(1, "22"), recordView.get(tx, key));
        assertEquals(rec(1, "22"), recordView.getAndUpsert(tx, rec(1, "33")));
        assertEquals(rec(1, "33"), recordView.getAndReplace(tx, rec(1, "44")));
        assertTrue(recordView.replace(tx, rec(1, "55")));
        assertEquals(rec(1, "55"), recordView.getAndDelete(tx, key));
        assertFalse(recordView.delete(tx, key));

        recordView.upsertAll(tx, List.of(rec(1, "6"), rec(2, "7")));
        assertEquals(2, recordView.getAll(tx, List.of(key, rec(2, null), rec(3, null))).size());

        tx.rollback();
        assertEquals(rec(1, "1"), recordView.get(null, key));
    }

    @Test
    void testRecordViewBinaryOperations() {
        RecordView<Tuple> recordView = table().recordView();

        Tuple key = key(1);
        recordView.upsert(null, kv(1, "1"));

        Transaction tx = client().transactions().begin();
        recordView.upsert(tx, kv(1, "22"));

        assertFalse(recordView.deleteExact(tx, kv(1, "1")));
        assertFalse(recordView.insert(tx, kv(1, "111")));
        assertEquals(kv(1, "22"), recordView.get(tx, key));
        assertEquals(kv(1, "22"), recordView.getAndUpsert(tx, kv(1, "33")));
        assertEquals(kv(1, "33"), recordView.getAndReplace(tx, kv(1, "44")));
        assertTrue(recordView.replace(tx, kv(1, "55")));
        assertEquals(kv(1, "55"), recordView.getAndDelete(tx, key));
        assertFalse(recordView.delete(tx, key));

        recordView.upsertAll(tx, List.of(kv(1, "6"), kv(2, "7")));
        assertEquals(2, recordView.getAll(tx, List.of(key, key(2), key(3))).size());

        tx.rollback();
        assertEquals(kv(1, "1"), recordView.get(null, key));
    }

    @Test
    void testCommitUpdatesData() {
        KeyValueView<Integer, String> kvView = kvView();
        kvView.put(null, 1, "1");

        Transaction tx = client().transactions().begin();
        assertEquals("1", kvView.get(null, 1));
        assertEquals("1", kvView.get(tx, 1));

        kvView.put(tx, 1, "2");
        assertEquals("2", kvView.get(tx, 1));

        tx.commit();
        assertEquals("2", kvView.get(null, 1));
    }

    @Test
    void testRollbackDoesNotUpdateData() {
        KeyValueView<Integer, String> kvView = kvView();
        kvView.put(null, 1, "1");

        Transaction tx = client().transactions().begin();
        assertEquals("1", kvView.get(null, 1));
        assertEquals("1", kvView.get(tx, 1));

        kvView.put(tx, 1, "2");
        assertEquals("2", kvView.get(tx, 1));

        tx.rollback();
        assertEquals("1", kvView.get(null, 1));
    }

    @Test
    void testAccessLockedKeyTimesOut() {
        KeyValueView<Integer, String> kvView = kvView();

        Transaction tx = client().transactions().begin();
        kvView.put(tx, -100, "1");

        IgniteClientException ex = assertThrows(IgniteClientException.class, () -> kvView.get(null, -100));
        assertThat(ex.getMessage(), containsString("TimeoutException"));

        tx.rollback();
    }

    @Test
    void testCommitRollbackSameTxThrows() {
        Transaction tx = client().transactions().begin();
        tx.commit();

        TransactionException ex = assertThrows(TransactionException.class, tx::rollback);
        assertEquals("Transaction is already committed.", ex.getMessage());
    }

    @Test
    void testRollbackCommitSameTxThrows() {
        Transaction tx = client().transactions().begin();
        tx.rollback();

        TransactionException ex = assertThrows(TransactionException.class, tx::commit);
        assertEquals("Transaction is already rolled back.", ex.getMessage());
    }

    @Test
    void testCustomTransactionInterfaceThrows() {
        var tx = new Transaction() {
            @Override
            public void commit() throws TransactionException {
            }

            @Override
            public CompletableFuture<Void> commitAsync() {
                return null;
            }

            @Override
            public void rollback() throws TransactionException {
            }

            @Override
            public CompletableFuture<Void> rollbackAsync() {
                return null;
            }
        };

        var ex = assertThrows(IgniteClientException.class, () -> kvView().put(tx, 1, "1"));

        String expected = "Unsupported transaction implementation: "
                + "'class org.apache.ignite.internal.runner.app.client.ItThinClientTransactionsTest";

        assertThat(ex.getMessage(), startsWith(expected));
    }

    @Test
    void testTransactionFromAnotherChannelThrows() throws Exception {
        Transaction tx = client().transactions().begin();

        try (IgniteClient client2 = IgniteClient.builder().addresses(getNodeAddress()).build()) {
            RecordView<Tuple> recordView = client2.tables().tables().get(0).recordView();

            IgniteClientException ex = assertThrows(IgniteClientException.class, () -> recordView.upsert(tx, Tuple.create()));

            assertEquals("Transaction context has been lost due to connection errors.", ex.getMessage());
        }
    }

    private KeyValueView<Integer, String> kvView() {
        return table().keyValueView(Mapper.of(Integer.class), Mapper.of(String.class));
    }

    private Table table() {
        return client().tables().tables().get(0);
    }

    private Tuple val(String v) {
        return Tuple.create().set(COLUMN_VAL, v);
    }

    private Tuple key(Integer k) {
        return Tuple.create().set(COLUMN_KEY, k);
    }

    private Tuple kv(Integer k, String v) {
        return Tuple.create().set(COLUMN_KEY, k).set(COLUMN_VAL, v);
    }

    private Rec rec(int key, String val) {
        var r = new Rec();

        r.key = key;
        r.val = val;

        return r;
    }

    private static class Rec {
        public int key;
        public String val;

        @Override
        public boolean equals(Object o) {
            if (this == o) {
                return true;
            }
            if (o == null || getClass() != o.getClass()) {
                return false;
            }
            Rec rec = (Rec) o;
            return key == rec.key && Objects.equals(val, rec.val);
        }

        @Override
        public int hashCode() {
            return Objects.hash(key, val);
        }
    }
}
