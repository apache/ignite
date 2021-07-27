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

package org.apache.ignite.client;

import java.util.concurrent.CompletionException;

import org.apache.ignite.table.Table;
import org.apache.ignite.table.Tuple;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Table tests.
 */
public class ClientTableTest extends AbstractClientTest {
    @Test
    public void testGetWithNullInNotNullableKeyColumnThrowsException() {
        Table table = getDefaultTable();

        var key = table.tupleBuilder().set("name", "123").build();

        var ex = assertThrows(CompletionException.class, () -> table.get(key));

        assertTrue(ex.getMessage().contains("Failed to set column (null was passed, but column is not nullable)"),
                ex.getMessage());
    }

    @Test
    public void testUpsertGet() {
        Table table = getDefaultTable();

        var tuple = table.tupleBuilder()
                .set("id", 123L)
                .set("name", "John")
                .build();

        table.upsert(tuple);

        Tuple key = table.tupleBuilder().set("id", 123).build();
        var resTuple = table.get(key);

        assertEquals("John", resTuple.stringValue("name"));
        assertEquals(123L, resTuple.longValue("id"));
        assertEquals("foo", resTuple.valueOrDefault("bar", "foo"));

        assertEquals("John", resTuple.value(1));
        assertEquals(123L, (Long) resTuple.value(0));

        assertEquals(2, resTuple.columnCount());
        assertEquals("id", resTuple.columnName(0));
        assertEquals("name", resTuple.columnName(1));

        var iter = tuple.iterator();

        assertTrue(iter.hasNext());
        assertEquals(123L, iter.next());

        assertTrue(iter.hasNext());
        assertEquals("John", iter.next());

        assertFalse(iter.hasNext());
        assertNull(iter.next());

        assertTupleEquals(tuple, resTuple);
    }

    @Test
    public void testUpsertGetAsync() {
        Table table = getDefaultTable();

        var tuple = table.tupleBuilder()
                .set("id", 42L)
                .set("name", "Jack")
                .build();

        Tuple key = table.tupleBuilder().set("id", 42).build();

        var resTuple = table.upsertAsync(tuple).thenCompose(t -> table.getAsync(key)).join();

        assertEquals("Jack", resTuple.stringValue("name"));
        assertEquals(42L, resTuple.longValue("id"));
        assertTupleEquals(tuple, resTuple);
    }

    private Table getDefaultTable() {
        server.tables().getOrCreateTable(DEFAULT_TABLE, tbl -> tbl.changeReplicas(1));

        return client.tables().table(DEFAULT_TABLE);
    }
}
