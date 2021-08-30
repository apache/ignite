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

import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.concurrent.CompletionException;
import org.apache.ignite.client.fakes.FakeSchemaRegistry;
import org.apache.ignite.internal.client.table.ClientTuple;
import org.apache.ignite.table.Table;
import org.apache.ignite.table.Tuple;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Table tests.
 */
public class ClientTableTest extends AbstractClientTest {
    private static final String DEFAULT_NAME = "John";

    private static final Long DEFAULT_ID = 123L;

    @Test
    public void testGetWithNullInNotNullableKeyColumnThrowsException() {
        var table = defaultTable();

        var key = Tuple.create().set("name", "123");

        var ex = assertThrows(CompletionException.class, () -> table.get(key));

        assertTrue(ex.getMessage().contains("Failed to set column (null was passed, but column is not nullable)"),
                ex.getMessage());
    }

    @Test
    public void testUpsertGet() {
        var table = defaultTable();
        var tuple = tuple();

        table.upsert(tuple);

        Tuple key = tuple(123L);
        var resTuple = table.get(key);

        assertEquals(DEFAULT_NAME, resTuple.stringValue("name"));
        assertEquals(DEFAULT_ID, resTuple.longValue("id"));
        assertEquals("foo", resTuple.valueOrDefault("bar", "foo"));

        assertEquals(DEFAULT_NAME, resTuple.value(1));
        assertEquals(DEFAULT_ID, resTuple.value(0));

        assertEquals(2, resTuple.columnCount());
        assertEquals("id", resTuple.columnName(0));
        assertEquals("name", resTuple.columnName(1));

        var iter = tuple.iterator();

        assertTrue(iter.hasNext());
        assertEquals(DEFAULT_ID, iter.next());

        assertTrue(iter.hasNext());
        assertEquals(DEFAULT_NAME, iter.next());

        assertFalse(iter.hasNext());
        assertNull(iter.next());

        assertTupleEquals(tuple, resTuple);
    }

    @Test
    public void testUpsertGetAsync() {
        var table = defaultTable();

        var tuple = tuple(42L, "Jack");
        var key = Tuple.create().set("id", 42);

        var resTuple = table.upsertAsync(tuple).thenCompose(t -> table.getAsync(key)).join();

        assertEquals("Jack", resTuple.stringValue("name"));
        assertEquals(42L, resTuple.longValue("id"));
        assertTupleEquals(tuple, resTuple);
    }

    @Test
    @Disabled("https://issues.apache.org/jira/browse/IGNITE-15194")
    public void testGetReturningTupleWithUnknownSchemaRequestsNewSchema() throws Exception {
        FakeSchemaRegistry.setLastVer(2);

        var table = defaultTable();
        Tuple tuple = tuple();
        table.upsert(tuple);

        FakeSchemaRegistry.setLastVer(1);

        try (var client2 = startClient()) {
            Table table2 = client2.tables().table(table.tableName());
            var tuple2 = tuple();
            var resTuple = table2.get(tuple2);

            assertEquals(1, ((ClientTuple)tuple2).schema().version());
            assertEquals(2, ((ClientTuple)resTuple).schema().version());

            assertEquals(DEFAULT_NAME, resTuple.stringValue("name"));
            assertEquals(DEFAULT_ID, resTuple.longValue("id"));
        }
    }

    @Test
    public void testInsert() {
        var table = defaultTable();

        var tuple = tuple();
        var tuple2 = tuple(DEFAULT_ID, "abc");

        assertTrue(table.insert(tuple));
        assertFalse(table.insert(tuple));
        assertFalse(table.insert(tuple2));

        var resTuple = table.get(defaultTupleKey());
        assertTupleEquals(tuple, resTuple);
    }

    @Test
    public void testInsertCustomTuple() {
        var table = defaultTable();
        var tuple = new CustomTuple(25L, "Foo");

        assertTrue(table.insert(tuple));
        assertFalse(table.insert(tuple));

        var resTuple = table.get(new CustomTuple(25L));

        assertTupleEquals(tuple, resTuple);
    }

    @Test
    public void testGetAll() {
        var table = defaultTable();
        table.insert(tuple(1L, "1"));
        table.insert(tuple(2L, "2"));
        table.insert(tuple(3L, "3"));

        List<Tuple> keys = Arrays.asList(tuple(1L), tuple(3L));
        Tuple[] res = sortedTuples(table.getAll(keys));

        assertEquals(2, res.length);

        assertEquals(1L, res[0].longValue("id"));
        assertEquals("1", res[0].stringValue("name"));

        assertEquals(3L, res[1].longValue("id"));
        assertEquals("3", res[1].stringValue("name"));
    }

    @Test
    public void testUpsertAll() {
        var table = defaultTable();

        List<Tuple> data = Arrays.asList(tuple(1L, "1"), tuple(2L, "2"));
        table.upsertAll(data);

        assertEquals("1", table.get(tuple(1L)).stringValue("name"));
        assertEquals("2", table.get(tuple(2L)).stringValue("name"));

        List<Tuple> data2 = Arrays.asList(tuple(1L, "10"), tuple(3L, "30"));
        table.upsertAll(data2);

        assertEquals("10", table.get(tuple(1L)).stringValue("name"));
        assertEquals("2", table.get(tuple(2L)).stringValue("name"));
        assertEquals("30", table.get(tuple(3L)).stringValue("name"));
    }

    @Test
    public void testInsertAll() {
        var table = defaultTable();

        List<Tuple> data = Arrays.asList(tuple(1L, "1"), tuple(2L, "2"));
        var skippedTuples = table.insertAll(data);

        assertEquals(0, skippedTuples.size());
        assertEquals("1", table.get(tuple(1L)).stringValue("name"));
        assertEquals("2", table.get(tuple(2L)).stringValue("name"));

        List<Tuple> data2 = Arrays.asList(tuple(1L, "10"), tuple(3L, "30"));
        var skippedTuples2 = table.insertAll(data2).toArray(new Tuple[0]);

        assertEquals(1, skippedTuples2.length);
        assertEquals(1L, skippedTuples2[0].longValue("id"));
        assertEquals("1", table.get(tuple(1L)).stringValue("name"));
        assertEquals("2", table.get(tuple(2L)).stringValue("name"));
        assertEquals("30", table.get(tuple(3L)).stringValue("name"));
    }
    
    @Test
    public void testReplace() {
        var table = defaultTable();
        table.insert(tuple(1L, "1"));

        assertFalse(table.replace(tuple(3L, "3")));
        assertNull(table.get(tuple(3L)));

        assertTrue(table.replace(tuple(1L, "2")));
        assertEquals("2", table.get(tuple(1L)).value("name"));
    }

    @Test
    public void testReplaceExact() {
        var table = defaultTable();
        table.insert(tuple(1L, "1"));

        assertFalse(table.replace(tuple(3L, "3"), tuple(3L, "4")));
        assertNull(table.get(tuple(3L)));

        assertFalse(table.replace(tuple(1L, "2"), tuple(1L, "3")));
        assertTrue(table.replace(tuple(1L, "1"), tuple(1L, "3")));
        assertEquals("3", table.get(tuple(1L)).value("name"));
    }

    @Test
    public void testGetAndReplace() {
        var table = defaultTable();
        var tuple = tuple(1L, "1");
        table.insert(tuple);

        assertNull(table.getAndReplace(tuple(3L, "3")));
        assertNull(table.get(tuple(3L)));

        var replaceRes = table.getAndReplace(tuple(1L, "2"));
        assertTupleEquals(tuple, replaceRes);
        assertEquals("2", table.get(tuple(1L)).value("name"));
    }

    @Test
    public void testDelete() {
        var table = defaultTable();
        table.insert(tuple(1L, "1"));

        assertFalse(table.delete(tuple(2L)));
        assertTrue(table.delete(tuple(1L)));
        assertNull(table.get(tuple(1L)));
    }

    @Test
    public void testDeleteExact() {
        var table = defaultTable();
        table.insert(tuple(1L, "1"));
        table.insert(tuple(2L, "2"));

        assertFalse(table.deleteExact(tuple(1L)));
        assertFalse(table.deleteExact(tuple(1L, "x")));
        assertTrue(table.deleteExact(tuple(1L, "1")));
        assertFalse(table.deleteExact(tuple(2L)));
        assertFalse(table.deleteExact(tuple(3L)));

        assertNull(table.get(tuple(1L)));
        assertNotNull(table.get(tuple(2L)));
    }

    @Test
    public void testGetAndDelete() {
        var table = defaultTable();
        var tuple = tuple(1L, "1");
        table.insert(tuple);

        var deleted = table.getAndDelete(tuple(1L));

        assertNull(table.getAndDelete(tuple(1L)));
        assertNull(table.getAndDelete(tuple(2L)));
        assertTupleEquals(tuple, deleted);
    }

    @Test
    public void testDeleteAll() {
        var table = defaultTable();

        List<Tuple> data = Arrays.asList(tuple(1L, "1"), tuple(2L, "2"));
        table.insertAll(data);

        List<Tuple> toDelete = Arrays.asList(tuple(1L, "x"), tuple(3L, "y"), tuple(4L, "z"));
        var skippedTuples = sortedTuples(table.deleteAll(toDelete));

        assertEquals(2, skippedTuples.length);
        assertNull(table.get(tuple(1L)));
        assertNotNull(table.get(tuple(2L)));

        assertEquals(3L, skippedTuples[0].longValue("id"));
        assertNull(skippedTuples[0].stringValue("name"));

        assertEquals(4L, skippedTuples[1].longValue("id"));
        assertNull(skippedTuples[1].stringValue("name"));
    }

    @Test
    public void testDeleteAllExact() {
        var table = defaultTable();

        List<Tuple> data = Arrays.asList(tuple(1L, "1"), tuple(2L, "2"));
        table.insertAll(data);

        List<Tuple> toDelete = Arrays.asList(tuple(1L, "1"), tuple(2L, "y"), tuple(3L, "z"));
        var skippedTuples = sortedTuples(table.deleteAllExact(toDelete));

        assertEquals(2, skippedTuples.length);
        assertNull(table.get(tuple(1L)));
        assertNotNull(table.get(tuple(2L)));

        assertEquals(2L, skippedTuples[0].longValue("id"));
        assertEquals("y", skippedTuples[0].stringValue("name"));

        assertEquals(3L, skippedTuples[1].longValue("id"));
        assertEquals("z", skippedTuples[1].stringValue("name"));
    }

    private static Tuple[] sortedTuples(Collection<Tuple> tuples) {
        Tuple[] res = tuples.toArray(new Tuple[0]);

        Arrays.sort(res, (x, y) -> (int) (x.longValue(0) - y.longValue(0)));

        return res;
    }

    private Tuple tuple() {
        return Tuple.create()
                .set("id", DEFAULT_ID)
                .set("name", DEFAULT_NAME);
    }

    private Tuple tuple(Long id) {
        return Tuple.create()
                .set("id", id);
    }

    private Tuple tuple(Long id, String name) {
        return Tuple.create()
                .set("id", id)
                .set("name", name);
    }

    private Tuple defaultTupleKey() {
        return Tuple.create()
                .set("id", DEFAULT_ID);
    }

    private Table defaultTable() {
        server.tables().getOrCreateTable(DEFAULT_TABLE, tbl -> tbl.changeReplicas(1));

        return client.tables().table(DEFAULT_TABLE);
    }
}
