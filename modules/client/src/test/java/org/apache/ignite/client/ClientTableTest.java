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

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.Arrays;
import java.util.List;
import org.apache.ignite.client.fakes.FakeSchemaRegistry;
import org.apache.ignite.internal.client.table.ClientTuple;
import org.apache.ignite.table.RecordView;
import org.apache.ignite.table.Tuple;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;

/**
 * Table tests.
 */
@SuppressWarnings("ZeroLengthArrayAllocation")
public class ClientTableTest extends AbstractClientTableTest {
    @Test
    public void testGetWithMissedKeyColumnThrowsException() {
        var table = defaultTable().recordView();

        var key = Tuple.create().set("name", "123");

        var ex = assertThrows(IgniteClientException.class, () -> table.get(null, key));

        assertTrue(ex.getMessage().contains("Missed key column: ID"),
                ex.getMessage());
    }

    @Test
    public void testUpsertGet() {
        var table = defaultTable().recordView();
        var tuple = tuple();

        table.upsert(null, tuple);

        Tuple key = tuple(DEFAULT_ID);
        var resTuple = table.get(null, key);

        assertEquals(DEFAULT_NAME, resTuple.stringValue("name"));
        assertEquals(DEFAULT_ID, resTuple.longValue("id"));
        assertEquals("foo", resTuple.valueOrDefault("bar", "foo"));

        assertEquals(DEFAULT_NAME, resTuple.value(1));
        assertEquals(DEFAULT_ID, resTuple.value(0));

        assertEquals(2, resTuple.columnCount());
        assertEquals("ID", resTuple.columnName(0));
        assertEquals("NAME", resTuple.columnName(1));

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
        var table = defaultTable().recordView();

        var tuple = tuple(42L, "Jack");
        var key = Tuple.create().set("id", 42L);

        var resTuple = table.upsertAsync(null, tuple).thenCompose(t -> table.getAsync(null, key)).join();

        assertEquals("Jack", resTuple.stringValue("name"));
        assertEquals(42L, resTuple.longValue("id"));
        assertTupleEquals(tuple, resTuple);
    }

    @Test
    @Disabled("https://issues.apache.org/jira/browse/IGNITE-15194")
    public void testGetReturningTupleWithUnknownSchemaRequestsNewSchema() throws Exception {
        FakeSchemaRegistry.setLastVer(2);

        var table = defaultTable();
        var recView = table.recordView();
        Tuple tuple = tuple();
        recView.upsert(null, tuple);

        FakeSchemaRegistry.setLastVer(1);

        try (var client2 = startClient()) {
            RecordView<Tuple> table2 = client2.tables().table(table.name()).recordView();
            var tuple2 = tuple();
            var resTuple = table2.get(null, tuple2);

            assertEquals(1, ((ClientTuple) tuple2).schema().version());
            assertEquals(2, ((ClientTuple) resTuple).schema().version());

            assertEquals(DEFAULT_NAME, resTuple.stringValue("name"));
            assertEquals(DEFAULT_ID, resTuple.longValue("id"));
        }
    }

    @Test
    public void testInsert() {
        var table = defaultTable().recordView();

        var tuple = tuple();
        var tuple2 = tuple(DEFAULT_ID, "abc");

        assertTrue(table.insert(null, tuple));
        assertFalse(table.insert(null, tuple));
        assertFalse(table.insert(null, tuple2));

        var resTuple = table.get(null, defaultTupleKey());
        assertTupleEquals(tuple, resTuple);
    }

    @Test
    public void testInsertCustomTuple() {
        var table = defaultTable().recordView();
        var tuple = new CustomTuple(25L, "Foo");

        assertTrue(table.insert(null, tuple));
        assertFalse(table.insert(null, tuple));

        var resTuple = table.get(null, new CustomTuple(25L));

        assertTupleEquals(tuple, resTuple);
    }

    @Test
    public void testGetAll() {
        var table = defaultTable().recordView();
        table.insert(null, tuple(1L, "1"));
        table.insert(null, tuple(2L, "2"));
        table.insert(null, tuple(3L, "3"));

        List<Tuple> keys = Arrays.asList(tuple(1L), tuple(3L));
        Tuple[] res = sortedTuples(table.getAll(null, keys));

        assertEquals(2, res.length);

        assertEquals(1L, res[0].longValue("id"));
        assertEquals("1", res[0].stringValue("name"));

        assertEquals(3L, res[1].longValue("id"));
        assertEquals("3", res[1].stringValue("name"));
    }

    @Test
    public void testUpsertAll() {
        var table = defaultTable().recordView();

        List<Tuple> data = Arrays.asList(tuple(1L, "1"), tuple(2L, "2"));
        table.upsertAll(null, data);

        assertEquals("1", table.get(null, tuple(1L)).stringValue("name"));
        assertEquals("2", table.get(null, tuple(2L)).stringValue("name"));

        List<Tuple> data2 = Arrays.asList(tuple(1L, "10"), tuple(3L, "30"));
        table.upsertAll(null, data2);

        assertEquals("10", table.get(null, tuple(1L)).stringValue("name"));
        assertEquals("2", table.get(null, tuple(2L)).stringValue("name"));
        assertEquals("30", table.get(null, tuple(3L)).stringValue("name"));
    }

    @Test
    public void testInsertAll() {
        var table = defaultTable().recordView();

        List<Tuple> data = Arrays.asList(tuple(1L, "1"), tuple(2L, "2"));
        var skippedTuples = table.insertAll(null, data);

        assertEquals(0, skippedTuples.size());
        assertEquals("1", table.get(null, tuple(1L)).stringValue("name"));
        assertEquals("2", table.get(null, tuple(2L)).stringValue("name"));

        List<Tuple> data2 = Arrays.asList(tuple(1L, "10"), tuple(3L, "30"));
        var skippedTuples2 = table.insertAll(null, data2).toArray(new Tuple[0]);

        assertEquals(1, skippedTuples2.length);
        assertEquals(1L, skippedTuples2[0].longValue("id"));
        assertEquals("1", table.get(null, tuple(1L)).stringValue("name"));
        assertEquals("2", table.get(null, tuple(2L)).stringValue("name"));
        assertEquals("30", table.get(null, tuple(3L)).stringValue("name"));
    }

    @Test
    public void testReplace() {
        var table = defaultTable().recordView();
        table.insert(null, tuple(1L, "1"));

        assertFalse(table.replace(null, tuple(3L, "3")));
        assertNull(table.get(null, tuple(3L)));

        assertTrue(table.replace(null, tuple(1L, "2")));
        assertEquals("2", table.get(null, tuple(1L)).value("name"));
    }

    @Test
    public void testReplaceExact() {
        var table = defaultTable().recordView();
        table.insert(null, tuple(1L, "1"));

        assertFalse(table.replace(null, tuple(3L, "3"), tuple(3L, "4")));
        assertNull(table.get(null, tuple(3L)));

        assertFalse(table.replace(null, tuple(1L, "2"), tuple(1L, "3")));
        assertTrue(table.replace(null, tuple(1L, "1"), tuple(1L, "3")));
        assertEquals("3", table.get(null, tuple(1L)).value("name"));
    }

    @Test
    public void testGetAndReplace() {
        var table = defaultTable().recordView();
        var tuple = tuple(1L, "1");
        table.insert(null, tuple);

        assertNull(table.getAndReplace(null, tuple(3L, "3")));
        assertNull(table.get(null, tuple(3L)));

        var replaceRes = table.getAndReplace(null, tuple(1L, "2"));
        assertTupleEquals(tuple, replaceRes);
        assertEquals("2", table.get(null, tuple(1L)).value("name"));
    }

    @Test
    public void testDelete() {
        var table = defaultTable().recordView();
        table.insert(null, tuple(1L, "1"));

        assertFalse(table.delete(null, tuple(2L)));
        assertTrue(table.delete(null, tuple(1L)));
        assertNull(table.get(null, tuple(1L)));
    }

    @Test
    public void testDeleteExact() {
        var table = defaultTable().recordView();
        table.insert(null, tuple(1L, "1"));
        table.insert(null, tuple(2L, "2"));

        assertFalse(table.deleteExact(null, tuple(1L)));
        assertFalse(table.deleteExact(null, tuple(1L, "x")));
        assertTrue(table.deleteExact(null, tuple(1L, "1")));
        assertFalse(table.deleteExact(null, tuple(2L)));
        assertFalse(table.deleteExact(null, tuple(3L)));

        assertNull(table.get(null, tuple(1L)));
        assertNotNull(table.get(null, tuple(2L)));
    }

    @Test
    public void testGetAndDelete() {
        var table = defaultTable().recordView();
        var tuple = tuple(1L, "1");
        table.insert(null, tuple);

        var deleted = table.getAndDelete(null, tuple(1L));

        assertNull(table.getAndDelete(null, tuple(1L)));
        assertNull(table.getAndDelete(null, tuple(2L)));
        assertTupleEquals(tuple, deleted);
    }

    @Test
    public void testDeleteAll() {
        var table = defaultTable().recordView();

        List<Tuple> data = Arrays.asList(tuple(1L, "1"), tuple(2L, "2"));
        table.insertAll(null, data);

        List<Tuple> toDelete = Arrays.asList(tuple(1L, "x"), tuple(3L, "y"), tuple(4L, "z"));
        var skippedTuples = sortedTuples(table.deleteAll(null, toDelete));

        assertEquals(2, skippedTuples.length);
        assertNull(table.get(null, tuple(1L)));
        assertNotNull(table.get(null, tuple(2L)));

        assertEquals(3L, skippedTuples[0].longValue("id"));
        assertNull(skippedTuples[0].stringValue("name"));

        assertEquals(4L, skippedTuples[1].longValue("id"));
        assertNull(skippedTuples[1].stringValue("name"));
    }

    @Test
    public void testDeleteAllExact() {
        var table = defaultTable().recordView();

        List<Tuple> data = Arrays.asList(tuple(1L, "1"), tuple(2L, "2"));
        table.insertAll(null, data);

        List<Tuple> toDelete = Arrays.asList(tuple(1L, "1"), tuple(2L, "y"), tuple(3L, "z"));
        var skippedTuples = sortedTuples(table.deleteAllExact(null, toDelete));

        assertEquals(2, skippedTuples.length);
        assertNull(table.get(null, tuple(1L)));
        assertNotNull(table.get(null, tuple(2L)));

        assertEquals(2L, skippedTuples[0].longValue("id"));
        assertEquals("y", skippedTuples[0].stringValue("name"));

        assertEquals(3L, skippedTuples[1].longValue("id"));
        assertEquals("z", skippedTuples[1].stringValue("name"));
    }

    @Test
    public void testColumnWithDefaultValueNotSetReturnsDefault() {
        RecordView<Tuple> table = tableWithDefaultValues().recordView();

        var tuple = Tuple.create()
                .set("id", 1);

        table.upsert(null, tuple);

        var res = table.get(null, tuple);

        assertEquals("def_str", res.stringValue("str"));
        assertEquals("def_str2", res.stringValue("str_non_null"));
    }

    @Test
    public void testNullableColumnWithDefaultValueSetNullReturnsNull() {
        RecordView<Tuple> table = tableWithDefaultValues().recordView();

        var tuple = Tuple.create()
                .set("id", 1)
                .set("str", null);

        table.upsert(null, tuple);

        var res = table.get(null, tuple);

        assertNull(res.stringValue("str"));
    }

    @Test
    public void testNonNullableColumnWithDefaultValueSetNullThrowsException() {
        RecordView<Tuple> table = tableWithDefaultValues().recordView();

        var tuple = Tuple.create()
                .set("id", 1)
                .set("str_non_null", null);

        var ex = assertThrows(IgniteClientException.class, () -> table.upsert(null, tuple));

        assertTrue(ex.getMessage().contains("null was passed, but column is not nullable"), ex.getMessage());
    }

    @Test
    public void testColumnTypeMismatchThrowsException() {
        var tuple = Tuple.create().set("id", "str");

        var ex = assertThrows(IgniteClientException.class, () -> defaultTable().recordView().upsert(null, tuple));

        assertTrue(ex.getMessage().contains("Incorrect value type for column 'ID': Expected Integer, but got String"), ex.getMessage());
    }
}
