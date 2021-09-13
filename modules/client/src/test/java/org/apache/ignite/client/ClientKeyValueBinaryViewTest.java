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

import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletionException;
import org.apache.ignite.table.KeyValueBinaryView;
import org.apache.ignite.table.Table;
import org.apache.ignite.table.Tuple;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * KeyValueBinaryView tests.
 */
public class ClientKeyValueBinaryViewTest extends AbstractClientTableTest {
    @Test
    public void testGetMissingRowReturnsNull() {
        Table table = defaultTable();
        KeyValueBinaryView kvView = table.kvView();

        assertNull(kvView.get(defaultTupleKey()));
    }

    @Test
    public void testTableUpsertKvGet() {
        Table table = defaultTable();
        table.upsert(tuple());

        KeyValueBinaryView kvView = table.kvView();

        Tuple key = defaultTupleKey();
        Tuple val = kvView.get(key);

        assertEquals(DEFAULT_NAME, val.value("name"));
        assertEquals(DEFAULT_NAME, val.value(0));
        assertEquals(1, val.columnCount());
    }

    @Test
    public void testKvPutTableGet() {
        Table table = defaultTable();
        KeyValueBinaryView kvView = table.kvView();

        Tuple key = defaultTupleKey();
        Tuple val = Tuple.create().set("name", "bar");

        kvView.put(key, val);
        Tuple res = table.get(key);

        assertEquals("bar", res.stringValue("name"));
        assertEquals(DEFAULT_ID, res.longValue("id"));
    }

    @Test
    public void testPutGet() {
        Table table = defaultTable();
        KeyValueBinaryView kvView = table.kvView();

        Tuple key = defaultTupleKey();
        Tuple val = Tuple.create().set("name", DEFAULT_NAME);

        kvView.put(key, val);
        Tuple resVal = kvView.get(key);

        assertTupleEquals(val, resVal);
    }

    @Test
    public void testGetUpdatePut() {
        Table table = defaultTable();
        KeyValueBinaryView kvView = table.kvView();

        Tuple key = defaultTupleKey();
        Tuple val = Tuple.create().set("name", DEFAULT_NAME);
        kvView.put(key, val);

        Tuple resVal = kvView.get(key);
        resVal.set("name", "123");
        kvView.put(key, resVal);

        Tuple resVal2 = kvView.get(key);

        assertTupleEquals(resVal, resVal2);
    }

    @Test
    public void testGetAll() {
        KeyValueBinaryView kvView = defaultTable().kvView();

        kvView.put(tupleKey(1L), tupleVal("1"));
        kvView.put(tupleKey(2L), tupleVal("2"));
        kvView.put(tupleKey(3L), tupleVal("3"));

        Map<Tuple, Tuple> res = kvView.getAll(List.of(tupleKey(1L), tupleKey(3L)));

        assertEquals(2, res.size());

        var keys = sortedTuples(res.keySet());

        assertEquals(1L, keys[0].longValue(0));
        assertEquals(3L, keys[1].longValue(0));

        assertEquals("1", res.get(keys[0]).stringValue("name"));
        assertEquals("3", res.get(keys[1]).stringValue(0));
    }

    @Test
    public void testGetAllEmptyKeysReturnsEmptyMap() {
        KeyValueBinaryView kvView = defaultTable().kvView();
        kvView.put(tupleKey(1L), tupleVal("1"));

        Map<Tuple, Tuple> res = kvView.getAll(List.of());
        assertEquals(0, res.size());
    }

    @Test
    public void testGetAllNonExistentKeysReturnsEmptyMap() {
        KeyValueBinaryView kvView = defaultTable().kvView();
        kvView.put(tupleKey(1L), tupleVal("1"));

        Map<Tuple, Tuple> res = kvView.getAll(List.of(tupleKey(-1L)));
        assertEquals(0, res.size());
    }

    @Test
    public void testContains() {
        KeyValueBinaryView kvView = defaultTable().kvView();
        kvView.put(tupleKey(1L), tupleVal("1"));

        assertTrue(kvView.contains(tupleKey(1L)));
        assertFalse(kvView.contains(tupleKey(2L)));
    }

    @Test
    public void testContainsThrowsOnEmptyKey() {
        KeyValueBinaryView kvView = defaultTable().kvView();

        var ex = assertThrows(CompletionException.class, () -> kvView.contains(Tuple.create()));
        assertTrue(ex.getMessage().contains("Failed to set column"), ex.getMessage());
    }

    @Test
    public void testPutAll() {
        KeyValueBinaryView kvView = defaultTable().kvView();
        kvView.putAll(Map.of(tupleKey(1L), tupleVal("1"), tupleKey(2L), tupleVal("2")));

        assertEquals("1", kvView.get(tupleKey(1L)).stringValue("name"));
        assertEquals("2", kvView.get(tupleKey(2L)).stringValue(0));
    }

    @Test
    public void testPutIfAbsent() {
        KeyValueBinaryView kvView = defaultTable().kvView();

        assertTrue(kvView.putIfAbsent(tupleKey(1L), tupleVal("1")));
        assertFalse(kvView.putIfAbsent(tupleKey(1L), tupleVal("1")));
        assertFalse(kvView.putIfAbsent(tupleKey(1L), tupleVal("2")));
        assertTrue(kvView.putIfAbsent(tupleKey(2L), tupleVal("1")));
    }

    @Test
    public void testRemove() {
        KeyValueBinaryView kvView = defaultTable().kvView();
        kvView.put(tupleKey(1L), tupleVal("1"));

        assertFalse(kvView.remove(tupleKey(2L)));
        assertTrue(kvView.remove(tupleKey(1L)));
        assertFalse(kvView.remove(tupleKey(1L)));
        assertFalse(kvView.contains(tupleKey(1L)));
    }

    @Test
    public void testRemoveExact() {
        KeyValueBinaryView kvView = defaultTable().kvView();
        kvView.put(tupleKey(1L), tupleVal("1"));

        assertFalse(kvView.remove(tupleKey(1L), tupleVal("2")));
        assertFalse(kvView.remove(tupleKey(2L), tupleVal("1")));
        assertTrue(kvView.contains(tupleKey(1L)));

        assertTrue(kvView.remove(tupleKey(1L), tupleVal("1")));
        assertFalse(kvView.contains(tupleKey(1L)));
    }

    @Test
    public void testRemoveAll() {
        KeyValueBinaryView kvView = defaultTable().kvView();
        kvView.putAll(Map.of(tupleKey(1L), tupleVal("1"), tupleKey(2L), tupleVal("2")));

        Collection<Tuple> res = kvView.removeAll(List.of(tupleKey(2L), tupleKey(3L)));

        assertTrue(kvView.contains(tupleKey(1L)));
        assertFalse(kvView.contains(tupleKey(2L)));

        assertEquals(1, res.size());
        assertEquals(3L, res.iterator().next().longValue(0));
    }

    @Test
    public void testReplace() {
        KeyValueBinaryView kvView = defaultTable().kvView();
        kvView.put(tupleKey(1L), tupleVal("1"));

        assertFalse(kvView.replace(tupleKey(3L), tupleVal("3")));
        assertTrue(kvView.replace(tupleKey(1L), tupleVal("2")));
        assertEquals("2", kvView.get(tupleKey(1L)).stringValue(0));
    }

    @Test
    public void testReplaceExact() {
        KeyValueBinaryView kvView = defaultTable().kvView();
        kvView.put(tupleKey(1L), tupleVal("1"));

        assertFalse(kvView.replace(tupleKey(1L), tupleVal("2"), tupleVal("3")));
        assertTrue(kvView.replace(tupleKey(1L), tupleVal("1"), tupleVal("3")));
        assertEquals("3", kvView.get(tupleKey(1L)).stringValue(0));
    }

    @Test
    public void testGetAndReplace() {
        KeyValueBinaryView kvView = defaultTable().kvView();
        kvView.put(tupleKey(1L), tupleVal("1"));

        assertNull(kvView.getAndReplace(tupleKey(2L), tupleVal("2")));

        Tuple res = kvView.getAndReplace(tupleKey(1L), tupleVal("2"));
        assertEquals("1", res.stringValue(0));
        assertEquals("2", kvView.get(tupleKey(1L)).stringValue(0));
    }

    @Test
    public void testGetAndRemove() {
        KeyValueBinaryView kvView = defaultTable().kvView();
        kvView.put(tupleKey(1L), tupleVal("1"));

        Tuple removed = kvView.getAndRemove(tupleKey(1L));

        assertNotNull(removed);
        assertEquals(1, removed.columnCount());
        assertEquals("1", removed.stringValue(0));
        assertEquals("1", removed.stringValue("name"));

        assertFalse(kvView.contains(tupleKey(1L)));
        assertNull(kvView.getAndRemove(tupleKey(1L)));
    }

    @Test
    public void testGetAndPut() {
        KeyValueBinaryView kvView = defaultTable().kvView();
        kvView.put(tupleKey(1L), tupleVal("1"));

        Tuple res1 = kvView.getAndPut(tupleKey(2L), tupleVal("2"));
        Tuple res2 = kvView.getAndPut(tupleKey(1L), tupleVal("3"));

        assertNull(res1);
        assertEquals("2", kvView.get(tupleKey(2L)).stringValue(0));

        assertNotNull(res2);
        assertEquals("1", res2.stringValue(0));
        assertEquals("3", kvView.get(tupleKey(1L)).stringValue(0));
    }
}
