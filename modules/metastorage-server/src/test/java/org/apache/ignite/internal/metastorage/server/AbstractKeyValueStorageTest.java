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

package org.apache.ignite.internal.metastorage.server;

import java.util.Collection;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.stream.Collectors;
import org.apache.ignite.internal.metastorage.common.OperationType;
import org.apache.ignite.internal.util.Cursor;
import org.apache.ignite.lang.ByteArray;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import static java.util.function.Function.identity;
import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;

/**
 * Tests for key-value storage implementations.
 */
public abstract class AbstractKeyValueStorageTest {
    /** */
    private KeyValueStorage storage;

    @BeforeEach
    public void setUp() {
        storage = storage();

        storage.start();
    }

    @AfterEach
    void tearDown() throws Exception {
        storage.close();
    }

    /**
     * @return Key value storage for this test.
     */
    abstract KeyValueStorage storage();

    @Test
    public void put() {
        byte[] key = k(1);
        byte[] val = kv(1, 1);

        assertEquals(0, storage.revision());
        assertEquals(0, storage.updateCounter());
        assertTrue(storage.get(key).empty());

        storage.put(key, val);

        assertEquals(1, storage.revision());
        assertEquals(1, storage.updateCounter());

        Entry e = storage.get(key);

        assertFalse(e.empty());
        assertFalse(e.tombstone());
        assertEquals(1, e.revision());
        assertEquals(1, e.updateCounter());

        storage.put(key, val);

        assertEquals(2, storage.revision());
        assertEquals(2, storage.updateCounter());

        e = storage.get(key);

        assertFalse(e.empty());
        assertFalse(e.tombstone());
        assertEquals(2, e.revision());
        assertEquals(2, e.updateCounter());
    }

    @Test
    void getAll() {
        byte[] key1 = k(1);
        byte[] val1 = kv(1, 1);

        byte[] key2 = k(2);
        byte[] val2_1 = kv(2, 21);
        byte[] val2_2 = kv(2, 22);

        byte[] key3 = k(3);
        byte[] val3 = kv(3, 3);

        byte[] key4 = k(4);

        assertEquals(0, storage.revision());
        assertEquals(0, storage.updateCounter());

        // Regular put.
        storage.put(key1, val1);

        // Rewrite.
        storage.put(key2, val2_1);
        storage.put(key2, val2_2);

        // Remove.
        storage.put(key3, val3);
        storage.remove(key3);

        assertEquals(5, storage.revision());
        assertEquals(5, storage.updateCounter());

        Collection<Entry> entries = storage.getAll(List.of(key1, key2, key3, key4));

        assertEquals(4, entries.size());

        Map<ByteArray, Entry> map = entries.stream().collect(Collectors.toMap(e -> new ByteArray(e.key()), identity()));

        // Test regular put value.
        Entry e1 = map.get(new ByteArray(key1));

        assertNotNull(e1);
        assertEquals(1, e1.revision());
        assertEquals(1, e1.updateCounter());
        assertFalse(e1.tombstone());
        assertFalse(e1.empty());
        assertArrayEquals(val1, e1.value());

        // Test rewritten value.
        Entry e2 = map.get(new ByteArray(key2));

        assertNotNull(e2);
        assertEquals(3, e2.revision());
        assertEquals(3, e2.updateCounter());
        assertFalse(e2.tombstone());
        assertFalse(e2.empty());
        assertArrayEquals(val2_2, e2.value());

        // Test removed value.
        Entry e3 = map.get(new ByteArray(key3));

        assertNotNull(e3);
        assertEquals(5, e3.revision());
        assertEquals(5, e3.updateCounter());
        assertTrue(e3.tombstone());
        assertFalse(e3.empty());

        // Test empty value.
        Entry e4 = map.get(new ByteArray(key4));

        assertNotNull(e4);
        assertFalse(e4.tombstone());
        assertTrue(e4.empty());
    }

    @Test
    void getAllWithRevisionBound() {
        byte[] key1 = k(1);
        byte[] val1 = kv(1, 1);

        byte[] key2 = k(2);
        byte[] val2_1 = kv(2, 21);
        byte[] val2_2 = kv(2, 22);

        byte[] key3 = k(3);
        byte[] val3 = kv(3, 3);

        byte[] key4 = k(4);

        assertEquals(0, storage.revision());
        assertEquals(0, storage.updateCounter());

        // Regular put.
        storage.put(key1, val1);

        // Rewrite.
        storage.put(key2, val2_1);
        storage.put(key2, val2_2);

        // Remove.
        storage.put(key3, val3);
        storage.remove(key3);

        assertEquals(5, storage.revision());
        assertEquals(5, storage.updateCounter());

        // Bounded by revision 2.
        Collection<Entry> entries = storage.getAll(List.of(key1, key2, key3, key4), 2);

        assertEquals(4, entries.size());

        Map<ByteArray, Entry> map = entries.stream().collect(Collectors.toMap(e -> new ByteArray(e.key()), identity()));

        // Test regular put value.
        Entry e1 = map.get(new ByteArray(key1));

        assertNotNull(e1);
        assertEquals(1, e1.revision());
        assertEquals(1, e1.updateCounter());
        assertFalse(e1.tombstone());
        assertFalse(e1.empty());
        assertArrayEquals(val1, e1.value());

        // Test while not rewritten value.
        Entry e2 = map.get(new ByteArray(key2));

        assertNotNull(e2);
        assertEquals(2, e2.revision());
        assertEquals(2, e2.updateCounter());
        assertFalse(e2.tombstone());
        assertFalse(e2.empty());
        assertArrayEquals(val2_1, e2.value());

        // Values with larger revision don't exist yet.
        Entry e3 = map.get(new ByteArray(key3));

        assertNotNull(e3);
        assertTrue(e3.empty());

        Entry e4 = map.get(new ByteArray(key4));

        assertNotNull(e4);
        assertTrue(e4.empty());

        // Bounded by revision 4.
        entries = storage.getAll(List.of(key1, key2, key3, key4), 4);

        assertEquals(4, entries.size());

        map = entries.stream().collect(Collectors.toMap(e -> new ByteArray(e.key()), identity()));

        // Test regular put value.
        e1 = map.get(new ByteArray(key1));

        assertNotNull(e1);
        assertEquals(1, e1.revision());
        assertEquals(1, e1.updateCounter());
        assertFalse(e1.tombstone());
        assertFalse(e1.empty());
        assertArrayEquals(val1, e1.value());

        // Test rewritten value.
        e2 = map.get(new ByteArray(key2));

        assertNotNull(e2);
        assertEquals(3, e2.revision());
        assertEquals(3, e2.updateCounter());
        assertFalse(e2.tombstone());
        assertFalse(e2.empty());
        assertArrayEquals(val2_2, e2.value());

        // Test not removed value.
        e3 = map.get(new ByteArray(key3));

        assertNotNull(e3);
        assertEquals(4, e3.revision());
        assertEquals(4, e3.updateCounter());
        assertFalse(e3.tombstone());
        assertFalse(e3.empty());
        assertArrayEquals(val3, e3.value());

        // Value with larger revision doesn't exist yet.
        e4 = map.get(new ByteArray(key4));

        assertNotNull(e4);
        assertTrue(e4.empty());
    }

    @Test
    public void getAndPut() {
        byte[] key = k(1);
        byte[] val = kv(1, 1);

        assertEquals(0, storage.revision());
        assertEquals(0, storage.updateCounter());
        assertTrue(storage.get(key).empty());

        Entry e = storage.getAndPut(key, val);

        assertEquals(1, storage.revision());
        assertEquals(1, storage.updateCounter());
        assertTrue(e.empty());
        assertFalse(e.tombstone());
        assertEquals(0, e.revision());
        assertEquals(0, e.updateCounter());

        e = storage.getAndPut(key, val);

        assertEquals(2, storage.revision());
        assertEquals(2, storage.updateCounter());
        assertFalse(e.empty());
        assertFalse(e.tombstone());
        assertEquals(1, e.revision());
        assertEquals(1, e.updateCounter());
    }

    @Test
    public void putAll() {
        byte[] key1 = k(1);
        byte[] val1 = kv(1, 1);

        byte[] key2 = k(2);
        byte[] val2_1 = kv(2, 21);
        byte[] val2_2 = kv(2, 22);

        byte[] key3 = k(3);
        byte[] val3_1 = kv(3, 31);
        byte[] val3_2 = kv(3, 32);

        byte[] key4 = k(4);

        assertEquals(0, storage.revision());
        assertEquals(0, storage.updateCounter());

        // Must be rewritten.
        storage.put(key2, val2_1);

        // Remove. Tombstone must be replaced by new value.
        storage.put(key3, val3_1);
        storage.remove(key3);

        assertEquals(3, storage.revision());
        assertEquals(3, storage.updateCounter());

        storage.putAll(List.of(key1, key2, key3), List.of(val1, val2_2, val3_2));

        assertEquals(4, storage.revision());
        assertEquals(6, storage.updateCounter());

        Collection<Entry> entries = storage.getAll(List.of(key1, key2, key3, key4));

        assertEquals(4, entries.size());

        Map<ByteArray, Entry> map = entries.stream().collect(Collectors.toMap(e -> new ByteArray(e.key()), identity()));

        // Test regular put value.
        Entry e1 = map.get(new ByteArray(key1));

        assertNotNull(e1);
        assertEquals(4, e1.revision());
        assertEquals(4, e1.updateCounter());
        assertFalse(e1.tombstone());
        assertFalse(e1.empty());
        assertArrayEquals(val1, e1.value());

        // Test rewritten value.
        Entry e2 = map.get(new ByteArray(key2));

        assertNotNull(e2);
        assertEquals(4, e2.revision());
        assertEquals(5, e2.updateCounter());
        assertFalse(e2.tombstone());
        assertFalse(e2.empty());
        assertArrayEquals(val2_2, e2.value());

        // Test removed value.
        Entry e3 = map.get(new ByteArray(key3));

        assertNotNull(e3);
        assertEquals(4, e3.revision());
        assertEquals(6, e3.updateCounter());
        assertFalse(e3.tombstone());
        assertFalse(e3.empty());

        // Test empty value.
        Entry e4 = map.get(new ByteArray(key4));

        assertNotNull(e4);
        assertFalse(e4.tombstone());
        assertTrue(e4.empty());
    }

    @Test
    public void getAndPutAll() {
        byte[] key1 = k(1);
        byte[] val1 = kv(1, 1);

        byte[] key2 = k(2);
        byte[] val2_1 = kv(2, 21);
        byte[] val2_2 = kv(2, 22);

        byte[] key3 = k(3);
        byte[] val3_1 = kv(3, 31);
        byte[] val3_2 = kv(3, 32);

        byte[] key4 = k(4);

        assertEquals(0, storage.revision());
        assertEquals(0, storage.updateCounter());

        // Must be rewritten.
        storage.put(key2, val2_1);

        // Remove. Tombstone must be replaced by new value.
        storage.put(key3, val3_1);
        storage.remove(key3);

        assertEquals(3, storage.revision());
        assertEquals(3, storage.updateCounter());

        Collection<Entry> entries = storage.getAndPutAll(List.of(key1, key2, key3), List.of(val1, val2_2, val3_2));

        assertEquals(4, storage.revision());
        assertEquals(6, storage.updateCounter());

        assertEquals(3, entries.size());

        Map<ByteArray, Entry> map = entries.stream().collect(Collectors.toMap(e -> new ByteArray(e.key()), identity()));

        // Test regular put value.
        Entry e1 = map.get(new ByteArray(key1));

        assertNotNull(e1);
        assertEquals(0, e1.revision());
        assertEquals(0, e1.updateCounter());
        assertFalse(e1.tombstone());
        assertTrue(e1.empty());

        // Test rewritten value.
        Entry e2 = map.get(new ByteArray(key2));

        assertNotNull(e2);
        assertEquals(1, e2.revision());
        assertEquals(1, e2.updateCounter());
        assertFalse(e2.tombstone());
        assertFalse(e2.empty());
        assertArrayEquals(val2_1, e2.value());

        // Test removed value.
        Entry e3 = map.get(new ByteArray(key3));

        assertNotNull(e3);
        assertEquals(3, e3.revision());
        assertEquals(3, e3.updateCounter());
        assertTrue(e3.tombstone());
        assertFalse(e3.empty());

        // Test state after putAll.
        entries = storage.getAll(List.of(key1, key2, key3, key4));

        assertEquals(4, entries.size());

        map = entries.stream().collect(Collectors.toMap(e -> new ByteArray(e.key()), identity()));

        // Test regular put value.
        e1 = map.get(new ByteArray(key1));

        assertNotNull(e1);
        assertEquals(4, e1.revision());
        assertEquals(4, e1.updateCounter());
        assertFalse(e1.tombstone());
        assertFalse(e1.empty());
        assertArrayEquals(val1, e1.value());

        // Test rewritten value.
        e2 = map.get(new ByteArray(key2));

        assertNotNull(e2);
        assertEquals(4, e2.revision());
        assertEquals(5, e2.updateCounter());
        assertFalse(e2.tombstone());
        assertFalse(e2.empty());
        assertArrayEquals(val2_2, e2.value());

        // Test removed value.
        e3 = map.get(new ByteArray(key3));

        assertNotNull(e3);
        assertEquals(4, e3.revision());
        assertEquals(6, e3.updateCounter());
        assertFalse(e3.tombstone());
        assertFalse(e3.empty());

        // Test empty value.
        Entry e4 = map.get(new ByteArray(key4));

        assertNotNull(e4);
        assertFalse(e4.tombstone());
        assertTrue(e4.empty());
    }

    @Test
    public void remove() {
        byte[] key = k(1);
        byte[] val = kv(1, 1);

        assertEquals(0, storage.revision());
        assertEquals(0, storage.updateCounter());
        assertTrue(storage.get(key).empty());

        // Remove non-existent entry.
        storage.remove(key);

        assertEquals(0, storage.revision());
        assertEquals(0, storage.updateCounter());
        assertTrue(storage.get(key).empty());

        storage.put(key, val);

        assertEquals(1, storage.revision());
        assertEquals(1, storage.updateCounter());

        // Remove existent entry.
        storage.remove(key);

        assertEquals(2, storage.revision());
        assertEquals(2, storage.updateCounter());

        Entry e = storage.get(key);

        assertFalse(e.empty());
        assertTrue(e.tombstone());
        assertEquals(2, e.revision());
        assertEquals(2, e.updateCounter());

        // Remove already removed entry (tombstone can't be removed).
        storage.remove(key);

        assertEquals(2, storage.revision());
        assertEquals(2, storage.updateCounter());

        e = storage.get(key);

        assertFalse(e.empty());
        assertTrue(e.tombstone());
        assertEquals(2, e.revision());
        assertEquals(2, e.updateCounter());
    }

    @Test
    public void getAndRemove() {
        byte[] key = k(1);
        byte[] val = kv(1, 1);

        assertEquals(0, storage.revision());
        assertEquals(0, storage.updateCounter());
        assertTrue(storage.get(key).empty());

        // Remove non-existent entry.
        Entry e = storage.getAndRemove(key);

        assertTrue(e.empty());
        assertEquals(0, storage.revision());
        assertEquals(0, storage.updateCounter());
        assertTrue(storage.get(key).empty());

        storage.put(key, val);

        assertEquals(1, storage.revision());
        assertEquals(1, storage.updateCounter());

        // Remove existent entry.
        e = storage.getAndRemove(key);

        assertFalse(e.empty());
        assertFalse(e.tombstone());
        assertEquals(1, e.revision());
        assertEquals(1, e.updateCounter());
        assertEquals(2, storage.revision());
        assertEquals(2, storage.updateCounter());

        e = storage.get(key);

        assertFalse(e.empty());
        assertTrue(e.tombstone());
        assertEquals(2, e.revision());
        assertEquals(2, e.updateCounter());

        // Remove already removed entry (tombstone can't be removed).
        e = storage.getAndRemove(key);

        assertFalse(e.empty());
        assertTrue(e.tombstone());
        assertEquals(2, e.revision());
        assertEquals(2, e.updateCounter());
        assertEquals(2, storage.revision());
        assertEquals(2, storage.updateCounter());

        e = storage.get(key);

        assertFalse(e.empty());
        assertTrue(e.tombstone());
        assertEquals(2, e.revision());
        assertEquals(2, e.updateCounter());
    }

    @Test
    public void removeAll() {
        byte[] key1 = k(1);
        byte[] val1 = kv(1, 1);

        byte[] key2 = k(2);
        byte[] val2_1 = kv(2, 21);
        byte[] val2_2 = kv(2, 22);

        byte[] key3 = k(3);
        byte[] val3_1 = kv(3, 31);

        byte[] key4 = k(4);

        assertEquals(0, storage.revision());
        assertEquals(0, storage.updateCounter());

        // Regular put.
        storage.put(key1, val1);

        // Rewrite.
        storage.put(key2, val2_1);
        storage.put(key2, val2_2);

        // Remove. Tombstone must not be removed again.
        storage.put(key3, val3_1);
        storage.remove(key3);

        assertEquals(5, storage.revision());
        assertEquals(5, storage.updateCounter());

        storage.removeAll(List.of(key1, key2, key3, key4));

        assertEquals(6, storage.revision());
        assertEquals(7, storage.updateCounter()); // Only two keys are updated.

        Collection<Entry> entries = storage.getAll(List.of(key1, key2, key3, key4));

        assertEquals(4, entries.size());

        Map<ByteArray, Entry> map = entries.stream().collect(Collectors.toMap(e -> new ByteArray(e.key()), identity()));

        // Test regular put value.
        Entry e1 = map.get(new ByteArray(key1));

        assertNotNull(e1);
        assertEquals(6, e1.revision());
        assertEquals(6, e1.updateCounter());
        assertTrue(e1.tombstone());
        assertFalse(e1.empty());

        // Test rewritten value.
        Entry e2 = map.get(new ByteArray(key2));

        assertNotNull(e2);
        assertEquals(6, e2.revision());
        assertEquals(7, e2.updateCounter());
        assertTrue(e2.tombstone());
        assertFalse(e2.empty());

        // Test removed value.
        Entry e3 = map.get(new ByteArray(key3));

        assertNotNull(e3);
        assertEquals(5, e3.revision());
        assertEquals(5, e3.updateCounter());
        assertTrue(e3.tombstone());
        assertFalse(e3.empty());

        // Test empty value.
        Entry e4 = map.get(new ByteArray(key4));

        assertNotNull(e4);
        assertFalse(e4.tombstone());
        assertTrue(e4.empty());
    }

    @Test
    public void getAndRemoveAll() {
        byte[] key1 = k(1);
        byte[] val1 = kv(1, 1);

        byte[] key2 = k(2);
        byte[] val2_1 = kv(2, 21);
        byte[] val2_2 = kv(2, 22);

        byte[] key3 = k(3);
        byte[] val3_1 = kv(3, 31);

        byte[] key4 = k(4);

        assertEquals(0, storage.revision());
        assertEquals(0, storage.updateCounter());

        // Regular put.
        storage.put(key1, val1);

        // Rewrite.
        storage.put(key2, val2_1);
        storage.put(key2, val2_2);

        // Remove. Tombstone must not be removed again.
        storage.put(key3, val3_1);
        storage.remove(key3);

        assertEquals(5, storage.revision());
        assertEquals(5, storage.updateCounter());

        Collection<Entry> entries = storage.getAndRemoveAll(List.of(key1, key2, key3, key4));

        assertEquals(6, storage.revision());
        assertEquals(7, storage.updateCounter()); // Only two keys are updated.

        assertEquals(4, entries.size());

        Map<ByteArray, Entry> map = entries.stream().collect(Collectors.toMap(e -> new ByteArray(e.key()), identity()));

        // Test regular put value.
        Entry e1 = map.get(new ByteArray(key1));

        assertNotNull(e1);
        assertEquals(1, e1.revision());
        assertEquals(1, e1.updateCounter());
        assertFalse(e1.tombstone());
        assertFalse(e1.empty());

        // Test rewritten value.
        Entry e2 = map.get(new ByteArray(key2));

        assertNotNull(e2);
        assertEquals(3, e2.revision());
        assertEquals(3, e2.updateCounter());
        assertFalse(e2.tombstone());
        assertFalse(e2.empty());

        // Test removed value.
        Entry e3 = map.get(new ByteArray(key3));

        assertNotNull(e3);
        assertEquals(5, e3.revision());
        assertEquals(5, e3.updateCounter());
        assertTrue(e3.tombstone());
        assertFalse(e3.empty());

        // Test empty value.
        Entry e4 = map.get(new ByteArray(key4));

        assertNotNull(e4);
        assertFalse(e4.tombstone());
        assertTrue(e4.empty());

        // Test state after getAndRemoveAll.
        entries = storage.getAll(List.of(key1, key2, key3, key4));

        assertEquals(4, entries.size());

        map = entries.stream().collect(Collectors.toMap(e -> new ByteArray(e.key()), identity()));

        // Test regular put value.
        e1 = map.get(new ByteArray(key1));

        assertNotNull(e1);
        assertEquals(6, e1.revision());
        assertEquals(6, e1.updateCounter());
        assertTrue(e1.tombstone());
        assertFalse(e1.empty());

        // Test rewritten value.
        e2 = map.get(new ByteArray(key2));

        assertNotNull(e2);
        assertEquals(6, e2.revision());
        assertEquals(7, e2.updateCounter());
        assertTrue(e2.tombstone());
        assertFalse(e2.empty());

        // Test removed value.
        e3 = map.get(new ByteArray(key3));

        assertNotNull(e3);
        assertEquals(5, e3.revision());
        assertEquals(5, e3.updateCounter());
        assertTrue(e3.tombstone());
        assertFalse(e3.empty());

        // Test empty value.
        e4 = map.get(new ByteArray(key4));

        assertNotNull(e4);
        assertFalse(e4.tombstone());
        assertTrue(e4.empty());
    }

    @Test
    public void getAfterRemove() {
        byte[] key = k(1);
        byte[] val = kv(1, 1);

        storage.getAndPut(key, val);

        storage.getAndRemove(key);

        Entry e = storage.get(key);

        assertEquals(2, storage.revision());
        assertEquals(2, storage.updateCounter());
        assertEquals(2, e.revision());
        assertTrue(e.tombstone());
    }

    @Test
    public void getAndPutAfterRemove() {
        byte[] key = k(1);
        byte[] val = kv(1, 1);

        storage.getAndPut(key, val);

        storage.getAndRemove(key);

        Entry e = storage.getAndPut(key, val);

        assertEquals(3, storage.revision());
        assertEquals(3, storage.updateCounter());
        assertEquals(2, e.revision());
        assertTrue(e.tombstone());
    }

    @Test
    public void putGetRemoveCompact() {
        byte[] key1 = k(1);
        byte[] val1_1 = kv(1, 1);
        byte[] val1_3 = kv(1, 3);

        byte[] key2 = k(2);
        byte[] val2_2 = kv(2, 2);

        assertEquals(0, storage.revision());
        assertEquals(0, storage.updateCounter());

        // Previous entry is empty.
        Entry emptyEntry = storage.getAndPut(key1, val1_1);

        assertEquals(1, storage.revision());
        assertEquals(1, storage.updateCounter());
        assertTrue(emptyEntry.empty());

        // Entry with rev == 1.
        Entry e1_1 = storage.get(key1);

        assertFalse(e1_1.empty());
        assertFalse(e1_1.tombstone());
        assertArrayEquals(key1, e1_1.key());
        assertArrayEquals(val1_1, e1_1.value());
        assertEquals(1, e1_1.revision());
        assertEquals(1, e1_1.updateCounter());
        assertEquals(1, storage.revision());
        assertEquals(1, storage.updateCounter());

        // Previous entry is empty.
        emptyEntry = storage.getAndPut(key2, val2_2);

        assertEquals(2, storage.revision());
        assertEquals(2, storage.updateCounter());
        assertTrue(emptyEntry.empty());

        // Entry with rev == 2.
        Entry e2 = storage.get(key2);

        assertFalse(e2.empty());
        assertFalse(e2.tombstone());
        assertArrayEquals(key2, e2.key());
        assertArrayEquals(val2_2, e2.value());
        assertEquals(2, e2.revision());
        assertEquals(2, e2.updateCounter());
        assertEquals(2, storage.revision());
        assertEquals(2, storage.updateCounter());

        // Previous entry is not empty.
        e1_1 = storage.getAndPut(key1, val1_3);

        assertFalse(e1_1.empty());
        assertFalse(e1_1.tombstone());
        assertArrayEquals(key1, e1_1.key());
        assertArrayEquals(val1_1, e1_1.value());
        assertEquals(1, e1_1.revision());
        assertEquals(1, e1_1.updateCounter());
        assertEquals(3, storage.revision());
        assertEquals(3, storage.updateCounter());

        // Entry with rev == 3.
        Entry e1_3 = storage.get(key1);

        assertFalse(e1_3.empty());
        assertFalse(e1_3.tombstone());
        assertArrayEquals(key1, e1_3.key());
        assertArrayEquals(val1_3, e1_3.value());
        assertEquals(3, e1_3.revision());
        assertEquals(3, e1_3.updateCounter());
        assertEquals(3, storage.revision());
        assertEquals(3, storage.updateCounter());

        // Remove existing entry.
        Entry e2_2 = storage.getAndRemove(key2);

        assertFalse(e2_2.empty());
        assertFalse(e2_2.tombstone());
        assertArrayEquals(key2, e2_2.key());
        assertArrayEquals(val2_2, e2_2.value());
        assertEquals(2, e2_2.revision());
        assertEquals(2, e2_2.updateCounter());
        assertEquals(4, storage.revision()); // Storage revision is changed.
        assertEquals(4, storage.updateCounter());

        // Remove already removed entry.
        Entry tombstoneEntry = storage.getAndRemove(key2);

        assertFalse(tombstoneEntry.empty());
        assertTrue(tombstoneEntry.tombstone());
        assertEquals(4, storage.revision()); // Storage revision is not changed.
        assertEquals(4, storage.updateCounter());

        // Compact and check that tombstones are removed.
        storage.compact();

        assertEquals(4, storage.revision());
        assertEquals(4, storage.updateCounter());
        assertTrue(storage.getAndRemove(key2).empty());
        assertTrue(storage.get(key2).empty());

        // Remove existing entry.
        e1_3 = storage.getAndRemove(key1);

        assertFalse(e1_3.empty());
        assertFalse(e1_3.tombstone());
        assertArrayEquals(key1, e1_3.key());
        assertArrayEquals(val1_3, e1_3.value());
        assertEquals(3, e1_3.revision());
        assertEquals(3, e1_3.updateCounter());
        assertEquals(5, storage.revision()); // Storage revision is changed.
        assertEquals(5, storage.updateCounter());

        // Remove already removed entry.
        tombstoneEntry = storage.getAndRemove(key1);

        assertFalse(tombstoneEntry.empty());
        assertTrue(tombstoneEntry.tombstone());
        assertEquals(5, storage.revision()); // // Storage revision is not changed.
        assertEquals(5, storage.updateCounter());

        // Compact and check that tombstones are removed.
        storage.compact();

        assertEquals(5, storage.revision());
        assertEquals(5, storage.updateCounter());
        assertTrue(storage.getAndRemove(key1).empty());
        assertTrue(storage.get(key1).empty());
    }

    @Test
    public void invokeWithRevisionCondition_successBranch() {
        byte[] key1 = k(1);
        byte[] val1_1 = kv(1, 11);
        byte[] val1_2 = kv(1, 12);

        byte[] key2 = k(2);
        byte[] val2 = kv(2, 2);

        byte[] key3 = k(3);
        byte[] val3 = kv(3, 3);

        assertEquals(0, storage.revision());
        assertEquals(0, storage.updateCounter());

        storage.put(key1, val1_1);

        assertEquals(1, storage.revision());
        assertEquals(1, storage.updateCounter());

        boolean branch = storage.invoke(
            new RevisionCondition(RevisionCondition.Type.EQUAL, key1, 1),
            List.of(
                new Operation(OperationType.PUT, key1, val1_2),
                new Operation(OperationType.PUT, key2, val2)
            ),
            List.of(new Operation(OperationType.PUT, key3, val3))
        );

        // "Success" branch is applied.
        assertTrue(branch);
        assertEquals(2, storage.revision());
        assertEquals(3, storage.updateCounter());

        Entry e1 = storage.get(key1);

        assertFalse(e1.empty());
        assertFalse(e1.tombstone());
        assertEquals(2, e1.revision());
        assertEquals(2, e1.updateCounter());
        assertArrayEquals(val1_2, e1.value());

        Entry e2 = storage.get(key2);

        assertFalse(e2.empty());
        assertFalse(e2.tombstone());
        assertEquals(2, e2.revision());
        assertEquals(3, e2.updateCounter());
        assertArrayEquals(val2, e2.value());

        // "Failure" branch isn't applied.
        Entry e3 = storage.get(key3);

        assertTrue(e3.empty());
    }

    @Test
    public void invokeWithRevisionCondition_failureBranch() {
        byte[] key1 = k(1);
        byte[] val1_1 = kv(1, 11);
        byte[] val1_2 = kv(1, 12);

        byte[] key2 = k(2);
        byte[] val2 = kv(2, 2);

        byte[] key3 = k(3);
        byte[] val3 = kv(3, 3);

        assertEquals(0, storage.revision());
        assertEquals(0, storage.updateCounter());

        storage.put(key1, val1_1);

        assertEquals(1, storage.revision());
        assertEquals(1, storage.updateCounter());

        boolean branch = storage.invoke(
            new RevisionCondition(RevisionCondition.Type.EQUAL, key1, 2),
            List.of(new Operation(OperationType.PUT, key3, val3)),
            List.of(
                new Operation(OperationType.PUT, key1, val1_2),
                new Operation(OperationType.PUT, key2, val2)
            )
        );

        // "Failure" branch is applied.
        assertFalse(branch);
        assertEquals(2, storage.revision());
        assertEquals(3, storage.updateCounter());

        Entry e1 = storage.get(key1);

        assertFalse(e1.empty());
        assertFalse(e1.tombstone());
        assertEquals(2, e1.revision());
        assertEquals(2, e1.updateCounter());
        assertArrayEquals(val1_2, e1.value());

        Entry e2 = storage.get(key2);

        assertFalse(e2.empty());
        assertFalse(e2.tombstone());
        assertEquals(2, e2.revision());
        assertEquals(3, e2.updateCounter());
        assertArrayEquals(val2, e2.value());

        // "Success" branch isn't applied.
        Entry e3 = storage.get(key3);

        assertTrue(e3.empty());
    }

    @Test
    public void invokeWithExistsCondition_successBranch() {
        byte[] key1 = k(1);
        byte[] val1_1 = kv(1, 11);
        byte[] val1_2 = kv(1, 12);

        byte[] key2 = k(2);
        byte[] val2 = kv(2, 2);

        byte[] key3 = k(3);
        byte[] val3 = kv(3, 3);

        assertEquals(0, storage.revision());
        assertEquals(0, storage.updateCounter());

        storage.put(key1, val1_1);

        assertEquals(1, storage.revision());
        assertEquals(1, storage.updateCounter());

        boolean branch = storage.invoke(
            new ExistenceCondition(ExistenceCondition.Type.EXISTS, key1),
            List.of(
                new Operation(OperationType.PUT, key1, val1_2),
                new Operation(OperationType.PUT, key2, val2)
            ),
            List.of(new Operation(OperationType.PUT, key3, val3))
        );

        // "Success" branch is applied.
        assertTrue(branch);
        assertEquals(2, storage.revision());
        assertEquals(3, storage.updateCounter());

        Entry e1 = storage.get(key1);

        assertFalse(e1.empty());
        assertFalse(e1.tombstone());
        assertEquals(2, e1.revision());
        assertEquals(2, e1.updateCounter());
        assertArrayEquals(val1_2, e1.value());

        Entry e2 = storage.get(key2);

        assertFalse(e2.empty());
        assertFalse(e2.tombstone());
        assertEquals(2, e2.revision());
        assertEquals(3, e2.updateCounter());
        assertArrayEquals(val2, e2.value());

        // "Failure" branch isn't applied.
        Entry e3 = storage.get(key3);

        assertTrue(e3.empty());
    }

    @Test
    public void invokeWithExistsCondition_failureBranch() {
        byte[] key1 = k(1);
        byte[] val1_1 = kv(1, 11);
        byte[] val1_2 = kv(1, 12);

        byte[] key2 = k(2);
        byte[] val2 = kv(2, 2);

        byte[] key3 = k(3);
        byte[] val3 = kv(3, 3);

        assertEquals(0, storage.revision());
        assertEquals(0, storage.updateCounter());

        storage.put(key1, val1_1);

        assertEquals(1, storage.revision());
        assertEquals(1, storage.updateCounter());

        boolean branch = storage.invoke(
            new ExistenceCondition(ExistenceCondition.Type.EXISTS, key3),
            List.of(new Operation(OperationType.PUT, key3, val3)),
            List.of(
                new Operation(OperationType.PUT, key1, val1_2),
                new Operation(OperationType.PUT, key2, val2)
            )
        );

        // "Failure" branch is applied.
        assertFalse(branch);
        assertEquals(2, storage.revision());
        assertEquals(3, storage.updateCounter());

        Entry e1 = storage.get(key1);

        assertFalse(e1.empty());
        assertFalse(e1.tombstone());
        assertEquals(2, e1.revision());
        assertEquals(2, e1.updateCounter());
        assertArrayEquals(val1_2, e1.value());

        Entry e2 = storage.get(key2);

        assertFalse(e2.empty());
        assertFalse(e2.tombstone());
        assertEquals(2, e2.revision());
        assertEquals(3, e2.updateCounter());
        assertArrayEquals(val2, e2.value());

        // "Success" branch isn't applied.
        Entry e3 = storage.get(key3);

        assertTrue(e3.empty());
    }

    @Test
    public void invokeWithNotExistsCondition_successBranch() {
        byte[] key1 = k(1);
        byte[] val1_1 = kv(1, 11);
        byte[] val1_2 = kv(1, 12);

        byte[] key2 = k(2);
        byte[] val2 = kv(2, 2);

        byte[] key3 = k(3);
        byte[] val3 = kv(3, 3);

        assertEquals(0, storage.revision());
        assertEquals(0, storage.updateCounter());

        storage.put(key1, val1_1);

        assertEquals(1, storage.revision());
        assertEquals(1, storage.updateCounter());

        boolean branch = storage.invoke(
            new ExistenceCondition(ExistenceCondition.Type.NOT_EXISTS, key2),
            List.of(
                new Operation(OperationType.PUT, key1, val1_2),
                new Operation(OperationType.PUT, key2, val2)
            ),
            List.of(new Operation(OperationType.PUT, key3, val3))
        );

        // "Success" branch is applied.
        assertTrue(branch);
        assertEquals(2, storage.revision());
        assertEquals(3, storage.updateCounter());

        Entry e1 = storage.get(key1);

        assertFalse(e1.empty());
        assertFalse(e1.tombstone());
        assertEquals(2, e1.revision());
        assertEquals(2, e1.updateCounter());
        assertArrayEquals(val1_2, e1.value());

        Entry e2 = storage.get(key2);

        assertFalse(e2.empty());
        assertFalse(e2.tombstone());
        assertEquals(2, e2.revision());
        assertEquals(3, e2.updateCounter());
        assertArrayEquals(val2, e2.value());

        // "Failure" branch isn't applied.
        Entry e3 = storage.get(key3);

        assertTrue(e3.empty());
    }

    @Test
    public void invokeWithNotExistsCondition_failureBranch() {
        byte[] key1 = k(1);
        byte[] val1_1 = kv(1, 11);
        byte[] val1_2 = kv(1, 12);

        byte[] key2 = k(2);
        byte[] val2 = kv(2, 2);

        byte[] key3 = k(3);
        byte[] val3 = kv(3, 3);

        assertEquals(0, storage.revision());
        assertEquals(0, storage.updateCounter());

        storage.put(key1, val1_1);

        assertEquals(1, storage.revision());
        assertEquals(1, storage.updateCounter());

        boolean branch = storage.invoke(
            new ExistenceCondition(ExistenceCondition.Type.NOT_EXISTS, key1),
            List.of(new Operation(OperationType.PUT, key3, val3)),
            List.of(
                new Operation(OperationType.PUT, key1, val1_2),
                new Operation(OperationType.PUT, key2, val2)
            )
        );

        // "Failure" branch is applied.
        assertFalse(branch);
        assertEquals(2, storage.revision());
        assertEquals(3, storage.updateCounter());

        Entry e1 = storage.get(key1);

        assertFalse(e1.empty());
        assertFalse(e1.tombstone());
        assertEquals(2, e1.revision());
        assertEquals(2, e1.updateCounter());
        assertArrayEquals(val1_2, e1.value());

        Entry e2 = storage.get(key2);

        assertFalse(e2.empty());
        assertFalse(e2.tombstone());
        assertEquals(2, e2.revision());
        assertEquals(3, e2.updateCounter());
        assertArrayEquals(val2, e2.value());

        // "Success" branch isn't applied.
        Entry e3 = storage.get(key3);

        assertTrue(e3.empty());
    }

    @Test
    public void invokeWithTombstoneCondition_successBranch() {
        byte[] key1 = k(1);
        byte[] val1_1 = kv(1, 11);

        byte[] key2 = k(2);
        byte[] val2 = kv(2, 2);

        byte[] key3 = k(3);
        byte[] val3 = kv(3, 3);

        assertEquals(0, storage.revision());
        assertEquals(0, storage.updateCounter());

        storage.put(key1, val1_1);
        storage.remove(key1); // Should be tombstone after remove.

        assertEquals(2, storage.revision());
        assertEquals(2, storage.updateCounter());

        boolean branch = storage.invoke(
            new TombstoneCondition(key1),
            List.of(new Operation(OperationType.PUT, key2, val2)),
            List.of(new Operation(OperationType.PUT, key3, val3))
        );

        // "Success" branch is applied.
        assertTrue(branch);
        assertEquals(3, storage.revision());
        assertEquals(3, storage.updateCounter());

        Entry e1 = storage.get(key1);

        assertFalse(e1.empty());
        assertTrue(e1.tombstone());
        assertEquals(2, e1.revision());
        assertEquals(2, e1.updateCounter());
        assertNull(e1.value());

        Entry e2 = storage.get(key2);

        assertFalse(e2.empty());
        assertFalse(e2.tombstone());
        assertEquals(3, e2.revision());
        assertEquals(3, e2.updateCounter());
        assertArrayEquals(val2, e2.value());

        // "Failure" branch isn't applied.
        Entry e3 = storage.get(key3);

        assertTrue(e3.empty());
    }

    @Test
    public void invokeWithTombstoneCondition_failureBranch() {
        byte[] key1 = k(1);
        byte[] val1_1 = kv(1, 11);

        byte[] key2 = k(2);
        byte[] val2 = kv(2, 2);

        byte[] key3 = k(3);
        byte[] val3 = kv(3, 3);

        assertEquals(0, storage.revision());
        assertEquals(0, storage.updateCounter());

        storage.put(key1, val1_1);

        assertEquals(1, storage.revision());
        assertEquals(1, storage.updateCounter());

        boolean branch = storage.invoke(
            new TombstoneCondition(key1),
            List.of(new Operation(OperationType.PUT, key2, val2)),
            List.of(new Operation(OperationType.PUT, key3, val3))
        );

        // "Failure" branch is applied.
        assertFalse(branch);
        assertEquals(2, storage.revision());
        assertEquals(2, storage.updateCounter());

        Entry e1 = storage.get(key1);

        assertFalse(e1.empty());
        assertFalse(e1.tombstone());
        assertEquals(1, e1.revision());
        assertEquals(1, e1.updateCounter());
        assertArrayEquals(val1_1, e1.value());

        Entry e3 = storage.get(key3);

        assertFalse(e3.empty());
        assertFalse(e3.tombstone());
        assertEquals(2, e3.revision());
        assertEquals(2, e3.updateCounter());
        assertArrayEquals(val3, e3.value());

        // "Success" branch isn't applied.
        Entry e2 = storage.get(key2);

        assertTrue(e2.empty());
    }

    @Test
    public void invokeWithValueCondition_successBranch() {
        byte[] key1 = k(1);
        byte[] val1_1 = kv(1, 11);
        byte[] val1_2 = kv(1, 12);

        byte[] key2 = k(2);
        byte[] val2 = kv(2, 2);

        byte[] key3 = k(3);
        byte[] val3 = kv(3, 3);

        assertEquals(0, storage.revision());
        assertEquals(0, storage.updateCounter());

        storage.put(key1, val1_1);

        assertEquals(1, storage.revision());
        assertEquals(1, storage.updateCounter());

        boolean branch = storage.invoke(
            new ValueCondition(ValueCondition.Type.EQUAL, key1, val1_1),
            List.of(
                new Operation(OperationType.PUT, key1, val1_2),
                new Operation(OperationType.PUT, key2, val2)
            ),
            List.of(new Operation(OperationType.PUT, key3, val3))
        );

        // "Success" branch is applied.
        assertTrue(branch);
        assertEquals(2, storage.revision());
        assertEquals(3, storage.updateCounter());

        Entry e1 = storage.get(key1);

        assertFalse(e1.empty());
        assertFalse(e1.tombstone());
        assertEquals(2, e1.revision());
        assertEquals(2, e1.updateCounter());
        assertArrayEquals(val1_2, e1.value());

        Entry e2 = storage.get(key2);

        assertFalse(e2.empty());
        assertFalse(e2.tombstone());
        assertEquals(2, e2.revision());
        assertEquals(3, e2.updateCounter());
        assertArrayEquals(val2, e2.value());

        // "Failure" branch isn't applied.
        Entry e3 = storage.get(key3);

        assertTrue(e3.empty());
    }

    @Test
    public void invokeWithValueCondition_failureBranch() {
        byte[] key1 = k(1);
        byte[] val1_1 = kv(1, 11);
        byte[] val1_2 = kv(1, 12);

        byte[] key2 = k(2);
        byte[] val2 = kv(2, 2);

        byte[] key3 = k(3);
        byte[] val3 = kv(3, 3);

        assertEquals(0, storage.revision());
        assertEquals(0, storage.updateCounter());

        storage.put(key1, val1_1);

        assertEquals(1, storage.revision());
        assertEquals(1, storage.updateCounter());

        boolean branch = storage.invoke(
            new ValueCondition(ValueCondition.Type.EQUAL, key1, val1_2),
            List.of(new Operation(OperationType.PUT, key3, val3)),
            List.of(
                new Operation(OperationType.PUT, key1, val1_2),
                new Operation(OperationType.PUT, key2, val2)
            )
        );

        // "Failure" branch is applied.
        assertFalse(branch);
        assertEquals(2, storage.revision());
        assertEquals(3, storage.updateCounter());

        Entry e1 = storage.get(key1);

        assertFalse(e1.empty());
        assertFalse(e1.tombstone());
        assertEquals(2, e1.revision());
        assertEquals(2, e1.updateCounter());
        assertArrayEquals(val1_2, e1.value());

        Entry e2 = storage.get(key2);

        assertFalse(e2.empty());
        assertFalse(e2.tombstone());
        assertEquals(2, e2.revision());
        assertEquals(3, e2.updateCounter());
        assertArrayEquals(val2, e2.value());

        // "Success" branch isn't applied.
        Entry e3 = storage.get(key3);

        assertTrue(e3.empty());
    }

    @Test
    public void invokeOperations() {
        byte[] key1 = k(1);
        byte[] val1 = kv(1, 1);

        byte[] key2 = k(2);
        byte[] val2 = kv(2, 2);

        byte[] key3 = k(3);
        byte[] val3 = kv(3, 3);

        assertEquals(0, storage.revision());
        assertEquals(0, storage.updateCounter());

        storage.put(key1, val1);

        assertEquals(1, storage.revision());
        assertEquals(1, storage.updateCounter());

        // No-op.
        boolean branch = storage.invoke(
            new ValueCondition(ValueCondition.Type.EQUAL, key1, val1),
            List.of(new Operation(OperationType.NO_OP, null, null)),
            List.of(new Operation(OperationType.NO_OP, null, null))
        );

        assertTrue(branch);

        // No updates.
        assertEquals(1, storage.revision());
        assertEquals(1, storage.updateCounter());

        // Put.
        branch = storage.invoke(
            new ValueCondition(ValueCondition.Type.EQUAL, key1, val1),
            List.of(
                new Operation(OperationType.PUT, key2, val2),
                new Operation(OperationType.PUT, key3, val3)
            ),
            List.of(new Operation(OperationType.NO_OP, null, null))
        );

        assertTrue(branch);

        // +1 for revision, +2 for update counter.
        assertEquals(2, storage.revision());
        assertEquals(3, storage.updateCounter());

        Entry e2 = storage.get(key2);

        assertFalse(e2.empty());
        assertFalse(e2.tombstone());
        assertEquals(2, e2.revision());
        assertEquals(2, e2.updateCounter());
        assertArrayEquals(key2, e2.key());
        assertArrayEquals(val2, e2.value());

        Entry e3 = storage.get(key3);

        assertFalse(e3.empty());
        assertFalse(e3.tombstone());
        assertEquals(2, e3.revision());
        assertEquals(3, e3.updateCounter());
        assertArrayEquals(key3, e3.key());
        assertArrayEquals(val3, e3.value());

        // Remove.
        branch = storage.invoke(
            new ValueCondition(ValueCondition.Type.EQUAL, key1, val1),
            List.of(
                new Operation(OperationType.REMOVE, key2, null),
                new Operation(OperationType.REMOVE, key3, null)
            ),
            List.of(new Operation(OperationType.NO_OP, null, null))
        );

        assertTrue(branch);

        // +1 for revision, +2 for update counter.
        assertEquals(3, storage.revision());
        assertEquals(5, storage.updateCounter());

        e2 = storage.get(key2);

        assertFalse(e2.empty());
        assertTrue(e2.tombstone());
        assertEquals(3, e2.revision());
        assertEquals(4, e2.updateCounter());
        assertArrayEquals(key2, e2.key());

        e3 = storage.get(key3);

        assertFalse(e3.empty());
        assertTrue(e3.tombstone());
        assertEquals(3, e3.revision());
        assertEquals(5, e3.updateCounter());
        assertArrayEquals(key3, e3.key());
    }

    @Test
    public void compact() {
        assertEquals(0, storage.revision());
        assertEquals(0, storage.updateCounter());

        // Compact empty.
        storage.compact();

        assertEquals(0, storage.revision());
        assertEquals(0, storage.updateCounter());

        // Compact non-empty.
        fill(storage, 1, 1);

        assertEquals(1, storage.revision());
        assertEquals(1, storage.updateCounter());

        fill(storage, 2, 2);

        assertEquals(3, storage.revision());
        assertEquals(3, storage.updateCounter());

        fill(storage, 3, 3);

        assertEquals(6, storage.revision());
        assertEquals(6, storage.updateCounter());

        storage.getAndRemove(k(3));

        assertEquals(7, storage.revision());
        assertEquals(7, storage.updateCounter());
        assertTrue(storage.get(k(3)).tombstone());

        storage.compact();

        assertEquals(7, storage.revision());
        assertEquals(7, storage.updateCounter());

        Entry e1 = storage.get(k(1));

        assertFalse(e1.empty());
        assertFalse(e1.tombstone());
        assertArrayEquals(k(1), e1.key());
        assertArrayEquals(kv(1,1), e1.value());
        assertEquals(1, e1.revision());
        assertEquals(1, e1.updateCounter());

        Entry e2 = storage.get(k(2));

        assertFalse(e2.empty());
        assertFalse(e2.tombstone());
        assertArrayEquals(k(2), e2.key());
        assertArrayEquals(kv(2,2), e2.value());
        assertTrue(storage.get(k(2), 2).empty());
        assertEquals(3, e2.revision());
        assertEquals(3, e2.updateCounter());

        Entry e3 = storage.get(k(3));

        assertTrue(e3.empty());
        assertTrue(storage.get(k(3), 5).empty());
        assertTrue(storage.get(k(3), 6).empty());
        assertTrue(storage.get(k(3), 7).empty());
    }

    @Test
    public void rangeCursor() {
        byte[] key1 = k(1);
        byte[] val1 = kv(1, 1);

        byte[] key2 = k(2);
        byte[] val2 = kv(2, 2);

        byte[] key3 = k(3);
        byte[] val3 = kv(3, 3);

        assertEquals(0, storage.revision());
        assertEquals(0, storage.updateCounter());

        storage.putAll(List.of(key1, key2, key3), List.of(val1, val2, val3));

        assertEquals(1, storage.revision());
        assertEquals(3, storage.updateCounter());

        // Range for latest revision without max bound.
        Cursor<Entry> cur = storage.range(key1, null);

        Iterator<Entry> it = cur.iterator();

        assertTrue(it.hasNext());

        Entry e1 = it.next();

        assertFalse(e1.empty());
        assertFalse(e1.tombstone());
        assertArrayEquals(key1, e1.key());
        assertArrayEquals(val1, e1.value());
        assertEquals(1, e1.revision());
        assertEquals(1, e1.updateCounter());

        assertTrue(it.hasNext());

        Entry e2 = it.next();

        assertFalse(e2.empty());
        assertFalse(e2.tombstone());
        assertArrayEquals(key2, e2.key());
        assertArrayEquals(val2, e2.value());
        assertEquals(1, e2.revision());
        assertEquals(2, e2.updateCounter());

        // Deliberately don't call it.hasNext()

        Entry e3 = it.next();

        assertFalse(e3.empty());
        assertFalse(e3.tombstone());
        assertArrayEquals(key3, e3.key());
        assertArrayEquals(val3, e3.value());
        assertEquals(1, e3.revision());
        assertEquals(3, e3.updateCounter());

        assertFalse(it.hasNext());

        try {
            it.next();

            fail();
        }
        catch (NoSuchElementException e) {
            System.out.println();
            // No-op.
        }

        // Range for latest revision with max bound.
        cur = storage.range(key1, key3);

        it = cur.iterator();

        assertTrue(it.hasNext());

        e1 = it.next();

        assertFalse(e1.empty());
        assertFalse(e1.tombstone());
        assertArrayEquals(key1, e1.key());
        assertArrayEquals(val1, e1.value());
        assertEquals(1, e1.revision());
        assertEquals(1, e1.updateCounter());

        assertTrue(it.hasNext());

        e2 = it.next();

        assertFalse(e2.empty());
        assertFalse(e2.tombstone());
        assertArrayEquals(key2, e2.key());
        assertArrayEquals(val2, e2.value());
        assertEquals(1, e2.revision());
        assertEquals(2, e2.updateCounter());

        assertFalse(it.hasNext());

        try {
            it.next();

            fail();
        }
        catch (NoSuchElementException e) {
            System.out.println();
            // No-op.
        }
    }

    @Test
    public void watchCursorForRange() throws Exception {
        byte[] key1 = k(1);
        byte[] val1_1 = kv(1, 11);

        byte[] key2 = k(2);
        byte[] val2_1 = kv(2, 21);
        byte[] val2_2 = kv(2, 22);

        byte[] key3 = k(3);
        byte[] val3_1 = kv(3, 31);

        assertEquals(0, storage.revision());
        assertEquals(0, storage.updateCounter());

        // Watch for all updates starting from revision 2.
        Cursor<WatchEvent> cur = storage.watch(key1, null, 2);

        Iterator<WatchEvent> it = cur.iterator();

        assertFalse(it.hasNext());
        assertNull(it.next());

        storage.putAll(List.of(key1, key2), List.of(val1_1, val2_1));

        assertEquals(1, storage.revision());
        assertEquals(2, storage.updateCounter());

        // Revision is less than 2.
        assertFalse(it.hasNext());
        assertNull(it.next());

        storage.putAll(List.of(key2, key3), List.of(val2_2, val3_1));

        assertEquals(2, storage.revision());
        assertEquals(4, storage.updateCounter());

        // Revision is 2.
        assertTrue(it.hasNext());

        WatchEvent watchEvent = it.next();

        assertFalse(watchEvent.single());

        Map<ByteArray, EntryEvent> map = watchEvent.entryEvents().stream()
            .collect(Collectors.toMap(evt -> new ByteArray(evt.entry().key()), identity()));

        assertEquals(2, map.size());

        // First update under revision.
        EntryEvent e2 = map.get(new ByteArray(key2));

        assertNotNull(e2);

        Entry oldEntry2 = e2.oldEntry();

        assertFalse(oldEntry2.empty());
        assertFalse(oldEntry2.tombstone());
        assertEquals(1, oldEntry2.revision());
        assertEquals(2, oldEntry2.updateCounter());
        assertArrayEquals(key2, oldEntry2.key());
        assertArrayEquals(val2_1, oldEntry2.value());

        Entry newEntry2 = e2.entry();

        assertFalse(newEntry2.empty());
        assertFalse(newEntry2.tombstone());
        assertEquals(2, newEntry2.revision());
        assertEquals(3, newEntry2.updateCounter());
        assertArrayEquals(key2, newEntry2.key());
        assertArrayEquals(val2_2, newEntry2.value());

        // Second update under revision.
        EntryEvent e3 = map.get(new ByteArray(key3));

        assertNotNull(e3);

        Entry oldEntry3 = e3.oldEntry();

        assertTrue(oldEntry3.empty());
        assertFalse(oldEntry3.tombstone());
        assertArrayEquals(key3, oldEntry3.key());

        Entry newEntry3 = e3.entry();

        assertFalse(newEntry3.empty());
        assertFalse(newEntry3.tombstone());
        assertEquals(2, newEntry3.revision());
        assertEquals(4, newEntry3.updateCounter());
        assertArrayEquals(key3, newEntry3.key());
        assertArrayEquals(val3_1, newEntry3.value());

        assertFalse(it.hasNext());

        storage.remove(key1);

        assertTrue(it.hasNext());

        watchEvent = it.next();

        assertTrue(watchEvent.single());

        EntryEvent e1 = watchEvent.entryEvent();

        Entry oldEntry1 = e1.oldEntry();

        assertFalse(oldEntry1.empty());
        assertFalse(oldEntry1.tombstone());
        assertEquals(1, oldEntry1.revision());
        assertEquals(1, oldEntry1.updateCounter());
        assertArrayEquals(key1, oldEntry1.key());
        assertArrayEquals(val1_1, oldEntry1.value());

        Entry newEntry1 = e1.entry();

        assertFalse(newEntry1.empty());
        assertTrue(newEntry1.tombstone());
        assertEquals(3, newEntry1.revision());
        assertEquals(5, newEntry1.updateCounter());
        assertArrayEquals(key1, newEntry1.key());
        assertNull(newEntry1.value());

        assertFalse(it.hasNext());

        cur.close();
    }

    @Test
    public void watchCursorForKey() {
        byte[] key1 = k(1);
        byte[] val1_1 = kv(1, 11);
        byte[] val1_2 = kv(1, 12);

        byte[] key2 = k(2);
        byte[] val2_1 = kv(2, 21);
        byte[] val2_2 = kv(2, 22);

        assertEquals(0, storage.revision());
        assertEquals(0, storage.updateCounter());

        Cursor<WatchEvent> cur = storage.watch(key1, 1);

        Iterator<WatchEvent> it = cur.iterator();

        assertFalse(it.hasNext());
        assertNull(it.next());

        storage.putAll(List.of(key1, key2), List.of(val1_1, val2_1));

        assertEquals(1, storage.revision());
        assertEquals(2, storage.updateCounter());

        assertTrue(it.hasNext());

        WatchEvent watchEvent = it.next();

        assertTrue(watchEvent.single());

        EntryEvent e1 = watchEvent.entryEvent();

        Entry oldEntry1 = e1.oldEntry();

        assertTrue(oldEntry1.empty());
        assertFalse(oldEntry1.tombstone());

        Entry newEntry1 = e1.entry();

        assertFalse(newEntry1.empty());
        assertFalse(newEntry1.tombstone());
        assertEquals(1, newEntry1.revision());
        assertEquals(1, newEntry1.updateCounter());
        assertArrayEquals(key1, newEntry1.key());
        assertArrayEquals(val1_1, newEntry1.value());

        assertFalse(it.hasNext());

        storage.put(key2, val2_2);

        assertFalse(it.hasNext());

        storage.put(key1, val1_2);

        assertTrue(it.hasNext());

        watchEvent = it.next();

        assertTrue(watchEvent.single());

        e1 = watchEvent.entryEvent();

        oldEntry1 = e1.oldEntry();

        assertFalse(oldEntry1.empty());
        assertFalse(oldEntry1.tombstone());
        assertEquals(1, oldEntry1.revision());
        assertEquals(1, oldEntry1.updateCounter());
        assertArrayEquals(key1, newEntry1.key());
        assertArrayEquals(val1_1, newEntry1.value());

        newEntry1 = e1.entry();

        assertFalse(newEntry1.empty());
        assertFalse(newEntry1.tombstone());
        assertEquals(3, newEntry1.revision());
        assertEquals(4, newEntry1.updateCounter());
        assertArrayEquals(key1, newEntry1.key());
        assertArrayEquals(val1_2, newEntry1.value());

        assertFalse(it.hasNext());
    }

    @Test
    public void watchCursorForKeys() {
        byte[] key1 = k(1);
        byte[] val1_1 = kv(1, 11);

        byte[] key2 = k(2);
        byte[] val2_1 = kv(2, 21);
        byte[] val2_2 = kv(2, 22);

        byte[] key3 = k(3);
        byte[] val3_1 = kv(3, 31);
        byte[] val3_2 = kv(3, 32);

        assertEquals(0, storage.revision());
        assertEquals(0, storage.updateCounter());

        Cursor<WatchEvent> cur = storage.watch(List.of(key1, key2), 1);

        Iterator<WatchEvent> it = cur.iterator();

        assertFalse(it.hasNext());
        assertNull(it.next());

        storage.putAll(List.of(key1, key2, key3), List.of(val1_1, val2_1, val3_1));

        assertEquals(1, storage.revision());
        assertEquals(3, storage.updateCounter());

        assertTrue(it.hasNext());

        WatchEvent watchEvent = it.next();

        assertFalse(watchEvent.single());

        assertFalse(it.hasNext());

        storage.put(key2, val2_2);

        assertTrue(it.hasNext());

        watchEvent = it.next();

        assertTrue(watchEvent.single());

        assertFalse(it.hasNext());

        storage.put(key3, val3_2);

        assertFalse(it.hasNext());
    }

    /** */
    private static void fill(KeyValueStorage storage, int keySuffix, int num) {
        for (int i = 0; i < num; i++)
            storage.getAndPut(k(keySuffix), kv(keySuffix, i + 1));
    }

    /** */
    private static byte[] k(int k) {
        return ("key" + k).getBytes();
    }

    /** */
    private static byte[] kv(int k, int v) {
        return ("key" + k + '_' + "val" + v).getBytes();
    }
}
