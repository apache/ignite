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

package org.apache.ignite.internal.storage;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;
import org.apache.ignite.internal.storage.basic.DeleteExactInvokeClosure;
import org.apache.ignite.internal.storage.basic.GetAndRemoveInvokeClosure;
import org.apache.ignite.internal.storage.basic.GetAndReplaceInvokeClosure;
import org.apache.ignite.internal.storage.basic.InsertInvokeClosure;
import org.apache.ignite.internal.storage.basic.ReplaceExactInvokeClosure;
import org.apache.ignite.internal.storage.basic.SimpleDataRow;
import org.apache.ignite.internal.storage.basic.SimpleReadInvokeClosure;
import org.apache.ignite.internal.storage.basic.SimpleRemoveInvokeClosure;
import org.apache.ignite.internal.storage.basic.SimpleWriteInvokeClosure;
import org.apache.ignite.internal.util.Cursor;
import org.jetbrains.annotations.NotNull;
import org.junit.jupiter.api.Test;

import static java.util.Collections.emptyList;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.collection.IsCollectionWithSize.hasSize;
import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Abstract test that covers basic scenarios of the storage API.
 */
public abstract class AbstractStorageTest {
    /** Test key. */
    private static final String KEY = "key";

    /** Test value. */
    private static final String VALUE = "value";

    /** Storage instance. */
    protected Storage storage;

    /**
     * Tests that read / write / remove work consistently on the same key.
     */
    @Test
    public void readWriteRemove() {
        SearchRow searchRow = searchRow(KEY);

        assertNull(storage.read(searchRow));

        DataRow dataRow = dataRow(KEY, VALUE);

        storage.write(dataRow);

        assertArrayEquals(dataRow.value().array(), storage.read(searchRow).value().array());

        storage.remove(searchRow);

        assertNull(storage.read(searchRow));
    }

    /**
     * Tests that invoke method works consistently with default read / write / remove closures implementations on the
     *      same key.
     */
    @Test
    public void invoke() {
        SearchRow searchRow = searchRow(KEY);

        SimpleReadInvokeClosure readClosure = new SimpleReadInvokeClosure();

        storage.invoke(searchRow, readClosure);

        assertNull(readClosure.row());

        DataRow dataRow = dataRow(KEY, VALUE);

        storage.invoke(searchRow, new SimpleWriteInvokeClosure(dataRow));

        storage.invoke(searchRow, readClosure);

        assertArrayEquals(dataRow.value().array(), readClosure.row().value().array());

        storage.invoke(searchRow, new SimpleRemoveInvokeClosure());

        storage.invoke(searchRow, readClosure);

        assertNull(readClosure.row());
    }

    /**
     * Tests that scan operation works properly without filter.
     *
     * @throws Exception If failed.
     */
    @Test
    public void scanSimple() throws Exception {
        List<DataRow> list = toList(storage.scan(key -> true));

        assertEquals(emptyList(), list);

        DataRow dataRow1 = dataRow("key1", "value1");

        storage.write(dataRow1);

        list = toList(storage.scan(key -> true));

        assertThat(list, hasSize(1));

        assertArrayEquals(dataRow1.value().array(), list.get(0).value().array());

        DataRow dataRow2 = dataRow("key2", "value2");

        storage.write(dataRow2);

        list = toList(storage.scan(key -> true));

        assertThat(list, hasSize(2));

        // "key1" and "key2" have the same order both by hash and lexicographically.
        assertArrayEquals(dataRow1.value().array(), list.get(0).value().array());
        assertArrayEquals(dataRow2.value().array(), list.get(1).value().array());
    }

    /**
     * Tests that scan operation works properly with passed filter.
     *
     * @throws Exception If failed.
     */
    @Test
    public void scanFiltered() throws Exception {
        DataRow dataRow1 = dataRow("key1", "value1");
        DataRow dataRow2 = dataRow("key2", "value2");

        storage.write(dataRow1);
        storage.write(dataRow2);

        List<DataRow> list = toList(storage.scan(key -> key.keyBytes()[3] == '1'));

        assertThat(list, hasSize(1));

        assertArrayEquals(dataRow1.value().array(), list.get(0).value().array());

        list = toList(storage.scan(key -> key.keyBytes()[3] == '2'));

        assertThat(list, hasSize(1));

        assertArrayEquals(dataRow2.value().array(), list.get(0).value().array());

        list = toList(storage.scan(key -> false));

        assertTrue(list.isEmpty());
    }

    /**
     * Tests that {@link InsertInvokeClosure} inserts a data row if there is no existing data row with the same key.
     */
    @Test
    public void testInsertClosure() {
        DataRow dataRow = dataRow(KEY, VALUE);

        var closure = new InsertInvokeClosure(dataRow);

        storage.invoke(dataRow, closure);

        assertTrue(closure.result());

        checkHasSameEntry(dataRow);
    }

    /**
     * Tests that {@link InsertInvokeClosure} doesn't insert a data row if there is an existing data row with the same key.
     */
    @Test
    public void testInsertClosure_failureBranch() {
        DataRow dataRow = dataRow(KEY, VALUE);

        var closure = new InsertInvokeClosure(dataRow);

        storage.invoke(dataRow, closure);

        assertTrue(closure.result());

        DataRow sameKeyRow = dataRow(KEY, "test");

        var sameClosure = new InsertInvokeClosure(sameKeyRow);

        storage.invoke(sameKeyRow, sameClosure);

        assertFalse(sameClosure.result());

        checkHasSameEntry(dataRow);
    }

    /**
     * Tests that {@link DeleteExactInvokeClosure} deletes a data row if a key and a value matches the ones passed
     * in the closure.
     */
    @Test
    public void testDeleteExactClosure() {
        DataRow dataRow = dataRow(KEY, VALUE);

        storage.write(dataRow);

        checkHasSameEntry(dataRow);

        var closure = new DeleteExactInvokeClosure(dataRow);

        storage.invoke(dataRow, closure);

        assertTrue(closure.result());

        assertNull(storage.read(dataRow));
    }

    /**
     * Tests that {@link DeleteExactInvokeClosure} doesn't delete a data row if a key and a value don't match
     * the ones passed in the closure.
     */
    @Test
    public void testDeleteExactClosure_failureBranch() {
        DataRow dataRow = dataRow(KEY, VALUE);

        storage.write(dataRow);

        checkHasSameEntry(dataRow);

        var closure = new DeleteExactInvokeClosure(dataRow(KEY, "test"));

        storage.invoke(dataRow, closure);

        assertFalse(closure.result());

        checkHasSameEntry(dataRow);
    }

    /**
     * Tests that {@link GetAndRemoveInvokeClosure} successfully retrieves and removes a data row.
     */
    @Test
    public void testGetAndRemoveClosure() {
        DataRow dataRow = dataRow(KEY, VALUE);

        storage.write(dataRow);

        checkHasSameEntry(dataRow);

        var closure = new GetAndRemoveInvokeClosure();

        storage.invoke(dataRow, closure);

        assertTrue(closure.result());

        checkRowsEqual(dataRow, closure.oldRow());

        assertNull(storage.read(dataRow));
    }

    /**
     * Tests that {@link GetAndRemoveInvokeClosure} doesn't retrieve and remove a data row if it doesn't exist.
     */
    @Test
    public void testGetAndRemoveClosure_failureBranch() {
        DataRow dataRow = dataRow(KEY, VALUE);

        storage.write(dataRow);

        checkHasSameEntry(dataRow);

        var closure = new GetAndRemoveInvokeClosure();

        storage.invoke(searchRow("test"), closure);

        assertFalse(closure.result());

        assertNull(closure.oldRow());

        checkHasSameEntry(dataRow);
    }

    /**
     * Tests that {@link GetAndReplaceInvokeClosure} with the {@code GetAndReplaceInvokeClosure#onlyIfExists} set to
     * {@code false} retrieves and replaces the existing entry in the storage.
     */
    @Test
    public void testGetAndReplaceClosureIfExistsFalse_entryExists() {
        String newValue = "newValue";

        DataRow dataRow = dataRow(KEY, VALUE);

        storage.write(dataRow);

        checkHasSameEntry(dataRow);

        DataRow newRow = dataRow(KEY, newValue);

        var closure = new GetAndReplaceInvokeClosure(newRow, false);

        storage.invoke(dataRow, closure);

        DataRow replaced = closure.oldRow();

        assertNotNull(replaced);

        assertTrue(closure.result());

        checkRowsEqual(dataRow, replaced);

        checkHasDifferentEntry(dataRow);
        checkHasSameEntry(newRow);
    }

    /**
     * Tests that {@link GetAndReplaceInvokeClosure} with the {@code GetAndReplaceInvokeClosure#onlyIfExists} set to
     * {@code false} successfully inserts a new data row and returns an empty row if a previous row with the same key
     * doesn't exist .
     */
    @Test
    public void testGetAndReplaceClosureIfExistsFalse_entryNotExists() {
        DataRow dataRow = dataRow(KEY, VALUE);

        var closure = new GetAndReplaceInvokeClosure(dataRow, false);

        storage.invoke(dataRow, closure);

        DataRow replaced = closure.oldRow();

        assertTrue(closure.result());

        assertNull(replaced);

        checkHasSameEntry(dataRow);
    }

    /**
     * Tests that {@link GetAndReplaceInvokeClosure} with the {@code GetAndReplaceInvokeClosure#onlyIfExists} set to
     * {@code true} retrieves and replaces the existing entry in the storage.
     */
    @Test
    public void testGetAndReplaceClosureIfExistsTrue_entryExists() {
        DataRow dataRow = dataRow(KEY, VALUE);

        storage.write(dataRow);

        checkHasSameEntry(dataRow);

        DataRow newRow = dataRow(KEY, "test");

        var closure = new GetAndReplaceInvokeClosure(newRow, true);

        storage.invoke(dataRow, closure);

        DataRow replaced = closure.oldRow();

        assertNotNull(replaced);

        assertTrue(closure.result());

        checkHasDifferentEntry(dataRow);
        checkHasSameEntry(newRow);
    }

    /**
     * Tests that {@link GetAndReplaceInvokeClosure} with the {@code GetAndReplaceInvokeClosure#onlyIfExists} set to
     * {@code true} doesn't insert a new entry if a previous one doesn't exist.
     */
    @Test
    public void testGetAndReplaceClosureIfExistsTrue_entryNotExists() {
        DataRow dataRow = dataRow(KEY, VALUE);

        var closure = new GetAndReplaceInvokeClosure(dataRow, true);

        storage.invoke(dataRow, closure);

        DataRow replaced = closure.oldRow();

        assertNull(replaced);

        assertFalse(closure.result());

        assertNull(storage.read(dataRow));
    }

    /**
     * Tests that {@link ReplaceExactInvokeClosure} replaces the data row with a given key and a given value
     * in the storage.
     */
    @Test
    public void testReplaceExactClosure() {
        String newValue = "newValue";

        DataRow dataRow = dataRow(KEY, VALUE);

        storage.write(dataRow);

        checkHasSameEntry(dataRow);

        DataRow newRow = dataRow(KEY, newValue);

        var closure = new ReplaceExactInvokeClosure(dataRow, newRow);

        storage.invoke(dataRow, closure);

        assertTrue(closure.result());

        checkHasDifferentEntry(dataRow);
        checkHasSameEntry(newRow);
    }

    /**
     * Tests that {@link ReplaceExactInvokeClosure} doesn't replace the data row with a given key and a given value
     * if the value in the storage doesn't match the value passed via the closure.
     */
    @Test
    public void testReplaceExactClosure_failureBranch() {
        String newValue = "newValue";

        DataRow dataRow = dataRow(KEY, VALUE);

        storage.write(dataRow);

        checkHasSameEntry(dataRow);

        DataRow newRow = dataRow(KEY, newValue);

        var closure = new ReplaceExactInvokeClosure(newRow, dataRow);

        storage.invoke(dataRow, closure);

        assertFalse(closure.result());

        checkHasSameEntry(dataRow);
    }

    /**
     * Tests the {@link Storage#readAll(List)} operation successfully reads data rows from the storage.
     */
    @Test
    public void testReadAll() {
        List<DataRow> rows = insertBulk(100);

        List<DataRow> rowsFromStorage = new ArrayList<>(storage.readAll(rows));

        Comparator<DataRow> comparator = Comparator.comparing(DataRow::keyBytes, Arrays::compare)
            .thenComparing(DataRow::valueBytes, Arrays::compare);

        rows.sort(comparator);
        rowsFromStorage.sort(comparator);

        assertEquals(rows, rowsFromStorage);
    }

    /**
     * Tests that {@link Storage#writeAll(List)} operation successfully writes a collection of data rows into the
     * storage.
     */
    @Test
    public void testWriteAll() {
        List<DataRow> rows = IntStream.range(0, 100)
            .mapToObj(i -> dataRow(KEY + i, VALUE + i))
            .collect(Collectors.toList());

        storage.writeAll(rows);
        rows.forEach(this::checkHasSameEntry);
    }

    /**
     * Tests that {@link Storage#insertAll(List)} operation doesn't insert data rows which keys
     * are already present in the storage. This operation must also return the list of such data rows.
     */
    @Test
    public void testInsertAll() {
        List<DataRow> rows = insertBulk(100);

        List<DataRow> oldRows = rows.subList(0, 50);

        List<DataRow> newInsertion = Stream.concat(
            oldRows.stream(),
            IntStream.range(100, 150).mapToObj(i -> dataRow(KEY + "_" + i, VALUE + "_" + i))
        ).collect(Collectors.toList());

        Collection<DataRow> cantInsert = storage.insertAll(newInsertion);

        assertEquals(oldRows, cantInsert);
    }

    /**
     * Tests that {@link Storage#removeAll(List)} operation successfully retrieves and removes a collection of
     * {@link SearchRow}s.
     */
    @Test
    public void testRemoveAll() throws Exception {
        List<DataRow> rows = insertBulk(100);

        Collection<SearchRow> skipped = storage.removeAll(rows);

        assertEquals(0, skipped.size());

        Cursor<DataRow> scan = storage.scan(row -> true);

        assertFalse(scan.hasNext());

        scan.close();
    }

    @Test
    public void testRemoveAllKeyNotExists() {
        SearchRow row = searchRow(KEY);
        Collection<SearchRow> skipped = storage.removeAll(Collections.singletonList(row));

        assertNotNull(skipped);

        assertEquals(1, skipped.size());
        assertEquals(row, skipped.iterator().next());
    }

    /**
     * Tests that {@link Storage#removeAllExact(List)} operation successfully removes and retrieves a collection
     * of data rows with the given exact keys and values from the storage.
     */
    @Test
    public void testRemoveAllExact() throws Exception {
        List<DataRow> rows = insertBulk(100);

        Collection<DataRow> skipped = storage.removeAllExact(rows);

        assertEquals(0, skipped.size());

        Cursor<DataRow> scan = storage.scan(row -> true);

        assertFalse(scan.hasNext());

        scan.close();
    }

    /**
     * Tests that {@link Storage#removeAllExact(List)} operation doesn't remove and retrieve a collection
     * of data rows with the given exact keys and values from the storage if the value in the storage doesn't match
     * the given value.
     */
    @Test
    public void testRemoveAllExact_failureBranch() {
        List<DataRow> rows = insertBulk(100);

        List<DataRow> notExactRows = IntStream.range(0, 100)
            .mapToObj(i -> dataRow(KEY + i, VALUE + (i + 1)))
            .collect(Collectors.toList());

        Collection<DataRow> skipped = storage.removeAllExact(notExactRows);

        assertEquals(notExactRows, skipped);

        rows.forEach(this::checkHasSameEntry);
    }

    /**
     * Inserts and returns a given amount of data rows with {@link #KEY}_i as a key and {@link #VALUE}_i as a value
     * where i is an index of the data row.
     * 
     * @param numberOfEntries Amount of entries to insert.
     * @return List of inserted rows.
     */
    private List<DataRow> insertBulk(int numberOfEntries) {
        List<DataRow> rows = IntStream.range(0, numberOfEntries)
            .mapToObj(i -> dataRow(KEY + "_" + i, VALUE + "_" + i))
            .collect(Collectors.toList());

        storage.insertAll(rows);
        rows.forEach(this::checkHasSameEntry);

        // Clone key and value byte arrays so that returned rows have new references.
        // This way we check that the ConcurrentHashMapStorage performs an actual array comparison.
        return rows.stream().map(row -> {
            byte[] valueBytes = row.valueBytes();

            assert valueBytes != null;

            return new SimpleDataRow(row.keyBytes().clone(), valueBytes.clone());
        }).collect(Collectors.toList());
    }

    /**
     * Checks that the storage contains a row with a given key but with a different value.
     *
     * @param row Data row.
     */
    private void checkHasDifferentEntry(DataRow row) {
        DataRow read = storage.read(row);

        assertNotNull(read);
        assertFalse(Arrays.equals(row.valueBytes(), read.valueBytes()));
    }

    /**
     * Checks that the storage contains a specific data row.
     *
     * @param row Expected data row.
     */
    private void checkHasSameEntry(DataRow row) {
        DataRow read = storage.read(row);

        assertNotNull(read);
        checkRowsEqual(row, read);
    }

    /**
     * Checks that two rows are equal.
     *
     * @param expected Expected data row.
     * @param actual Actual data row.
     */
    private static void checkRowsEqual(DataRow expected, DataRow actual) {
        assertArrayEquals(expected.keyBytes(), actual.keyBytes());
        assertArrayEquals(expected.valueBytes(), actual.valueBytes());
    }

    /**
     * Wraps string key into a search row.
     *
     * @param key String key.
     * @return Search row.
     */
    private static SearchRow searchRow(String key) {
        return new SearchRow() {
            @Override public byte @NotNull [] keyBytes() {
                return key.getBytes(StandardCharsets.UTF_8);
            }

            @Override public @NotNull ByteBuffer key() {
                return ByteBuffer.wrap(keyBytes());
            }
        };
    }

    /**
     * Wraps string key/value pair into a data row.
     *
     * @param key String key.
     * @param value String value.
     * @return Data row.
     */
    private static DataRow dataRow(String key, String value) {
        return new SimpleDataRow(
            key.getBytes(StandardCharsets.UTF_8),
            value.getBytes(StandardCharsets.UTF_8)
        );
    }

    /**
     * Converts cursor to list.
     *
     * @param cursor Cursor.
     * @param <T> Type of cursor content.
     * @return List.
     * @throws Exception If error occurred during iteration or while closing the cursor.
     */
    @NotNull
    private static <T> List<T> toList(Cursor<T> cursor) throws Exception {
        try (cursor) {
            return StreamSupport.stream(cursor.spliterator(), false).collect(Collectors.toList());
        }
    }
}
