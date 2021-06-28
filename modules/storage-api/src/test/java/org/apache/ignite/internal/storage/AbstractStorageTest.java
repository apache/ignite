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

import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;
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
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Abstract test that covers basic scenarios of the storage API.
 */
public abstract class AbstractStorageTest {
    /** Storage instance. */
    protected Storage storage;

    /**
     * Wraps string key into a search row.
     *
     * @param key String key.
     * @return Search row.
     */
    private SearchRow searchRow(String key) {
        return new SimpleDataRow(
            key.getBytes(StandardCharsets.UTF_8),
            null
        );
    }

    /**
     * Wraps string key/value pair into a data row.
     *
     * @param key String key.
     * @param value String value.
     * @return Data row.
     */
    private DataRow dataRow(String key, String value) {
        return new SimpleDataRow(
            key.getBytes(StandardCharsets.UTF_8),
            value.getBytes(StandardCharsets.UTF_8)
        );
    }

    /**
     * Tests that read / write / remove work consistently on the same key.
     *
     * @throws Exception If failed.
     */
    @Test
    public void readWriteRemove() throws Exception {
        SearchRow searchRow = searchRow("key");

        assertNull(storage.read(searchRow).value());

        DataRow dataRow = dataRow("key", "value");

        storage.write(dataRow);

        assertArrayEquals(dataRow.value().array(), storage.read(searchRow).value().array());

        storage.remove(searchRow);

        assertNull(storage.read(searchRow).value());
    }

    /**
     * Tests that invoke method works consistently with default read / write / remove closures implementations on the
     *      same key.
     *
     * @throws Exception If failed.
     */
    @Test
    public void invoke() throws Exception {
        SearchRow searchRow = searchRow("key");

        SimpleReadInvokeClosure readClosure = new SimpleReadInvokeClosure();

        storage.invoke(searchRow, readClosure);

        assertNull(readClosure.row().value());

        DataRow dataRow = dataRow("key", "value");

        storage.invoke(searchRow, new SimpleWriteInvokeClosure(dataRow));

        storage.invoke(searchRow, readClosure);

        assertArrayEquals(dataRow.value().array(), readClosure.row().value().array());

        storage.invoke(searchRow, new SimpleRemoveInvokeClosure());

        storage.invoke(searchRow, readClosure);

        assertNull(readClosure.row().value());
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
     * Converts cursor to list.
     *
     * @param cursor Cursor.
     * @param <T> Type of cursor content.
     * @return List.
     * @throws Exception If error occurred during iteration or while closing the cursor.
     */
    @NotNull
    private <T> List<T> toList(Cursor<T> cursor) throws Exception {
        try (cursor) {
            return StreamSupport.stream(cursor.spliterator(), false).collect(Collectors.toList());
        }
    }
}
