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

package org.apache.ignite.internal.storage.basic;

import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.function.Predicate;
import java.util.stream.Collectors;
import org.apache.ignite.internal.storage.DataRow;
import org.apache.ignite.internal.storage.InvokeClosure;
import org.apache.ignite.internal.storage.SearchRow;
import org.apache.ignite.internal.storage.Storage;
import org.apache.ignite.internal.storage.StorageException;
import org.apache.ignite.internal.util.Cursor;
import org.apache.ignite.lang.ByteArray;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

/**
 * Storage implementation based on {@link ConcurrentHashMap}.
 */
public class ConcurrentHashMapStorage implements Storage {
    /** Storage content. */
    private final ConcurrentMap<ByteArray, byte[]> map = new ConcurrentHashMap<>();

    /** {@inheritDoc} */
    @Override @Nullable public DataRow read(SearchRow key) throws StorageException {
        byte[] keyBytes = key.keyBytes();

        byte[] valueBytes = map.get(new ByteArray(keyBytes));

        return valueBytes == null ? null : new SimpleDataRow(keyBytes, valueBytes);
    }

    /** {@inheritDoc} */
    @Override public Collection<DataRow> readAll(List<? extends SearchRow> keys) {
        return keys.stream()
            .map(this::read)
            .filter(Objects::nonNull)
            .collect(Collectors.toList());
    }

    /** {@inheritDoc} */
    @Override public void write(DataRow row) throws StorageException {
        map.put(new ByteArray(row.keyBytes()), row.valueBytes());
    }

    /** {@inheritDoc} */
    @Override public void writeAll(List<? extends DataRow> rows) throws StorageException {
        rows.forEach(this::write);
    }

    /** {@inheritDoc} */
    @Override public Collection<DataRow> insertAll(List<? extends DataRow> rows) throws StorageException {
        return rows.stream()
            .map(row -> map.putIfAbsent(new ByteArray(row.keyBytes()), row.valueBytes()) == null ? null : row)
            .filter(Objects::nonNull)
            .collect(Collectors.toList());
    }

    /** {@inheritDoc} */
    @Override public void remove(SearchRow key) throws StorageException {
        map.remove(new ByteArray(key.keyBytes()));
    }

    /** {@inheritDoc} */
    @Override public Collection<SearchRow> removeAll(List<? extends SearchRow> keys) {
        var skippedRows = new ArrayList<SearchRow>(keys.size());

        for (SearchRow key : keys) {
            byte[] keyBytes = key.keyBytes();

            byte[] removedValueBytes = map.remove(new ByteArray(keyBytes));

            if (removedValueBytes == null)
                skippedRows.add(key);
        }

        return skippedRows;
    }

    /** {@inheritDoc} */
    @Override public Collection<DataRow> removeAllExact(List<? extends DataRow> keyValues) {
        var skippedRows = new ArrayList<DataRow>(keyValues.size());

        for (DataRow row : keyValues) {
            var key = new ByteArray(row.keyBytes());

            byte[] existingValueBytes = map.get(key);

            if (Arrays.equals(existingValueBytes, row.valueBytes()))
                map.remove(key);
            else
                skippedRows.add(row);
        }

        return skippedRows;
    }

    /** {@inheritDoc} */
    @Nullable
    @Override public <T> T invoke(SearchRow key, InvokeClosure<T> clo) throws StorageException {
        byte[] keyBytes = key.keyBytes();

        ByteArray mapKey = new ByteArray(keyBytes);

        byte[] existingDataBytes = map.get(mapKey);

        clo.call(existingDataBytes == null ? null : new SimpleDataRow(keyBytes, existingDataBytes));

        switch (clo.operationType()) {
            case WRITE:
                DataRow newRow = clo.newRow();

                assert newRow != null;

                map.put(mapKey, newRow.valueBytes());

                break;

            case REMOVE:
                map.remove(mapKey);

                break;

            case NOOP:
                break;
        }

        return clo.result();
    }

    /** {@inheritDoc} */
    @Override public Cursor<DataRow> scan(Predicate<SearchRow> filter) throws StorageException {
        Iterator<SimpleDataRow> iter = map.entrySet().stream()
            .map(e -> new SimpleDataRow(e.getKey().bytes(), e.getValue()))
            .filter(filter)
            .iterator();

        return new Cursor<>() {
            /** {@inheritDoc} */
            @Override public boolean hasNext() {
                return iter.hasNext();
            }

            /** {@inheritDoc} */
            @Override public DataRow next() {
                return iter.next();
            }

            /** {@inheritDoc} */
            @NotNull @Override public Iterator<DataRow> iterator() {
                return this;
            }

            /** {@inheritDoc} */
            @Override public void close() {
                // No-op.
            }
        };
    }

    /** {@inheritDoc} */
    @Override public @NotNull CompletableFuture<Void> snapshot(Path snapshotPath) {
        throw new UnsupportedOperationException("Not implemented!");
    }

    /** {@inheritDoc} */
    @Override public void restoreSnapshot(Path snapshotPath) {
        throw new UnsupportedOperationException("Not implemented!");
    }

    /** {@inheritDoc} */
    @Override public void close() throws Exception {
        // No-op.
    }
}
