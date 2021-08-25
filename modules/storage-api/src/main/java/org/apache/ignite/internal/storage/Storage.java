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

import java.nio.file.Path;
import java.util.Collection;
import java.util.concurrent.CompletableFuture;
import java.util.function.Predicate;
import org.apache.ignite.internal.util.Cursor;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

/**
 * Interface providing methods to read, remove and update keys in storage.
 * Any locking is unnecessary as this storage is used within RAFT groups where all write operations are
 * serialized.
 */
public interface Storage extends AutoCloseable {
    /**
     * Reads a DataRow for a given key.
     *
     * @param key Search row.
     * @return Data row.
     * @throws StorageException If failed to read the data or the storage is already stopped.
     */
    public DataRow read(SearchRow key) throws StorageException;

    /**
     * Reads {@link DataRow}s for a given collection of keys.
     *
     * @param keys Search rows.
     * @return Data rows.
     * @throws StorageException If failed to read the data or the storage is already stopped.
     */
    public Collection<DataRow> readAll(Collection<? extends SearchRow> keys);

    /**
     * Writes a DataRow into the storage.
     *
     * @param row Data row.
     * @throws StorageException If failed to write the data or the storage is already stopped.
     */
    public void write(DataRow row) throws StorageException;

    /**
     * Writes a collection of {@link DataRow}s into the storage.
     *
     * @param rows Data rows.
     * @throws StorageException If failed to write the data or the storage is already stopped.
     */
    public void writeAll(Collection<? extends DataRow> rows) throws StorageException;

    /**
     * Inserts a collection of {@link DataRow}s into the storage and returns a collection of rows that
     * can't be inserted due to their keys being already present in the storage.
     *
     * @param rows Data rows.
     * @return Collection of rows that could not be inserted.
     * @throws StorageException If failed to write the data or the storage is already stopped.
     */
    public Collection<DataRow> insertAll(Collection<? extends DataRow> rows) throws StorageException;

    /**
     * Removes a DataRow associated with a given Key.
     *
     * @param key Search row.
     * @throws StorageException If failed to remove the data or the storage is already stopped.
     */
    public void remove(SearchRow key) throws StorageException;

    /**
     * Removes {@link DataRow}s mapped by given keys.
     *
     * @param keys Search rows.
     * @return List of removed data rows.
     * @throws StorageException If failed to remove the data or the storage is already stopped.
     */
    public Collection<DataRow> removeAll(Collection<? extends SearchRow> keys);

    /**
     * Removes {@link DataRow}s mapped by given keys and containing given values.
     *
     * @param keyValues Data rows.
     * @return List of removed data rows.
     * @throws StorageException If failed to remove the data or the storage is already stopped.
     */
    public Collection<DataRow> removeAllExact(Collection<? extends DataRow> keyValues);

    /**
     * Executes an update with custom logic implemented by storage.UpdateClosure interface.
     *
     * @param key Search key.
     * @param clo Invoke closure.
     * @param <T> Closure invocation's result type.
     * @throws StorageException If failed to read data or storage is already stopped.
     */
    @Nullable
    public <T> T invoke(SearchRow key, InvokeClosure<T> clo) throws StorageException;

    /**
     * Creates cursor over the storage data.
     *
     * @param filter Filter for the scan query.
     * @return Cursor with filtered data.
     * @throws StorageException If failed to read data or storage is already stopped.
     */
    public Cursor<DataRow> scan(Predicate<SearchRow> filter) throws StorageException;

    /**
     * Creates a snapshot of the storage's current state in the specified directory.
     *
     * @param snapshotPath Directory to store a snapshot.
     * @return Future representing pending completion of the operation. Can not be {@code null}.
     */
    @NotNull
    CompletableFuture<Void> snapshot(Path snapshotPath);

    /**
     * Restores a state of the storage which was previously captured with a {@link #snapshot(Path)}.
     *
     * @param snapshotPath Path to the snapshot's directory.
     */
    void restoreSnapshot(Path snapshotPath);
}

