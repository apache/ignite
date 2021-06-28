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

import java.util.function.Predicate;
import org.apache.ignite.internal.util.Cursor;

/**
 * Interface providing methods to read, remove and update keys in storage.
 */
public interface Storage {
    /**
     * Reads a DataRow for a given key.
     *
     * @param key Search row.
     * @return Data row.
     * @throws StorageException If failed to read data or storage is already stopped.
     */
    public DataRow read(SearchRow key) throws StorageException;

    /**
     * Writes a DataRow to the storage.
     *
     * @param row Data row.
     * @throws StorageException If failed to read data or storage is already stopped.
     */
    public void write(DataRow row) throws StorageException;

    /**
     * Removes a DataRow associated with a given Key.
     *
     * @param key Search row.
     * @throws StorageException If failed to read data or storage is already stopped.
     */
    public void remove(SearchRow key) throws StorageException;

    /**
     * Executes an update with custom logic implemented by storage.UpdateClosure interface.
     *
     * @param key Search key.
     * @param clo Invoke closure.
     * @throws StorageException If failed to read data or storage is already stopped.
     */
    public void invoke(SearchRow key, InvokeClosure clo) throws StorageException;

    /**
     * Creates cursor over the storage data.
     *
     * @param filter Filter for the scan query.
     * @return Cursor with filtered data.
     * @throws StorageException If failed to read data or storage is already stopped.
     */
    public Cursor<DataRow> scan(Predicate<SearchRow> filter) throws StorageException;
}

