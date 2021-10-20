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

package org.apache.ignite.internal.storage.engine;

import org.apache.ignite.configuration.schemas.table.TableConfiguration;
import org.apache.ignite.internal.storage.PartitionStorage;
import org.apache.ignite.internal.storage.StorageException;
import org.jetbrains.annotations.Nullable;

/**
 * Table storage that contains meta, partitions and SQL indexes.
 */
public interface TableStorage {
    /**
     * Retrieves or creates a partition for the current table. Not expected to be called concurrently with the same
     * partition id.
     *
     * @param partId Partition id.
     * @return Partition storage.
     * @throws IllegalArgumentException If partition id is out of bounds.
     * @throws StorageException If an error has occurred during the partition creation.
     */
    PartitionStorage getOrCreatePartition(int partId) throws StorageException;

    /**
     * Returns the partition storage or {@code null} if the requested storage doesn't exist.
     *
     * @param partId Partition id.
     * @return Partition storage or {@code null}.
     * @throws IllegalArgumentException If partition id is out of bounds.
     */
    @Nullable
    PartitionStorage getPartition(int partId);

    /**
     * Destroys a partition if it exists.
     *
     * @param partId Partition id.
     * @throws IllegalArgumentException If partition id is out of bounds.
     * @throws StorageException If an error has occurred during the partition destruction.
     */
    void dropPartition(int partId) throws StorageException;

    /**
     * Returns the table configuration.
     *
     * @return Table configuration.
     */
    TableConfiguration configuration();

    /**
     * Returns the data region containing table's data.
     *
     * @return Data region containing table's data.
     */
    DataRegion dataRegion();

    /**
     * Starts the storage.
     *
     * @throws StorageException If an error has occurred during the start of the storage.
     */
    void start() throws StorageException;

    /**
     * Stops the storage.
     *
     * @throws StorageException If an error has occurred during the stop of the storage.
     */
    void stop() throws StorageException;

    /**
     * Stops and destroys the storage and cleans all allocated resources.
     *
     * @throws StorageException If an error has occurred during the destruction of the storage.
     */
    void destroy() throws StorageException;
}
