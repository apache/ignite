/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
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

package org.apache.ignite.internal.storage.index;

import org.apache.ignite.internal.storage.SearchRow;
import org.apache.ignite.internal.util.Cursor;

/**
 * Storage for a Sorted Index.
 *
 * <p>This storage serves as a sorted mapping from a subset of a table's columns (a.k.a. index columns) to a {@link SearchRow}
 * from a {@link org.apache.ignite.internal.storage.PartitionStorage} from the same table.
 *
 * @see org.apache.ignite.schema.definition.index.SortedIndexDefinition
 */
public interface SortedIndexStorage extends AutoCloseable {
    /**
     * Returns the Index Descriptor of this storage.
     */
    SortedIndexDescriptor indexDescriptor();

    /**
     * Returns a factory for creating index rows for this storage.
     */
    IndexRowFactory indexRowFactory();

    /**
     * Returns a class deserializing index columns.
     */
    IndexRowDeserializer indexRowDeserializer();

    /**
     * Adds the given index key and {@link SearchRow} to the index.
     *
     * <p>Putting a new value under the same key will overwrite the previous associated value.
     */
    void put(IndexRow row);

    /**
     * Removes the given key from the index.
     *
     * <p>Removing a non-existent key is a no-op.
     */
    void remove(IndexRow row);

    /**
     * Returns a range of index values between the lower bound (inclusive) and the upper bound (inclusive).
     */
    // TODO: add options https://issues.apache.org/jira/browse/IGNITE-16059
    Cursor<IndexRow> range(IndexRowPrefix lowerBound, IndexRowPrefix upperBound);

    /**
     * Removes all data in this index and frees the associated resources.
     */
    void destroy();
}
