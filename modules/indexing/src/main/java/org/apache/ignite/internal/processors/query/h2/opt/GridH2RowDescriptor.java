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

package org.apache.ignite.internal.processors.query.h2.opt;

import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.internal.processors.cache.CacheObject;
import org.apache.ignite.internal.processors.cache.GridCacheContext;
import org.apache.ignite.internal.processors.query.GridQueryTypeDescriptor;
import org.apache.ignite.internal.processors.query.h2.IgniteH2Indexing;
import org.apache.ignite.internal.util.offheap.unsafe.GridOffHeapSmartPointerFactory;
import org.apache.ignite.internal.util.offheap.unsafe.GridUnsafeGuard;
import org.apache.ignite.internal.util.offheap.unsafe.GridUnsafeMemory;
import org.h2.value.Value;
import org.jetbrains.annotations.Nullable;

/**
 * Row descriptor.
 */
public interface GridH2RowDescriptor extends GridOffHeapSmartPointerFactory<GridH2KeyValueRowOffheap> {
    /**
     * Gets indexing.
     *
     * @return indexing.
     */
    public IgniteH2Indexing indexing();

    /**
     * Gets type descriptor.
     *
     * @return Type descriptor.
     */
    public GridQueryTypeDescriptor type();

    /**
     * Gets cache context for this row descriptor.
     *
     * @return Cache context.
     */
    public GridCacheContext<?, ?> context();

    /**
     * @return Cache configuration.
     */
    public CacheConfiguration configuration();

    /**
     * Creates new row.
     *
     * @param key Key.
     * @param val Value.
     * @param expirationTime Expiration time in millis.
     * @return Row.
     * @throws IgniteCheckedException If failed.
     */
    public GridH2Row createRow(CacheObject key, @Nullable CacheObject val, long expirationTime)
        throws IgniteCheckedException;

    /**
     * @param key Cache key.
     * @return Value.
     * @throws IgniteCheckedException If failed.
     */
    public Object readFromSwap(Object key) throws IgniteCheckedException;

    /**
     * @return Value type.
     */
    public int valueType();

    /**
     * @return Total fields count.
     */
    public int fieldsCount();

    /**
     * Gets value type for column index.
     *
     * @param col Column index.
     * @return Value type.
     */
    public int fieldType(int col);

    /**
     * Gets column value by column index.
     *
     * @param key Key.
     * @param val Value.
     * @param col Column index.
     * @return  Column value.
     */
    public Object columnValue(Object key, Object val, int col);

    /**
     * Gets column value by column index.
     *
     * @param key Key.
     * @param val Value.
     * @param colVal Value to set to column.
     * @param col Column index.
     */
    public void setColumnValue(Object key, Object val, Object colVal, int col);

    /**
     * Determine whether a column corresponds to a property of key or to one of value.
     *
     * @param col Column index.
     * @return {@code true} if given column corresponds to a key property, {@code false} otherwise
     */
    public boolean isColumnKeyProperty(int col);

    /**
     * @return Unsafe memory.
     */
    public GridUnsafeMemory memory();

    /**
     * @param row Deserialized offheap row to cache in heap.
     */
    public void cache(GridH2KeyValueRowOffheap row);

    /**
     * @param ptr Offheap pointer to remove from cache.
     */
    public void uncache(long ptr);

    /**
     * @return Guard.
     */
    public GridUnsafeGuard guard();

    /**
     * Wraps object to respective {@link Value}.
     *
     * @param o Object.
     * @param type Value type.
     * @return Value.
     * @throws IgniteCheckedException If failed.
     */
    public Value wrap(Object o, int type) throws IgniteCheckedException;

    /**
     * @return {@code True} if should check swap value before offheap.
     */
    public boolean preferSwapValue();

    /**
     * @return {@code True} if index should support snapshots.
     */
    public boolean snapshotableIndex();

    /**
     * @return Escape all identifiers.
     */
    public boolean quoteAllIdentifiers();
}