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
import org.apache.ignite.internal.processors.cache.CacheObject;
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
     * @return Owner.
     */
    public IgniteH2Indexing owner();

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
}