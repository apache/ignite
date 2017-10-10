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
import org.apache.ignite.internal.processors.cache.KeyCacheObject;
import org.apache.ignite.internal.processors.cache.version.GridCacheVersion;
import org.apache.ignite.internal.processors.cache.GridCacheContext;
import org.apache.ignite.internal.processors.query.GridQueryTypeDescriptor;
import org.apache.ignite.internal.processors.query.h2.IgniteH2Indexing;
import org.h2.result.SearchRow;
import org.h2.value.Value;
import org.jetbrains.annotations.Nullable;

/**
 * Row descriptor.
 */
public interface GridH2RowDescriptor {
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
     * Creates new row.
     *
     * @param key Key.
     * @param val Value.
     * @param ver Version.
     * @param expirationTime Expiration time in millis.
     * @return Row.
     * @throws IgniteCheckedException If failed.
     */
    public GridH2Row createRow(KeyCacheObject key, int part, @Nullable CacheObject val, GridCacheVersion ver,
        long expirationTime) throws IgniteCheckedException;

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
     * Wraps object to respective {@link Value}.
     *
     * @param o Object.
     * @param type Value type.
     * @return Value.
     * @throws IgniteCheckedException If failed.
     */
    public Value wrap(Object o, int type) throws IgniteCheckedException;

    /**
     * Checks if provided column id matches key column or key alias.
     *
     * @param colId Column id.
     * @return Result.
     */
    public boolean isKeyColumn(int colId);

    /**
     * Checks if provided column id matches value column or alias.
     *
     * @param colId Column id.
     * @return Result.
     */
    public boolean isValueColumn(int colId);

    /**
     * Checks if provided column id matches key, key alias,
     * value, value alias or version column.
     *
     * @param colId Column id.
     * @return Result.
     */
    public boolean isKeyValueOrVersionColumn(int colId);

    /**
     * Checks if provided index condition is allowed for key column or key alias column.
     *
     * @param masks Array containing Index Condition masks for each column.
     * @param mask Index Condition to check.
     * @return Result.
     */
    public boolean checkKeyIndexCondition(int masks[], int mask);

    /**
     * Initializes value cache with key, val and version.
     *
     * @param valCache Value cache.
     * @param key Key.
     * @param value Value.
     * @param version Version.
     */
    public void initValueCache(Value valCache[], Value key, Value value, Value version);

    /**
     * Clones provided row and copies values of alias key and val columns
     * into respective key and val positions.
     *
     * @param row Source row.
     * @return Result.
     */
    public SearchRow prepareProxyIndexRow(SearchRow row);

    /**
     * Gets alternative column id that may substitute the given column id.
     *
     * For alias column returns original one.
     * For original column returns its alias.
     *
     * Otherwise, returns the given column id.
     *
     * @param colId Column id.
     * @return Result.
     */
    public int getAlternativeColumnId(int colId);
}