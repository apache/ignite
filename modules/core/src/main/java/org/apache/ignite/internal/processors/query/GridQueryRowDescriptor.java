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

package org.apache.ignite.internal.processors.query;

import java.util.Set;
import org.apache.ignite.internal.processors.cache.GridCacheContext;
import org.jetbrains.annotations.Nullable;

/**
 * Row descriptor.
 */
public interface GridQueryRowDescriptor {
    /**
     * Callback for table metadata update event.
     */
    public void onMetadataUpdated();

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
    @Nullable public GridCacheContext<?, ?> context();

    /**
     * @return Total fields count.
     */
    public int fieldsCount();

    /**
     * Gets field value by field index.
     *
     * @param key Key.
     * @param val Value.
     * @param fieldIdx Field index.
     * @return Field value.
     */
    public Object getFieldValue(Object key, Object val, int fieldIdx);

    /**
     * Sets field value by field index.
     *
     * @param key Key.
     * @param val Value.
     * @param fieldVal Value to set to field.
     * @param fieldIdx Field index.
     */
    public void setFieldValue(Object key, Object val, Object fieldVal, int fieldIdx);

    /**
     * Determine whether a field corresponds to a property of key or to one of value.
     *
     * @param fieldIdx Field index.
     * @return {@code true} if given field corresponds to a key property, {@code false} otherwise.
     */
    public boolean isFieldKeyProperty(int fieldIdx);

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
    
    /**
     * Gets a copy of a set of table key column names.
     *
     * @return Set of a table key column names.
     */
    public Set<String> getRowKeyColumnNames();
}
