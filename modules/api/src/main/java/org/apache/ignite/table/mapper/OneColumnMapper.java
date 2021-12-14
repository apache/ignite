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

package org.apache.ignite.table.mapper;

import org.jetbrains.annotations.Nullable;

/**
 * Maps the whole object of natively supported type to a single column. Used to map the object itself, but not the object fields.
 *
 * @apiNote When mapper is used for mapping key/value objects, and the schema contains single key/value column, it is possible to
 *         map object without specifying a column name. However, if there are more columns and no concrete one was specified, then table
 *         operations with using such mapper may fail with a schema mismatch exception due to ambiguity.
 */
public interface OneColumnMapper<ObjectT> extends Mapper<ObjectT> {
    /**
     * Returns a column name the object is mapped to, or {@code null} if not specified. If column name wasn't specified, the mapper maps the
     * entire object to a single available column.
     *
     * <p>Note: If more than one key/value column will be available to map to then table operation will fail with a schema mismatch
     * exception due to ambiguity.
     *
     * @return Column name that a whole object is mapped to, or {@code null}.
     */
    @Nullable String mappedColumn();

    /**
     * Returns type converter for mapped column.
     *
     * @return Type converter or {@code null} if not set.
     */
    @Nullable TypeConverter<ObjectT, ?> converter();
}
