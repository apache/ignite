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

import java.util.Map;
import java.util.Objects;
import org.jetbrains.annotations.NotNull;

/**
 * Mapper implementation which maps object fields to the columns by their names.
 *
 * @param <T> Target type.
 */
class PojoMapperImpl<T> implements PojoMapper<T> {
    /** Target type. */
    private final Class<T> targetType;

    /** Column-to-field name mapping. */
    private final Map<String, String> mapping;

    private final Map<String, TypeConverter<?, ?>> converters;

    /**
     * Creates a mapper for given type.
     *
     * @param targetType Target type.
     * @param mapping    Column-to-field name mapping.
     * @param converters Column converters.
     */
    PojoMapperImpl(@NotNull Class<T> targetType, @NotNull Map<String, String> mapping, Map<String, TypeConverter<?, ?>> converters) {
        this.converters = converters;
        if (Objects.requireNonNull(mapping).isEmpty()) {
            throw new IllegalArgumentException("Empty mapping isn't allowed.");
        }

        this.targetType = targetType;
        this.mapping = mapping;
    }

    /** {@inheritDoc} */
    @Override
    public Class<T> targetType() {
        return targetType;
    }

    /** {@inheritDoc} */
    @Override
    public String fieldForColumn(@NotNull String columnName) {
        return mapping.get(columnName);
    }

    /** {@inheritDoc} */
    @Override
    public <FieldT, ColumnT> TypeConverter<FieldT, ColumnT> converterForColumn(@NotNull String columnName) {
        return (TypeConverter<FieldT, ColumnT>) converters.get(columnName);
    }
}
