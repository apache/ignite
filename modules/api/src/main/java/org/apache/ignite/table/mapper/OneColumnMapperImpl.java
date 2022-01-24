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

import org.apache.ignite.internal.util.IgniteObjectName;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

/**
 * Simple mapper implementation that maps a whole object of the type {@link ObjectT} to a one column.
 *
 * @param <ObjectT> Target type.
 */
class OneColumnMapperImpl<ObjectT> implements OneColumnMapper<ObjectT> {
    /** Target type. */
    private final Class<ObjectT> targetType;

    /** Column name. */
    private final String mappedColumn;

    /** Converter. */
    private final TypeConverter<ObjectT, ?> converter;

    OneColumnMapperImpl(@NotNull Class<ObjectT> targetType, @Nullable String mappedColumn, @Nullable TypeConverter<ObjectT, ?> converter) {
        this.targetType = targetType;
        this.mappedColumn = IgniteObjectName.parse(mappedColumn);
        this.converter = converter;
    }

    /** {@inheritDoc} */
    @Override
    public Class<ObjectT> targetType() {
        return targetType;
    }

    /** {@inheritDoc} */
    @Override
    public @Nullable String mappedColumn() {
        return mappedColumn;
    }

    /** {@inheritDoc} */
    @Override
    public @Nullable TypeConverter<ObjectT, ?> converter() {
        return converter;
    }
}
