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

import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

/**
 * Mapper interface defines methods that are required for a marshaller to map class field names to table columns.
 *
 * @param <T> Mapped type.
 */
public interface Mapper<T> {
    /**
     * Creates a mapper for a class.
     *
     * @param cls Key class.
     * @param <K> Key type.
     * @return Mapper.
     */
    static <K> Mapper<K> of(Class<K> cls) {
        return identity(cls);
    }
    
    /**
     * Creates a mapper builder for a class.
     *
     * @param cls Value class.
     * @param <V> Value type.
     * @return Mapper builder.
     */
    static <V> MapperBuilder<V> builderFor(Class<V> cls) {
        return new MapperBuilder<>(cls);
    }
    
    /**
     * Creates identity mapper which is used for simple types that have native support or objects with field names that match column names.
     *
     * @param targetClass Target type class.
     * @param <T>         Target type.
     * @return Mapper.
     */
    static <T> Mapper<T> identity(Class<T> targetClass) {
        return new IdentityMapper<T>(targetClass);
    }
    
    /**
     * Return mapped type.
     *
     * @return Mapped type.
     */
    Class<T> targetType();
    
    /**
     * Maps a column name to a field name.
     *
     * @param columnName Column name.
     * @return Field name or {@code null} if no field mapped to a column.
     */
    @Nullable String columnToField(@NotNull String columnName);
}
