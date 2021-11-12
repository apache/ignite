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

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Objects;
import java.util.function.Function;
import org.apache.ignite.table.Tuple;
import org.jetbrains.annotations.NotNull;

/**
 * Mapper builder.
 *
 * @param <T> Mapped type.
 */
public final class MapperBuilder<T> {
    /** Target type. */
    private Class<T> targetType;
    
    /** Column-to-field name mapping. */
    private Map<String, String> columnToFields;
    
    /**
     * Creates a mapper builder for a type.
     *
     * @param targetType Target type.
     */
    MapperBuilder(@NotNull Class<T> targetType) {
        this.targetType = targetType;
        
        columnToFields = new HashMap<>(targetType.getDeclaredFields().length);
    }
    
    /**
     * Add mapping for a field to a column.
     *
     * @param fieldName  Field name.
     * @param columnName Column name.
     * @return {@code this} for chaining.
     * @throws IllegalArgumentException if a column was already mapped to some field.
     * @throws IllegalStateException if tries to reuse the builder after a mapping has been built.
     */
    public MapperBuilder<T> map(@NotNull String fieldName, @NotNull String columnName) {
        if (columnToFields == null) {
            throw new IllegalStateException("Mapper builder can't be reused.");
        }
        
        if (columnToFields.put(Objects.requireNonNull(columnName), Objects.requireNonNull(fieldName)) != null) {
            throw new IllegalArgumentException("Mapping for a column already exists: " + columnName);
        }
        
        return this;
    }
    
    /**
     * Map a field to a type of given class.
     *
     * @param fieldName   Field name.
     * @param targetClass Target class.
     * @return {@code this} for chaining.
     */
    public MapperBuilder<T> map(@NotNull String fieldName, Class<?> targetClass) {
        throw new UnsupportedOperationException("Not implemented yet.");
    }
    
    /**
     * Adds a functional mapping for a field, the result depends on function call for every particular row.
     *
     * @param fieldName       Field name.
     * @param mappingFunction Mapper function.
     * @return {@code this} for chaining.
     */
    public MapperBuilder<T> map(@NotNull String fieldName, Function<Tuple, Object> mappingFunction) {
        throw new UnsupportedOperationException("Not implemented yet.");
    }
    
    /**
     * Sets a target class to deserialize to.
     *
     * @param targetClass Target class.
     * @return {@code this} for chaining.
     */
    public MapperBuilder<T> deserializeTo(@NotNull Class<?> targetClass) {
        throw new UnsupportedOperationException("Not implemented yet.");
    }
    
    /**
     * Builds mapper.
     *
     * @return Mapper.
     * @throws IllegalStateException if nothing were mapped or more than one column were mapped to the same field.
     */
    public Mapper<T> build() {
        if (columnToFields.isEmpty()) {
            throw new IllegalStateException("Empty mapping isn't allowed.");
        }
        
        Map<String, String> mapping = this.columnToFields;
        
        this.columnToFields = null;
        
        HashSet<String> fields = new HashSet<>(mapping.size());
        
        for (String f : mapping.values()) {
            if (!fields.add(f)) {
                throw new IllegalStateException("More than one column is mapped to the field: field=" + f);
            }
        }
        
        return new DefaultColumnMapper<>(targetType, mapping);
    }
}
