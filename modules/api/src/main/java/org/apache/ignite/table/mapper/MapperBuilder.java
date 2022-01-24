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

import java.lang.reflect.Field;
import java.lang.reflect.Modifier;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Objects;
import org.apache.ignite.internal.util.IgniteObjectName;
import org.jetbrains.annotations.NotNull;

/**
 * Mapper builder provides methods for mapping object fields to columns.
 *
 * <p>By default, the user must explicitly map required fields to columns
 * in one-to-one manner using {@link #map} and/or {@link #map(String, String, TypeConverter)} methods, all missed columns and/or fields
 * become unmapped, and will be ignored during further table operations.
 *
 * <p>Calling {@link #automap()} method maps all matching fields to columns by name. Any fields that don't match a column by name are
 * ignored.
 *
 * <p>TBD: add some code examples.
 *
 * <p>Note: builder can't be reused after the {@link #build()} method is called.
 *
 * @param <T> Type of which objects the mapper will handle.
 */
public final class MapperBuilder<T> {
    /** Target type. */
    private Class<T> targetType;

    /** Column-to-field name mapping. */
    private Map<String, String> columnToFields;

    /** Column converters. */
    private Map<String, TypeConverter<?, ?>> columnConverters = new HashMap<>();

    /** Column name for one-column mapping. */
    private final String mappedToColumn;

    /** Flag indicates, if all unmapped object fields should be mapped to the columns with same names automatically. */
    private boolean automapFlag = false;

    /** {@code True} if the {@link #build()} method was called, {@code false} otherwise. */
    private boolean isStale;

    /**
     * Creates a mapper builder for a POJO type.
     *
     * @param targetType Target type.
     */
    MapperBuilder(@NotNull Class<T> targetType) {
        this.targetType = ensureValidPojo(targetType);

        mappedToColumn = null;
        columnToFields = new HashMap<>(targetType.getDeclaredFields().length);
    }

    /**
     * Creates a mapper builder for a natively supported type.
     *
     * @param targetType   Target type.
     * @param mappedColumn Column name to map to; column name must use SQL-parser style quotation, e.g.
     *                     "myColumn" - means column name "MYCOLUMN",
     *                     "\"MyColumn\"" - "MyColumn", etc.
     */
    MapperBuilder(@NotNull Class<T> targetType, String mappedColumn) {
        this.targetType = Mapper.ensureNativelySupported(targetType);

        mappedToColumn = mappedColumn;
        columnToFields = null;
    }

    /**
     * Ensures class is of the supported kind for POJO mapping.
     *
     * @param type Class to validate.
     * @return {@code type} if it is valid POJO.
     * @throws IllegalArgumentException If {@code type} can't be used as POJO for mapping and/or of invalid kind.
     */
    public <O> Class<O> ensureValidPojo(@NotNull Class<O> type) {
        if (Mapper.nativelySupported(type)) {
            throw new IllegalArgumentException("Unsupported class. Can't map fields of natively supported type: " + type.getName());
        } else if (type.isAnonymousClass() || type.isLocalClass() || type.isSynthetic()
                           || (type.isMemberClass() && !Modifier.isStatic(type.getModifiers()))) {
            throw new IllegalArgumentException(
                    "Unsupported class. Only top-level or nested static classes are supported: " + type.getName());
        } else if (Modifier.isAbstract(type.getModifiers())) {
            throw new IllegalArgumentException("Unsupported class. Abstract classes are not supported: " + type.getName());
        } else if (type.isEnum()) {
            throw new IllegalArgumentException("Unsupported class. Enum is not supported, please use a converter: " + type.getName());
        } else if (type.isArray()) {
            throw new IllegalArgumentException("Unsupported class. Arrays are not supported: " + type.getName());
        } else if (type.isAnnotation() || type.isInterface()) {
            throw new IllegalArgumentException("Unsupported class. Interfaces are not supported: " + type.getName());
        }

        try {
            type.getDeclaredConstructor();

            return type;
        } catch (NoSuchMethodException e) {
            throw new IllegalArgumentException("Class must have default constructor: " + type.getName());
        }
    }

    /**
     * Ensure this instance is valid.
     *
     * @throws IllegalStateException if tries to reuse the builder after a mapping has been built.
     */
    private void ensureNotStale() {
        if (isStale) {
            throw new IllegalStateException("Mapper builder can't be reused.");
        }
    }

    /**
     * Ensure field name is valid and field with this name exists.
     *
     * @param fieldName Field name.
     * @return Field's name for chaining.
     * @throws IllegalArgumentException If field is {@code null} or class has no declared field with given name.
     */
    private String requireValidField(String fieldName) {
        try {
            if (fieldName == null || targetType.getDeclaredField(fieldName) == null) {
                throw new IllegalArgumentException("Mapping for a column already exists: " + fieldName);
            }
        } catch (NoSuchFieldException e) {
            throw new IllegalArgumentException(
                    String.format("Field not found for class: field=%s, class=%s", fieldName, targetType.getName()));
        }

        return fieldName;
    }

    /**
     * Maps a field to a column.
     *
     * @param fieldName        Field name.
     * @param columnName       Column name with SQL-parser style quotation, e.g.
     *                         "myColumn" - means column name "MYCOLUMN",
     *                         "\"MyColumn\"" - "MyColumn", etc.
     * @param fieldColumnPairs Vararg that accepts (fieldName, columnName) pairs, column's names should use SQL SQL-parser style
     *                        quotation like {@code columnName} parameter.
     * @return {@code this} for chaining.
     * @throws IllegalArgumentException If a field name has not paired column name in {@code fieldColumnPairs}, or a column was already
     *                                  mapped to another field.
     * @throws IllegalStateException    if tries to reuse the builder after a mapping has been built.
     */
    public MapperBuilder<T> map(@NotNull String fieldName, @NotNull String columnName, String... fieldColumnPairs) {
        ensureNotStale();

        String colName0 = IgniteObjectName.parse(columnName);

        if (columnToFields == null) {
            throw new IllegalArgumentException("Natively supported types doesn't support field mapping.");
        } else if (fieldColumnPairs.length % 2 != 0) {
            throw new IllegalArgumentException("fieldColumnPairs length should be even.");
        } else if (columnToFields.put(Objects.requireNonNull(colName0), requireValidField(fieldName)) != null) {
            throw new IllegalArgumentException("Mapping for a column already exists: " + colName0);
        }

        for (int i = 0; i < fieldColumnPairs.length; i += 2) {
            if (columnToFields.put(
                    IgniteObjectName.parse(Objects.requireNonNull(fieldColumnPairs[i + 1])),
                    requireValidField(fieldColumnPairs[i])) != null
            ) {
                throw new IllegalArgumentException("Mapping for a column already exists: " + colName0);
            }
        }

        return this;
    }

    /**
     * Maps a field to a column with using type converter. The value will be converted before write to and after read from column using
     * provided converter.
     *
     * @param <ObjectT>  Value type. Must match the object field type if the individual field is mapped to a given column.
     * @param <ColumnT>  Column type.
     * @param fieldName  Field name.
     * @param columnName Column name with SQL-parser style quotation, e.g.
     *                   "myColumn" - means column name "MYCOLUMN",
     *                   "\"MyColumn\"" - "MyColumn", etc.
     * @param converter  Converter for objects of {@link ColumnT} and {@link ObjectT}.
     */
    public <ObjectT, ColumnT> MapperBuilder<T> map(
            @NotNull String fieldName,
            @NotNull String columnName,
            @NotNull TypeConverter<ObjectT, ColumnT> converter
    ) {
        map(fieldName, columnName);
        convert(columnName, converter);

        return this;
    }

    /**
     * Sets a converter for a column, which value must be converted before write/after read.
     *
     * @param <ObjectT>  Value type. Must match either the object field type if a field mapped to given column, or the object type {@link T}
     * @param <ColumnT>  Column type.
     * @param columnName Column name with SQL-parser style quotation, e.g.
     *                   "myColumn" - means column name "MYCOLUMN",
     *                   "\"MyColumn\"" - "MyColumn", etc.
     * @param converter  Converter for objects of {@link ColumnT} and {@link ObjectT}.
     */
    public <ObjectT, ColumnT> MapperBuilder<T> convert(@NotNull String columnName, @NotNull TypeConverter<ObjectT, ColumnT> converter) {
        ensureNotStale();

        if (columnConverters.put(IgniteObjectName.parse(columnName), converter) != null) {
            throw new IllegalArgumentException("Column converter already exists: " + columnName);
        }

        return this;
    }

    /**
     * Maps all matching fields to columns by name. Any fields that don't match a column by name are ignored.
     *
     * @return {@code this} for chaining.
     */
    public MapperBuilder<T> automap() {
        ensureNotStale();

        automapFlag = true;

        return this;
    }

    /**
     * Builds mapper.
     *
     * @return Mapper.
     * @throws IllegalStateException if nothing was mapped, or more than one column was mapped to the same field.
     */
    public Mapper<T> build() {
        ensureNotStale();

        isStale = true;

        if (columnToFields == null) {
            return new OneColumnMapperImpl<>(targetType, mappedToColumn, (TypeConverter<T, ?>) columnConverters.get(mappedToColumn));
        }

        Map<String, String> mapping = this.columnToFields;

        HashSet<String> fields = new HashSet<>(mapping.size());
        for (String fldName : mapping.values()) {
            if (!fields.add(fldName)) {
                throw new IllegalStateException("More than one column is mapped to the field: field=" + fldName);
            }
        }

        if (automapFlag) {
            Arrays.stream(targetType.getDeclaredFields())
                    .map(Field::getName)
                    .filter(fldName -> !fields.contains(fldName))
                    // Ignore manually mapped fields/columns.
                    .forEach(fldName -> mapping.putIfAbsent(fldName.toUpperCase(), fldName));
        }

        return new PojoMapperImpl<>(targetType, mapping, columnConverters);
    }
}
