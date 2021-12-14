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

import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.util.BitSet;
import java.util.Objects;
import java.util.UUID;
import java.util.function.Function;
import org.apache.ignite.table.Tuple;
import org.jetbrains.annotations.NotNull;

/**
 * Mapper interface defines methods that are required for a marshaller to map class field names to table columns.
 *
 * <p>NB: Only natively supported types, top-level POJOs or static nested classes are supported. Anonymous, local, inner classes,
 * interfaces, annotation, and so on, causes an exception.
 *
 * @param <T> Type of which objects the mapper handles.
 * @apiNote Implementation shouldn't use this interface directly, please, use {@link PojoMapper} or {@link OneColumnMapper}
 *         instead.
 * @see PojoMapper
 * @see OneColumnMapper
 */
public interface Mapper<T> {
    /**
     * Creates a mapper for the specified class. Natively supported types will be mapped to a single schema column, otherwise individual
     * object fields will be mapped to columns with the same name.
     *
     * <p>Note: Natively supported types can be mapped only to a single key/value column. If table may have more than one column, then
     * table operation will fail with exception. Use {@link #of(Class, String)} instead, to map to a concrete column name.
     *
     * @param type Target type.
     * @return Mapper for key objects representing a single key column.
     * @throws IllegalArgumentException If {@code type} is of unsupported kind.
     */
    static <O> Mapper<O> of(@NotNull Class<O> type) {
        if (nativelySupported(type)) {
            // TODO: Cache mappers (IGNITE-16094).
            return new OneColumnMapperImpl<>(type, null, null);
        } else {
            return builder(type).automap().build();
        }
    }

    /**
     * Creates a mapper for the case when an object represents only one column.
     *
     * <p>The mapper can be used as a key, value, or record mapper. However, a single-column record looks like a degraded case.
     *
     * @param type       Parametrized type of which objects the mapper will handle.
     * @param columnName Column name to map object to.
     * @return Mapper for objects representing a one column.
     * @throws IllegalArgumentException If {@code type} is of unsupported kind.
     */
    static <O> Mapper<O> of(@NotNull Class<O> type, @NotNull String columnName) {
        return new OneColumnMapperImpl<>(ensureNativelySupported(type), columnName, null);
    }

    /**
     * Creates a mapper for the case when an object represents only one column and additional transformation is required.
     *
     * <p>The mapper can be used as key, value, or record mapper. However, single column record looks as degraded case.
     *
     * @param type       Parametrized type of which objects the mapper will handle.
     * @param columnName Column name to map object to.
     * @param <ObjectT>  Mapper target type.
     * @param <ColumnT>  MUST be a type, which compatible with the column type.
     * @return Mapper for objects representing a one column.
     * @throws IllegalArgumentException If {@code type} is of unsupported kind.
     */
    static <ObjectT, ColumnT> Mapper<ObjectT> of(
            @NotNull Class<ObjectT> type,
            @NotNull String columnName,
            @NotNull TypeConverter<ObjectT, ColumnT> converter
    ) {
        return new OneColumnMapperImpl<>(Objects.requireNonNull(type), Objects.requireNonNull(columnName),
                Objects.requireNonNull(converter));
    }

    /**
     * Shortcut method creates a mapper for a case when object individual fields map to the column by their names.
     *
     * <p>The mapper can be used as key, value, or record mapper.
     *
     * @param type             Parametrized type of which objects the mapper will handle.
     * @param fieldName        Object field name.
     * @param columnName       Column name.
     * @param fieldColumnPairs Vararg that accepts (fieldName, columnName) pairs.
     * @return Mapper .
     * @throws IllegalArgumentException If a field name has not paired column name in {@code fieldColumnPairs}, or {@code type} is of
     *                                  unsupported kind.
     */
    static <O> Mapper<O> of(@NotNull Class<O> type, @NotNull String fieldName, @NotNull String columnName, String... fieldColumnPairs) {
        if (fieldColumnPairs.length % 2 != 0) {
            throw new IllegalArgumentException(
                    "Missed a column name, which the field is mapped to: " + fieldColumnPairs[fieldColumnPairs.length - 1]);
        }

        return builder(type).map(Objects.requireNonNull(fieldName), Objects.requireNonNull(columnName), fieldColumnPairs).build();
    }

    /**
     * Adds a manual functional mapping for an object and row represented by tuple.
     *
     * @param objectToRow Object to tuple function.
     * @param rowToObject Tuple to object function.
     * @return {@code this} for chaining.
     */
    static <O> Mapper<O> of(Function<O, Tuple> objectToRow, Function<Tuple, O> rowToObject) {
        //TODO: implement custom user mapping https://issues.apache.org/jira/browse/IGNITE-16116
        throw new UnsupportedOperationException("Not implemented yet.");
    }

    /**
     * Creates a mapper builder for objects of given class.
     *
     * <p>Note: Builder itself can't be reused.
     *
     * @param type Parametrized type of which objects the mapper will handle. Class MUST have the default constructor,
     * @return Mapper builder.
     * @throws IllegalArgumentException If {@code type} is of unsupported kind.
     */
    static <O> MapperBuilder<O> builder(@NotNull Class<O> type) {
        if (nativelySupported(type)) {
            return new MapperBuilder<>(type, null);
        } else {
            return new MapperBuilder<>(type);
        }
    }

    /**
     * Ensures class is of natively supported kind and can be used in one-column mapping.
     *
     * @param type Class to validate.
     * @return {@code type} if it is of natively supported kind.
     * @throws IllegalArgumentException If {@code type} is invalid and can't be used in one-column mapping.
     */
    static <O> Class<O> ensureNativelySupported(@NotNull Class<O> type) {
        if (nativelySupported(type)) {
            return type;
        }

        throw new IllegalArgumentException("Class has no native support (type converter required): " + type.getName());
    }

    /**
     * Checks if class is of natively supported type.
     *
     * @param type Class to check.
     * @return {@code True} if given type is supported natively, and can be mapped to a single column.
     */
    static boolean nativelySupported(Class<?> type) {
        return !Objects.requireNonNull(type).isPrimitive()
                       && (String.class == type
                                   || UUID.class == type
                                   || BitSet.class == type
                                   || byte[].class == type
                                   || LocalDate.class == type
                                   || LocalTime.class == type
                                   || LocalDateTime.class == type
                                   || Instant.class == type
                                   || Number.class
                                              .isAssignableFrom(type)); // Byte, Short, Integer, Long, Float, Double, BigInteger, BigDecimal
    }

    /**
     * Returns a type of which object the mapper handles.
     *
     * @return Mapper target type.
     */
    Class<T> targetType();
}