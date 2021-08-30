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

package org.apache.ignite.table;

import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.util.BitSet;
import java.util.UUID;
import org.apache.ignite.binary.BinaryObject;
import org.jetbrains.annotations.NotNull;

/**
 * Tuple represents arbitrary set of columns whose values is accessible by column name.
 * <p>
 * Provides specialized method for some value-types to avoid boxing/unboxing.
 */
public interface Tuple extends Iterable<Object> {
    /**
     * Creates a tuple.
     *
     * @return Tuple.
     */
    static Tuple create() {
        return new TupleImpl();
    }

    /**
     * Creates a tuple with specified initial capacity.
     *
     * @param capacity Initial capacity.
     * @return Tuple.
     */
    static Tuple create(int capacity) {
        return new TupleImpl(capacity);
    }

    /**
     * Gets the number of columns in this tuple.
     *
     * @return Number of columns.
     */
    int columnCount();

    /**
     * Gets the name of the column with the specified index.
     *
     * @param columnIndex Column index.
     * @return Column name.
     * @throws IndexOutOfBoundsException If a value for a column with given index doesn't exists.
     */
    String columnName(int columnIndex);

    /**
     * Gets the index of the column with the specified name.
     *
     * @param columnName Column name.
     * @return Column index, or {@code -1} when a column with given name is not present.
     */
    int columnIndex(@NotNull String columnName);

    /**
     * Gets column value when a column with specified name is present in this tuple; returns default value otherwise.
     *
     * @param columnName Column name.
     * @param defaultValue Default value.
     * @param <T> Column default value type.
     * @return Column value if this tuple contains a column with the specified name. Otherwise returns {@code defaultValue}.
     */
    <T> T valueOrDefault(@NotNull String columnName, T defaultValue);

    /**
     * Sets column value.
     *
     * @param columnName Column name.
     * @param value Value to set.
     * @return {@code this} for chaining.
     */
    Tuple set(@NotNull String columnName, Object value);

    /**
     * Gets column value for given column name.
     *
     * @param columnName Column name.
     * @param <T> Value type.
     * @return Column value.
     * @throws IllegalArgumentException If column with given name doesn't exists.
     */
    <T> T value(@NotNull String columnName);

    /**
     * Gets column value for given column index.
     *
     * @param columnIndex Column index.
     * @param <T> Value type.
     * @return Column value.
     * @throws IndexOutOfBoundsException If column with given index doesn't exists.
     */
    <T> T value(int columnIndex);

    /**
     * Gets binary object column.
     *
     * @param columnName Column name.
     * @return Column value.
     * @throws IllegalArgumentException If column with given name doesn't exists.
     */
    BinaryObject binaryObjectValue(@NotNull String columnName);

    /**
     * Gets binary object column.
     *
     * @param columnIndex Column index.
     * @return Column value.
     * @throws IndexOutOfBoundsException If column with given index doesn't exists.
     */
    BinaryObject binaryObjectValue(int columnIndex);

    /**
     * Gets {@code byte} column value.
     *
     * @param columnName Column name.
     * @return Column value.
     * @throws IllegalArgumentException If column with given name doesn't exists.
     */
    byte byteValue(@NotNull String columnName);

    /**
     * Gets {@code byte} column value.
     *
     * @param columnIndex Column index.
     * @return Column value.
     * @throws IndexOutOfBoundsException If column with given index doesn't exists.
     */
    byte byteValue(int columnIndex);

    /**
     * Gets {@code short} column value.
     *
     * @param columnName Column name.
     * @return Column value.
     * @throws IllegalArgumentException If column with given name doesn't exists.
     */
    short shortValue(@NotNull String columnName);

    /**
     * Gets {@code short} column value.
     *
     * @param columnIndex Column index.
     * @return Column value.
     * @throws IndexOutOfBoundsException If column with given index doesn't exists.
     */
    short shortValue(int columnIndex);

    /**
     * Gets {@code int} column value.
     *
     * @param columnName Column name.
     * @return Column value.
     * @throws IllegalArgumentException If column with given name doesn't exists.
     */
    int intValue(@NotNull String columnName);

    /**
     * Gets {@code int} column value.
     *
     * @param columnIndex Column index.
     * @return Column value.
     * @throws IndexOutOfBoundsException If column with given index doesn't exists.
     */
    int intValue(int columnIndex);

    /**
     * Gets {@code long} column value.
     *
     * @param columnName Column name.
     * @return Column value.
     * @throws IllegalArgumentException If column with given name doesn't exists.
     */
    long longValue(@NotNull String columnName);

    /**
     * Gets {@code long} column value.
     *
     * @param columnIndex Column index.
     * @return Column value.
     * @throws IndexOutOfBoundsException If column with given index doesn't exists.
     */
    long longValue(int columnIndex);

    /**
     * Gets {@code float} column value.
     *
     * @param columnName Column name.
     * @return Column value.
     * @throws IllegalArgumentException If column with given name doesn't exists.
     */
    float floatValue(@NotNull String columnName);

    /**
     * Gets {@code float} column value.
     *
     * @param columnIndex Column index.
     * @return Column value.
     * @throws IndexOutOfBoundsException If column with given index doesn't exists.
     */
    float floatValue(int columnIndex);

    /**
     * Gets {@code double} column value.
     *
     * @param columnName Column name.
     * @return Column value.
     * @throws IllegalArgumentException If column with given name doesn't exists.
     */
    double doubleValue(@NotNull String columnName);

    /**
     * Gets {@code double} column value.
     *
     * @param columnIndex Column index.
     * @return Column value.
     * @throws IndexOutOfBoundsException If column with given index doesn't exists.
     */
    double doubleValue(int columnIndex);

    /**
     * Gets {@code String} column value.
     *
     * @param columnName Column name.
     * @return Column value.
     * @throws IllegalArgumentException If column with given name doesn't exists.
     */
    String stringValue(@NotNull String columnName);

    /**
     * Gets {@code String} column value.
     *
     * @param columnIndex Column index.
     * @return Column value.
     * @throws IndexOutOfBoundsException If column with given index doesn't exists.
     */
    String stringValue(int columnIndex);

    /**
     * Gets {@code UUID} column value.
     *
     * @param columnName Column name.
     * @return Column value.
     * @throws IllegalArgumentException If column with given name doesn't exists.
     */
    UUID uuidValue(@NotNull String columnName);

    /**
     * Gets {@code UUID} column value.
     *
     * @param columnIndex Column index.
     * @return Column value.
     * @throws IndexOutOfBoundsException If column with given index doesn't exists.
     */
    UUID uuidValue(int columnIndex);

    /**
     * Gets {@code BitSet} column value.
     *
     * @param columnName Column name.
     * @return Column value.
     * @throws IllegalArgumentException If column with given name doesn't exists.
     */
    BitSet bitmaskValue(@NotNull String columnName);

    /**
     * Gets {@code BitSet} column value.
     *
     * @param columnIndex Column index.
     * @return Column value.
     * @throws IndexOutOfBoundsException If column with given index doesn't exists.
     */
    BitSet bitmaskValue(int columnIndex);

    /**
     * Gets {@code LocalDate} column value.
     *
     * @param columnName Column name.
     * @return Column value.
     * @throws IllegalArgumentException If column with given name doesn't exists.
     */
    LocalDate dateValue(String columnName);

    /**
     * Gets {@code LocalDate} column value.
     *
     * @param columnIndex Column index.
     * @return Column value.
     * @throws IndexOutOfBoundsException If column with given index doesn't exists.
     */
    LocalDate dateValue(int columnIndex);

    /**
     * Gets {@code LocalTime} column value.
     *
     * @param columnName Column name.
     * @return Column value.
     * @throws IllegalArgumentException If column with given name doesn't exists.
     */
    LocalTime timeValue(String columnName);

    /**
     * Gets {@code LocalTime} column value.
     *
     * @param columnIndex Column index.
     * @return Column value.
     * @throws IndexOutOfBoundsException If column with given index doesn't exists.
     */
    LocalTime timeValue(int columnIndex);

    /**
     * Gets {@code LocalDateTime} column value.
     *
     * @param columnName Column name.
     * @return Column value.
     * @throws IllegalArgumentException If column with given name doesn't exists.
     */
    LocalDateTime datetimeValue(String columnName);

    /**
     * Gets {@code LocalDateTime} column value.
     *
     * @param columnIndex Column index.
     * @return Column value.
     * @throws IndexOutOfBoundsException If column with given index doesn't exists.
     */
    LocalDateTime datetimeValue(int columnIndex);

    /**
     * Gets {@code Instant} column value.
     *
     * @param columnName Column name.
     * @return Column value.
     * @throws IllegalArgumentException If column with given name doesn't exists.
     */
    Instant timestampValue(String columnName);

    /**
     * Gets {@code Instant} column value.
     *
     * @param columnIndex Column index.
     * @return Column value.
     * @throws IndexOutOfBoundsException If column with given index doesn't exists.
     */
    Instant timestampValue(int columnIndex);
}
