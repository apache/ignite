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
import java.util.Objects;
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
     * Returns the hash code value for the tuple.
     * <p>
     * The hash code of a tuple is defined to be the sum of the hash codes of each pair of column name and column value.
     * This ensures that {@code m1.equals(m2)} implies that {@code m1.hashCode()==m2.hashCode()} for any tuples
     * {@code m1} and {@code m2}, as required by the general contract of {@link Object#hashCode}.
     * <p>
     * The hash code of a pair of column name and column value {@code i} is defined to be:
     * <pre>(columnName(i).hashCode()) ^ (value(i)==null ? 0 : value(i).hashCode())</pre>
     *
     * @param tuple Tuple.
     * @return The hash code value for the tuple.
     */
    static int hashCode(Tuple tuple) {
        int hash = 0;

        for (int idx = 0; idx < tuple.columnCount(); idx++) {
            String columnName = tuple.columnName(idx);
            Object columnValue = tuple.value(idx);

            hash += columnName.hashCode() ^ (columnValue == null ? 0 : columnValue.hashCode());
        }

        return hash;
    }

    /**
     * Compares tuples for equality.
     * <p>
     * Returns {@code true} if both tuples represent the same column name to column value mappings.
     * <p>
     * This implementation first checks if both tuples is of same size; if not, it returns {@code false};
     * If so, it iterates over columns of first tuple and checks that the second tuple contains each mapping
     * that the first one contains.  If the second tuple fails to contain such a mapping, {@code false} is returned;
     * If the iteration completes, {@code true} is returned.
     *
     * @param firstTuple First tuple to compare.
     * @param secondTuple Second tuple to compare.
     * @return {@code true} if the first tuple is equal to the second tuple.
     */
    static boolean equals(Tuple firstTuple, Tuple secondTuple) {
        if (firstTuple == secondTuple)
            return true;

        int columns = firstTuple.columnCount();

        if (columns != secondTuple.columnCount())
            return false;

        for (int idx = 0; idx < columns; idx++) {
            int idx2 = secondTuple.columnIndex(firstTuple.columnName(idx));

            if (idx2 < 0)
                return false;

            if (!Objects.deepEquals(firstTuple.value(idx), secondTuple.value(idx2)))
                return false;
        }

        return true;
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

    /**
     * Returns the hash code value for this tuple.
     *
     * @return the hash code value for this tuple.
     * @see #hashCode(Tuple)
     * @see Object#hashCode()
     */
    int hashCode();

    /**
     * Indicates whether some other object is "equal to" this one.
     *
     * @return {@code true} if this object is the same as the obj argument; {@code false} otherwise.
     * @see Tuple#equals(Tuple, Tuple)
     * @see Object#equals(Object)
     */
    boolean equals(Object obj);
}
