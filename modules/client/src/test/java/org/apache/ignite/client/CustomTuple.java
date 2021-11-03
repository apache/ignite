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

package org.apache.ignite.client;

import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.util.BitSet;
import java.util.Iterator;
import java.util.UUID;
import org.apache.ignite.binary.BinaryObject;
import org.apache.ignite.table.Tuple;
import org.jetbrains.annotations.NotNull;

/**
 * User-defined test {@link Tuple} implementation.
 */
public class CustomTuple implements Tuple {
    /**
     *
     */
    private final Long id;

    /**
     *
     */
    private final String name;

    /**
     *
     */
    public CustomTuple(Long id) {
        this(id, null);
    }

    /**
     *
     */
    public CustomTuple(Long id, String name) {
        this.id = id;
        this.name = name;
    }

    @Override
    public int columnCount() {
        return 2;
    }

    @Override
    public String columnName(int columnIndex) {
        switch (columnIndex) {
            case 0:
                return "id";
            case 1:
                return "name";
            default:
                break;
        }

        return null;
    }

    @Override
    public int columnIndex(@NotNull String columnName) {
        switch (columnName) {
            case "id":
                return 0;
            case "name":
                return 1;
            default:
                break;
        }

        return -1;
    }

    @Override
    public <T> T valueOrDefault(@NotNull String columnName, T def) {
        switch (columnName) {
            case "id":
                return (T) id;
            case "name":
                return (T) name;
            default:
                break;
        }

        return def;
    }

    @Override
    public Tuple set(@NotNull String columnName, Object value) {
        throw new UnsupportedOperationException("Tuple is immutable.");
    }

    @Override
    public <T> T value(@NotNull String columnName) {
        return valueOrDefault(columnName, null);
    }

    @Override
    public <T> T value(int columnIndex) {
        switch (columnIndex) {
            case 0:
                return (T) id;
            case 1:
                return (T) name;
            default:
                break;
        }

        return null;
    }

    @Override
    public BinaryObject binaryObjectValue(@NotNull String columnName) {
        throw new UnsupportedOperationException();
    }

    @Override
    public BinaryObject binaryObjectValue(int columnIndex) {
        throw new UnsupportedOperationException();
    }

    @Override
    public byte byteValue(@NotNull String columnName) {
        throw new UnsupportedOperationException();
    }

    @Override
    public byte byteValue(int columnIndex) {
        throw new UnsupportedOperationException();
    }

    @Override
    public short shortValue(@NotNull String columnName) {
        throw new UnsupportedOperationException();
    }

    @Override
    public short shortValue(int columnIndex) {
        throw new UnsupportedOperationException();
    }

    @Override
    public int intValue(@NotNull String columnName) {
        throw new UnsupportedOperationException();
    }

    @Override
    public int intValue(int columnIndex) {
        throw new UnsupportedOperationException();
    }

    @Override
    public long longValue(@NotNull String columnName) {
        throw new UnsupportedOperationException();
    }

    @Override
    public long longValue(int columnIndex) {
        throw new UnsupportedOperationException();
    }

    @Override
    public float floatValue(@NotNull String columnName) {
        throw new UnsupportedOperationException();
    }

    @Override
    public float floatValue(int columnIndex) {
        throw new UnsupportedOperationException();
    }

    @Override
    public double doubleValue(@NotNull String columnName) {
        throw new UnsupportedOperationException();
    }

    @Override
    public double doubleValue(int columnIndex) {
        throw new UnsupportedOperationException();
    }

    @Override
    public String stringValue(@NotNull String columnName) {
        throw new UnsupportedOperationException();
    }

    @Override
    public String stringValue(int columnIndex) {
        throw new UnsupportedOperationException();
    }

    @Override
    public UUID uuidValue(@NotNull String columnName) {
        throw new UnsupportedOperationException();
    }

    @Override
    public UUID uuidValue(int columnIndex) {
        throw new UnsupportedOperationException();
    }

    @Override
    public BitSet bitmaskValue(@NotNull String columnName) {
        throw new UnsupportedOperationException();
    }

    @Override
    public BitSet bitmaskValue(int columnIndex) {
        throw new UnsupportedOperationException();
    }

    @Override
    public LocalDate dateValue(String columnName) {
        throw new UnsupportedOperationException();
    }

    @Override
    public LocalDate dateValue(int columnIndex) {
        throw new UnsupportedOperationException();
    }

    @Override
    public LocalTime timeValue(String columnName) {
        throw new UnsupportedOperationException();
    }

    @Override
    public LocalTime timeValue(int columnIndex) {
        throw new UnsupportedOperationException();
    }

    @Override
    public LocalDateTime datetimeValue(String columnName) {
        throw new UnsupportedOperationException();
    }

    @Override
    public LocalDateTime datetimeValue(int columnIndex) {
        throw new UnsupportedOperationException();
    }

    @Override
    public Instant timestampValue(String columnName) {
        throw new UnsupportedOperationException();
    }

    @Override
    public Instant timestampValue(int columnIndex) {
        throw new UnsupportedOperationException();
    }

    @NotNull
    @Override
    public Iterator<Object> iterator() {
        throw new UnsupportedOperationException();
    }

    @Override
    public int hashCode() {
        return Tuple.hashCode(this);
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) {
            return true;
        }

        if (obj instanceof Tuple) {
            return Tuple.equals(this, (Tuple) obj);
        }

        return false;
    }
}
