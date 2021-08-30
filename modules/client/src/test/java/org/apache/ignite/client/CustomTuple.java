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
    /** */
    private final Long id;

    /** */
    private final String name;

    /** */
    public CustomTuple(Long id) {
        this(id, null);
    }

    /** */
    public CustomTuple(Long id, String name) {
        this.id = id;
        this.name = name;
    }

    @Override public int columnCount() {
        return 2;
    }

    @Override public String columnName(int columnIndex) {
        switch (columnIndex) {
            case 0:
                return "id";
            case 1:
                return "name";
        }

        return null;
    }

    @Override public int columnIndex(String columnName) {
        switch (columnName) {
            case "id":
                return 0;
            case "name":
                return 1;
        }

        return -1;
    }

    @Override public <T> T valueOrDefault(String columnName, T def) {
        switch (columnName) {
            case "id":
                return (T)id;
            case "name":
                return (T)name;
        }

        return def;
    }

    @Override public Tuple set(String columnName, Object value) {
        throw new UnsupportedOperationException("Tuple is immutable.");
    }

    @Override public <T> T value(String columnName) {
        return valueOrDefault(columnName, null);
    }

    @Override public <T> T value(int columnIndex) {
        switch (columnIndex) {
            case 0:
                return (T)id;
            case 1:
                return (T)name;
        }

        return null;
    }

    @Override public BinaryObject binaryObjectValue(String columnName) {
        throw new UnsupportedOperationException();
    }

    @Override public BinaryObject binaryObjectValue(int columnIndex) {
        throw new UnsupportedOperationException();
    }

    @Override public byte byteValue(String columnName) {
        throw new UnsupportedOperationException();
    }

    @Override public byte byteValue(int columnIndex) {
        throw new UnsupportedOperationException();
    }

    @Override public short shortValue(String columnName) {
        throw new UnsupportedOperationException();
    }

    @Override public short shortValue(int columnIndex) {
        throw new UnsupportedOperationException();
    }

    @Override public int intValue(String columnName) {
        throw new UnsupportedOperationException();
    }

    @Override public int intValue(int columnIndex) {
        throw new UnsupportedOperationException();
    }

    @Override public long longValue(String columnName) {
        throw new UnsupportedOperationException();
    }

    @Override public long longValue(int columnIndex) {
        throw new UnsupportedOperationException();
    }

    @Override public float floatValue(String columnName) {
        throw new UnsupportedOperationException();
    }

    @Override public float floatValue(int columnIndex) {
        throw new UnsupportedOperationException();
    }

    @Override public double doubleValue(String columnName) {
        throw new UnsupportedOperationException();
    }

    @Override public double doubleValue(int columnIndex) {
        throw new UnsupportedOperationException();
    }

    @Override public String stringValue(String columnName) {
        throw new UnsupportedOperationException();
    }

    @Override public String stringValue(int columnIndex) {
        throw new UnsupportedOperationException();
    }

    @Override public UUID uuidValue(String columnName) {
        throw new UnsupportedOperationException();
    }

    @Override public UUID uuidValue(int columnIndex) {
        throw new UnsupportedOperationException();
    }

    @Override public BitSet bitmaskValue(String columnName) {
        throw new UnsupportedOperationException();
    }

    @Override public BitSet bitmaskValue(int columnIndex) {
        throw new UnsupportedOperationException();
    }

    @Override public LocalDate dateValue(String columnName) {
        throw new UnsupportedOperationException();
    }

    @Override public LocalDate dateValue(int columnIndex) {
        throw new UnsupportedOperationException();
    }

    @Override public LocalTime timeValue(String columnName) {
        throw new UnsupportedOperationException();
    }

    @Override public LocalTime timeValue(int columnIndex) {
        throw new UnsupportedOperationException();
    }

    @Override public LocalDateTime datetimeValue(String columnName) {
        throw new UnsupportedOperationException();
    }

    @Override public LocalDateTime datetimeValue(int columnIndex) {
        throw new UnsupportedOperationException();
    }

    @Override public Instant timestampValue(String columnName) {
        throw new UnsupportedOperationException();
    }

    @Override public Instant timestampValue(int columnIndex) {
        throw new UnsupportedOperationException();
    }

    @NotNull @Override public Iterator<Object> iterator() {
        throw new UnsupportedOperationException();
    }
}
