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

package org.apache.ignite.internal.client.table;

import static org.apache.ignite.internal.client.proto.ClientDataType.BIGINTEGER;
import static org.apache.ignite.internal.client.proto.ClientDataType.BITMASK;
import static org.apache.ignite.internal.client.proto.ClientDataType.BOOLEAN;
import static org.apache.ignite.internal.client.proto.ClientDataType.BYTES;
import static org.apache.ignite.internal.client.proto.ClientDataType.DATE;
import static org.apache.ignite.internal.client.proto.ClientDataType.DATETIME;
import static org.apache.ignite.internal.client.proto.ClientDataType.DECIMAL;
import static org.apache.ignite.internal.client.proto.ClientDataType.DOUBLE;
import static org.apache.ignite.internal.client.proto.ClientDataType.FLOAT;
import static org.apache.ignite.internal.client.proto.ClientDataType.INT16;
import static org.apache.ignite.internal.client.proto.ClientDataType.INT32;
import static org.apache.ignite.internal.client.proto.ClientDataType.INT64;
import static org.apache.ignite.internal.client.proto.ClientDataType.INT8;
import static org.apache.ignite.internal.client.proto.ClientDataType.NUMBER;
import static org.apache.ignite.internal.client.proto.ClientDataType.STRING;
import static org.apache.ignite.internal.client.proto.ClientDataType.TIME;
import static org.apache.ignite.internal.client.proto.ClientDataType.TIMESTAMP;

import java.util.HashMap;
import java.util.Map;
import org.apache.ignite.internal.client.proto.ClientDataType;
import org.apache.ignite.internal.client.proto.TuplePart;
import org.apache.ignite.internal.marshaller.BinaryMode;
import org.apache.ignite.internal.marshaller.Marshaller;
import org.apache.ignite.internal.marshaller.MarshallerColumn;
import org.apache.ignite.lang.IgniteException;
import org.apache.ignite.table.mapper.Mapper;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

/**
 * Client schema.
 */
@SuppressWarnings({"rawtypes", "AssignmentOrReturnOfFieldWithMutableType", "unchecked"})
public class ClientSchema {
    /** Schema version. Incremented on each schema modification. */
    private final int ver;

    /** Key columns count. */
    private final int keyColumnCount;

    /** Columns. */
    private final ClientColumn[] columns;

    /** Columns map by name. */
    private final Map<String, ClientColumn> map = new HashMap<>();

    /**
     * Constructor.
     *
     * @param ver     Schema version.
     * @param columns Columns.
     */
    public ClientSchema(int ver, ClientColumn[] columns) {
        assert ver >= 0;
        assert columns != null;

        this.ver = ver;
        this.columns = columns;

        var keyCnt = 0;

        for (var col : columns) {
            if (col.key()) {
                keyCnt++;
            }

            map.put(col.name(), col);
        }

        keyColumnCount = keyCnt;
    }

    /**
     * Returns version.
     *
     * @return Version.
     */
    public int version() {
        return ver;
    }

    /**
     * Returns columns.
     *
     * @return Columns.
     */
    public @NotNull ClientColumn[] columns() {
        return columns;
    }

    /**
     * Gets a column by name.
     *
     * @param name Column name.
     * @return Column by name.
     * @throws IgniteException When a column with the specified name does not exist.
     */
    public @NotNull ClientColumn column(String name) {
        var column = map.get(name);

        if (column == null) {
            throw new IgniteException("Column is not present in schema: " + name);
        }

        return column;
    }

    /**
     * Gets a column by name.
     *
     * @param name Column name.
     * @return Column by name.
     */
    public @Nullable ClientColumn columnSafe(String name) {
        return map.get(name);
    }

    /**
     * Returns key column count.
     *
     * @return Key column count.
     */
    public int keyColumnCount() {
        return keyColumnCount;
    }

    public <T> Marshaller getMarshaller(Mapper mapper, TuplePart part) {
        // TODO: Cache Marshallers (IGNITE-16094).
        return createMarshaller(mapper, part);
    }

    private Marshaller createMarshaller(Mapper mapper, TuplePart part) {
        int colCount = columns.length;
        int firstColIdx = 0;

        if (part == TuplePart.KEY) {
            colCount = keyColumnCount;
        } else if (part == TuplePart.VAL) {
            colCount = columns.length - keyColumnCount;
            firstColIdx = keyColumnCount;
        }

        MarshallerColumn[] cols = new MarshallerColumn[colCount];

        for (int i = 0; i < colCount; i++) {
            var col = columns[i  + firstColIdx];

            // TODO: Pass default value supplier that indicates "no value" (IGNITE-16093).
            cols[i] = new MarshallerColumn(col.name(), mode(col.type()));
        }

        return Marshaller.createMarshaller(cols, mapper, part == TuplePart.KEY);
    }

    private static BinaryMode mode(int dataType) {
        switch (dataType) {
            case BOOLEAN:
                throw new IgniteException("TODO: " + dataType);

            case INT8:
                return BinaryMode.BYTE;

            case INT16:
                return BinaryMode.SHORT;

            case INT32:
                return BinaryMode.INT;

            case INT64:
                return BinaryMode.LONG;

            case FLOAT:
                return BinaryMode.FLOAT;

            case DOUBLE:
                return BinaryMode.DOUBLE;

            case ClientDataType.UUID:
                return BinaryMode.UUID;

            case STRING:
                return BinaryMode.STRING;

            case BYTES:
                return BinaryMode.BYTE_ARR;

            case DECIMAL:
                return BinaryMode.DECIMAL;

            // Falls through.
            case BIGINTEGER:
            case NUMBER:
                return BinaryMode.NUMBER;

            case BITMASK:
                return BinaryMode.BITSET;

            case DATE:
                return BinaryMode.DATE;

            case TIME:
                return BinaryMode.TIME;

            case DATETIME:
                return BinaryMode.DATETIME;

            case TIMESTAMP:
                return BinaryMode.TIMESTAMP;

            default:
                throw new IgniteException("Unknown client data type: " + dataType);
        }
    }
}
