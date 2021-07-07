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

package org.apache.ignite.internal.schema;

import java.util.Arrays;
import java.util.UUID;
import org.junit.jupiter.api.Test;

import static org.apache.ignite.internal.schema.NativeTypes.INT8;
import static org.apache.ignite.internal.schema.NativeTypes.BYTES;
import static org.apache.ignite.internal.schema.NativeTypes.INT32;
import static org.apache.ignite.internal.schema.NativeTypes.INT16;
import static org.apache.ignite.internal.schema.NativeTypes.STRING;
import static org.apache.ignite.internal.schema.NativeTypes.UUID;
import static org.junit.jupiter.api.Assertions.assertArrayEquals;

/**
 * Validate row layout for different schema configurations.
 */
public class RowAssemblerSimpleSchemaTest {
    /** Uuid test value. */
    public final UUID uuidVal = new UUID(-5204230847775358097L, 4916207022290092939L);

    /** Table ID test value. */
    public final UUID tableId = java.util.UUID.randomUUID();

    /**
     * Validate row layout for schema of fix-len non-null key and fix-len nullable value.
     */
    @Test
    public void fixedKeyFixedNullableValue() {
        SchemaDescriptor schema = new SchemaDescriptor(tableId, 42,
            new Column[] {new Column("keyIntCol", INT32, false)},
            new Column[] {new Column("valIntCol", INT32, true)});

        assertRowBytesEquals(
            new byte[] {42, 0, 26, 0, 33, 0, 0, 0, 8, 0, 0, 0, 33, 0, 0, 0, 9, 0, 0, 0, 0, -71, -1, -1, -1},
            new RowAssembler(schema, 24, 0, 0)
                .appendInt(33)
                .appendInt(-71)
                .build());

        // Null value.
        assertRowBytesEquals(
            new byte[] {42, 0, 26, 0, 33, 0, 0, 0, 8, 0, 0, 0, 33, 0, 0, 0, 5, 0, 0, 0, 1},
            new RowAssembler(schema, 21, 0, 0)
                .appendInt(33)
                .appendNull()
                .build());

        // No value.
        assertRowBytesEquals(
            new byte[] {42, 0, 27, 0, 33, 0, 0, 0, 8, 0, 0, 0, 33, 0, 0, 0},
            new RowAssembler(schema, 16, 0, 0)
                .appendInt(33)
                .build());
    }

    /**
     * Validate row layout for schema of fix-len non-null key and fix-len non-null value.
     */
    @Test
    public void fixedKeyFixedValue() {
        SchemaDescriptor schema = new SchemaDescriptor(tableId, 42,
            new Column[] {new Column("keyShortCol", INT16, false)},
            new Column[] {new Column("valShortCol", INT16, false)});

        // With value.
        assertRowBytesEquals(
            new byte[] {42, 0, 30, 0, 33, 0, 0, 0, 6, 0, 0, 0, 33, 0, 6, 0, 0, 0, -71, -1},
            new RowAssembler(schema, 20, 0, 0)
                .appendShort((short)33)
                .appendShort((short)-71)
                .build());

        // No value.
        assertRowBytesEquals(
            new byte[] {42, 0, 31, 0, 33, 0, 0, 0, 6, 0, 0, 0, 33, 0},
            new RowAssembler(schema, 14, 0, 0)
                .appendShort((short)33)
                .build());
    }

    /**
     * Validate row layout for schema of fix-len non-null key and var-len nullable value.
     */
    @Test
    public void fixedKeyVarlenNullableValue() {
        SchemaDescriptor schema = new SchemaDescriptor(tableId, 42,
            new Column[] {new Column("keyShortCol", INT16, false)},
            new Column[] {new Column("valStrCol", STRING, true)});

        assertRowBytesEquals(
            new byte[] {42, 0, 26, 0, -33, -1, -1, -1, 6, 0, 0, 0, -33, -1, 8, 0, 0, 0, 0, 118, 97, 108},
            new RowAssembler(schema, 0, 0, 1)
                .appendShort((short)-33)
                .appendString("val")
                .build());

        // Null value.
        assertRowBytesEquals(
            new byte[] {42, 0, 26, 0, 33, 0, 0, 0, 6, 0, 0, 0, 33, 0, 5, 0, 0, 0, 1},
            new RowAssembler(schema, 0, 0, 0)
                .appendShort((short)33)
                .appendNull()
                .build());

        // No value.
        assertRowBytesEquals(
            new byte[] {42, 0, 27, 0, 33, 0, 0, 0, 6, 0, 0, 0, 33, 0},
            new RowAssembler(schema, 0, 0, 0)
                .appendShort((short)33)
                .build());
    }

    /**
     * Validate row layout for schema of fix-len non-null key and var-len non-null value.
     */
    @Test
    public void fixedKeyVarlenValue() {
        SchemaDescriptor schema = new SchemaDescriptor(tableId, 42,
            new Column[] {new Column("keyShortCol", INT16, false)},
            new Column[] {new Column("valStrCol", STRING, false)});

        assertRowBytesEquals(
            new byte[] {42, 0, 30, 0, -33, -1, -1, -1, 6, 0, 0, 0, -33, -1, 7, 0, 0, 0, 118, 97, 108},
            new RowAssembler(schema, 0, 0, 1)
                .appendShort((short)-33)
                .appendString("val")
                .build());

        // No value.
        assertRowBytesEquals(
            new byte[] {42, 0, 31, 0, 33, 0, 0, 0, 6, 0, 0, 0, 33, 0},
            new RowAssembler(schema, 0, 0, 0)
                .appendShort((short)33)
                .build());
    }

    /**
     * Validate row layout for schema of fix-len nullable key and fix-len non-null value.
     */
    @Test
    public void fixedNullableKeyFixedValue() {
        SchemaDescriptor schema = new SchemaDescriptor(tableId, 42,
            new Column[] {new Column("keyShortCol", INT16, true)},
            new Column[] {new Column("valByteCol", INT8, false)});

        assertRowBytesEquals(
            new byte[] {42, 0, 28, 0, -33, -1, -1, -1, 7, 0, 0, 0, 0, -33, -1, 5, 0, 0, 0, 71},
            new RowAssembler(schema, 0, 0, 0)
                .appendShort((short)-33)
                .appendByte((byte)71)
                .build());

        // Null key.
        assertRowBytesEquals(
            new byte[] {42, 0, 28, 0, 0, 0, 0, 0, 5, 0, 0, 0, 1, 5, 0, 0, 0, -71},
            new RowAssembler(schema, 0, 0, 0)
                .appendNull()
                .appendByte((byte)-71)
                .build());

        // No value.
        assertRowBytesEquals(
            new byte[] {42, 0, 29, 0, 33, 0, 0, 0, 7, 0, 0, 0, 0, 33, 0},
            new RowAssembler(schema, 0, 0, 0)
                .appendShort((short)33)
                .build());
    }

    /**
     * Validate row layout for schema of fix-len nullable key and fix-len nullable value.
     */
    @Test
    public void fixedNullableKeyFixedNullableValue() {
        SchemaDescriptor schema = new SchemaDescriptor(tableId, 42,
            new Column[] {new Column("keyShortCol", INT16, true)},
            new Column[] {new Column("valShortCol", INT16, true)});

        // Null key.
        assertRowBytesEquals(
            new byte[] {42, 0, 24, 0, 0, 0, 0, 0, 5, 0, 0, 0, 1, 7, 0, 0, 0, 0, 33, 0},
            new RowAssembler(schema, 0, 0, 0)
                .appendNull()
                .appendShort((short)33)
                .build());

        // Null value.
        assertRowBytesEquals(
            new byte[] {42, 0, 24, 0, 33, 0, 0, 0, 7, 0, 0, 0, 0, 33, 0, 5, 0, 0, 0, 1},
            new RowAssembler(schema, 0, 0, 0)
                .appendShort((short)33)
                .appendNull()
                .build());

        // Null both.
        assertRowBytesEquals(
            new byte[] {42, 0, 24, 0, 0, 0, 0, 0, 5, 0, 0, 0, 1, 5, 0, 0, 0, 1},
            new RowAssembler(schema, 0, 0, 0)
                .appendNull()
                .appendNull()
                .build());

        // No value.
        assertRowBytesEquals(
            new byte[] {42, 0, 25, 0, 33, 0, 0, 0, 7, 0, 0, 0, 0, 33, 0},
            new RowAssembler(schema, 0, 0, 0)
                .appendShort((short)33)
                .build());

    }

    /**
     * Validate row layout for schema of fix-len nullable key and var-len nullable value.
     */
    @Test
    public void fixedNullableKeyVarlenNullableValue() {
        SchemaDescriptor schema = new SchemaDescriptor(tableId, 42,
            new Column[] {new Column("keyIntCol", INT16, true)},
            new Column[] {new Column("valStrCol", STRING, true)});

        assertRowBytesEquals(
            new byte[] {42, 0, 24, 0, 33, 0, 0, 0, 7, 0, 0, 0, 0, 33, 0, 8, 0, 0, 0, 0, 118, 97, 108},
            new RowAssembler(schema, 0, 0, 1)
                .appendShort((short)33)
                .appendString("val")
                .build());

        // Null key.
        assertRowBytesEquals(
            new byte[] {42, 0, 24, 0, 0, 0, 0, 0, 5, 0, 0, 0, 1, 8, 0, 0, 0, 0, 118, 97, 108},
            new RowAssembler(schema, 0, 0, 1)
                .appendNull()
                .appendString("val")
                .build());

        // Null value.
        assertRowBytesEquals(
            new byte[] {42, 0, 24, 0, 33, 0, 0, 0, 7, 0, 0, 0, 0, 33, 0, 5, 0, 0, 0, 1},
            new RowAssembler(schema, 0, 0, 0)
                .appendShort((short)33)
                .appendNull()
                .build());

        // Null both.
        assertRowBytesEquals(
            new byte[] {42, 0, 24, 0, 0, 0, 0, 0, 5, 0, 0, 0, 1, 5, 0, 0, 0, 1},
            new RowAssembler(schema, 0, 0, 0)
                .appendNull()
                .appendNull()
                .build());

        // No value.
        assertRowBytesEquals(
            new byte[] {42, 0, 25, 0, 33, 0, 0, 0, 7, 0, 0, 0, 0, 33, 0},
            new RowAssembler(schema, 0, 0, 0)
                .appendShort((short)33)
                .build());
    }

    /**
     * Validate row layout for schema of fix-len nullable key and var-len non-null value.
     */
    @Test
    public void fixedNullableKeyVarlenValue() {
        SchemaDescriptor schema = new SchemaDescriptor(tableId, 42,
            new Column[] {new Column("keyByteCol", INT8, true)},
            new Column[] {new Column("valStrCol", STRING, false)});

        assertRowBytesEquals(
            new byte[] {42, 0, 28, 0, 33, 0, 0, 0, 6, 0, 0, 0, 0, 33, 7, 0, 0, 0, 118, 97, 108},
            new RowAssembler(schema, 0, 0, 1)
                .appendByte((byte)33)
                .appendString("val").build());

        // Null key.
        assertRowBytesEquals(
            new byte[] {42, 0, 28, 0, 0, 0, 0, 0, 5, 0, 0, 0, 1, 7, 0, 0, 0, 118, 97, 108},
            new RowAssembler(schema, 0, 0, 1)
                .appendNull()
                .appendString("val").build());

        // No value.
        assertRowBytesEquals(
            new byte[] {42, 0, 29, 0, 33, 0, 0, 0, 6, 0, 0, 0, 0, 33},
            new RowAssembler(schema, 0, 0, 0)
                .appendByte((byte)33)
                .build());
    }

    /**
     * Validate row layout for schema of var-len non-null key and fix-len nullable value.
     */
    @Test
    public void varlenKeyFixedNullableValue() {
        SchemaDescriptor schema = new SchemaDescriptor(tableId, 42,
            new Column[] {new Column("keyStrCol", STRING, false)},
            new Column[] {new Column("valUuidCol", UUID, true)});

        assertRowBytesEquals(
            new byte[] {
                42, 0, 26, 0, 95, -98, 1, 0, 7, 0, 0, 0, 107, 101, 121,
                21, 0, 0, 0, 0, -117, -61, -31, 85, 61, -32, 57, 68, 111, 67, 56, -3, -99, -37, -58, -73},
            new RowAssembler(schema, 0, 1, 0)
                .appendString("key")
                .appendUuid(uuidVal)
                .build());

        // Null value.
        assertRowBytesEquals(
            new byte[] {42, 0, 26, 0, 95, -98, 1, 0, 7, 0, 0, 0,107, 101, 121, 5, 0, 0, 0, 1},
            new RowAssembler(schema, 0, 1, 0)

                .appendString("key")
                .appendNull()
                .build());

        // No value.
        assertRowBytesEquals(
            new byte[] {42, 0, 27, 0, 95, -98, 1, 0, 7, 0, 0, 0, 107, 101, 121},
            new RowAssembler(schema, 0, 1, 0)
                .appendString("key")
                .build());

    }

    /**
     * Validate row layout for schema of var-len non-null key and fix-len non-null value.
     */
    @Test
    public void varlenKeyFixedValue() {
        SchemaDescriptor schema = new SchemaDescriptor(tableId, 42,
            new Column[] {new Column("keyStrCol", STRING, false)},
            new Column[] {new Column("valUuidCol", UUID, false)});

        assertRowBytesEquals(
            new byte[] {
                42, 0, 30, 0, 95, -98, 1, 0, 7, 0, 0, 0, 107, 101, 121,
                20, 0, 0, 0, -117, -61, -31, 85, 61, -32, 57, 68, 111, 67, 56, -3, -99, -37, -58, -73},
            new RowAssembler(schema, 0, 1, 0)
                .appendString("key")
                .appendUuid(uuidVal)
                .build());

        // No value.
        assertRowBytesEquals(
            new byte[] {42, 0, 31, 0, 95, -98, 1, 0, 7, 0, 0, 0, 107, 101, 121},
            new RowAssembler(schema, 0, 1, 0)
                .appendString("key")
                .build());

    }

    /**
     * Validate row layout for schema of var-len non-null key and var-len nullable value.
     */
    @Test
    public void varlenKeyVarlenNullableValue() {
        SchemaDescriptor schema = new SchemaDescriptor(tableId, 42,
            new Column[] {new Column("keyStrCol", STRING, false)},
            new Column[] {new Column("valBytesCol", BYTES, true)});

        assertRowBytesEquals(
            new byte[] {42, 0, 26, 0, 95, -98, 1, 0, 7, 0, 0, 0, 107, 101, 121, 9, 0, 0, 0, 0, -1, 1, 0, 120},
            new RowAssembler(schema, 0, 1, 1)
                .appendString("key")
                .appendBytes(new byte[] {-1, 1, 0, 120})
                .build());

        // Null value.
        assertRowBytesEquals(
            new byte[] {42, 0, 26, 0, 95, -98, 1, 0, 7, 0, 0, 0, 107, 101, 121, 5, 0, 0, 0, 1},
            new RowAssembler(schema, 0, 1, 0)
                .appendString("key")
                .appendNull()
                .build());

        // No value.
        assertRowBytesEquals(
            new byte[] {42, 0, 27, 0, 95, -98, 1, 0, 7, 0, 0, 0, 107, 101, 121},
            new RowAssembler(schema, 0, 1, 0)
                .appendString("key")
                .build());

    }

    /**
     * Validate row layout for schema of var-len non-null key and var-len non-null value.
     */
    @Test
    public void varlenKeyVarlenValue() {
        SchemaDescriptor schema = new SchemaDescriptor(tableId, 42,
            new Column[] {new Column("keyStrCol", STRING, false)},
            new Column[] {new Column("valBytesCol", BYTES, false)});

        assertRowBytesEquals(
            new byte[] {42, 0, 30, 0, 95, -98, 1, 0, 7, 0, 0, 0, 107, 101, 121, 8, 0, 0, 0, -1, 1, 0, 120},
            new RowAssembler(schema, 0, 1, 1)
                .appendString("key")
                .appendBytes(new byte[] {-1, 1, 0, 120})
                .build());

        // No value.
        assertRowBytesEquals(
            new byte[] {42, 0, 31, 0, 95, -98, 1, 0, 7, 0, 0, 0, 107, 101, 121},
            new RowAssembler(schema, 0, 1, 0)
                .appendString("key")
                .build());
    }

    /**
     * Validate row layout for schema of var-len nullable key and fix-len nullable value.
     */
    @Test
    public void varlenNullableKeyFixedNullableValue() {
        SchemaDescriptor schema = new SchemaDescriptor(tableId, 42,
            new Column[] {new Column("keyStrCol", STRING, true)},
            new Column[] {new Column("valShortCol", INT16, true)});

        assertRowBytesEquals(
            new byte[] {42, 0, 24, 0, 95, -98, 1, 0, 8, 0, 0, 0, 0, 107, 101, 121, 7, 0, 0, 0, 0, -71, -1},
            new RowAssembler(schema, 0, 1, 0)
                .appendString("key")
                .appendShort((short)-71)
                .build());

        // Null key.
        assertRowBytesEquals(
            new byte[] {42, 0, 24, 0, 0, 0, 0, 0, 5, 0, 0, 0, 1, 7, 0, 0, 0, 0, 71, 0},
            new RowAssembler(schema, 0, 0, 0)
                .appendNull()
                .appendShort((short)71)
                .build());

        // Null value.
        assertRowBytesEquals(
            new byte[] {42, 0, 24, 0, 95, -98, 1, 0, 8, 0, 0, 0, 0, 107, 101, 121, 5, 0, 0, 0, 1},
            new RowAssembler(schema, 0, 1, 0)
                .appendString("key")
                .appendNull()
                .build());

        // Null both.
        assertRowBytesEquals(
            new byte[] {42, 0, 24, 0, 0, 0, 0, 0, 5, 0, 0, 0, 1, 5, 0, 0, 0, 1},
            new RowAssembler(schema, 0, 0, 0)
                .appendNull()
                .appendNull()
                .build());

        // No value.
        assertRowBytesEquals(
            new byte[] {42, 0, 25, 0, 95, -98, 1, 0, 8, 0, 0, 0, 0, 107, 101, 121},
            new RowAssembler(schema, 0, 1, 0)
                .appendString("key")
                .build());

    }

    /**
     * Validate row layout for schema of var-len nullable key and fix-len non-null value.
     */
    @Test
    public void varlenNullableKeyFixedValue() {
        SchemaDescriptor schema = new SchemaDescriptor(tableId, 42,
            new Column[] {new Column("keyStrCol", STRING, true)},
            new Column[] {new Column("valShortCol", INT16, false)});

        assertRowBytesEquals(
            new byte[] {42, 0, 28, 0, 95, -98, 1, 0, 8, 0, 0, 0, 0, 107, 101, 121, 6, 0, 0, 0, -71, -1},
            new RowAssembler(schema, 0, 1, 0)
                .appendString("key")
                .appendShort((short)-71L)
                .build());

        // Null key.
        assertRowBytesEquals(
            new byte[] {42, 0, 28, 0, 0, 0, 0, 0, 5, 0, 0, 0, 1, 6, 0, 0, 0, 71, 0},
            new RowAssembler(schema, 0, 0, 0)
                .appendNull()
                .appendShort((short)71)
                .build());

        // No value.
        assertRowBytesEquals(
            new byte[] {42, 0, 29, 0, 95, -98, 1, 0, 8, 0, 0, 0, 0, 107, 101, 121},
            new RowAssembler(schema, 0, 1, 0)
                .appendString("key")
                .build());
    }

    /**
     * Validate row layout for schema of var-len nullable key and var-len nullable value.
     */
    @Test
    public void varlenNullableKeyVarlenNullableValue() {
        SchemaDescriptor schema = new SchemaDescriptor(tableId, 42,
            new Column[] {new Column("keyStrCol", STRING, true)},
            new Column[] {new Column("valBytesCol", BYTES, true)});

        assertRowBytesEquals(
            new byte[] {42, 0, 24, 0, 95, -98, 1, 0, 8, 0, 0, 0, 0, 107, 101, 121, 9, 0, 0, 0, 0, -1, 1, 0, 120},
            new RowAssembler(schema, 0, 1, 1)
                .appendString("key")
                .appendBytes(new byte[] {-1, 1, 0, 120})
                .build());

        // Null key.
        assertRowBytesEquals(
            new byte[] {42, 0, 24, 0, 0, 0, 0, 0, 5, 0, 0, 0, 1, 9, 0, 0, 0, 0, -1, 1, 0, 120},
            new RowAssembler(schema, 0, 0, 1)
                .appendNull()
                .appendBytes(new byte[] {-1, 1, 0, 120})
                .build());

        // Null value.
        assertRowBytesEquals(
            new byte[] {42, 0, 24, 0, 95, -98, 1, 0, 8, 0, 0, 0, 0, 107, 101, 121, 5, 0, 0, 0, 1},
            new RowAssembler(schema, 0, 1, 0)
                .appendString("key")
                .appendNull()
                .build());

        // Null both.
        assertRowBytesEquals(
            new byte[] {42, 0, 24, 0, 0, 0, 0, 0, 5, 0, 0, 0, 1, 5, 0, 0, 0, 1},
            new RowAssembler(schema, 0, 0, 0)
                .appendNull()
                .appendNull()
                .build());

        // No value.
        assertRowBytesEquals(
            new byte[] {42, 0, 25, 0, 95, -98, 1, 0, 8, 0, 0, 0, 0, 107, 101, 121},
            new RowAssembler(schema, 0, 1, 0)
                .appendString("key")
                .build());
    }

    /**
     * Validate row layout for schema of var-len nullable key and var-len non-null value.
     */
    @Test
    public void varlenNullableKeyVarlenValue() {
        SchemaDescriptor schema = new SchemaDescriptor(tableId, 42,
            new Column[] {new Column("keyStrCol", STRING, true)},
            new Column[] {new Column("valBytesCol", BYTES, false)});

        assertRowBytesEquals(
            new byte[] {42, 0, 28, 0, 95, -98, 1, 0, 8, 0, 0, 0, 0, 107, 101, 121, 8, 0, 0, 0, -1, 1, 0, 120},
            new RowAssembler(schema, 0, 1, 1)
                .appendString("key")
                .appendBytes(new byte[] {-1, 1, 0, 120})
                .build());

        // Null key.
        assertRowBytesEquals(
            new byte[] {42, 0, 28, 0, 0, 0, 0, 0, 5, 0, 0, 0, 1, 8, 0, 0, 0, -1, 1, 0, 120},
            new RowAssembler(schema, 0, 0, 1)
                .appendNull()
                .appendBytes(new byte[] {-1, 1, 0, 120})
                .build());

        // No value.
        assertRowBytesEquals(
            new byte[] {42, 0, 29, 0, 95, -98, 1, 0, 8, 0, 0, 0, 0, 107, 101, 121},
            new RowAssembler(schema, 0, 1, 0)
                .appendString("key")
                .build());
    }

    /**
     * Validate row layout for key\value columns of different types.
     */
    @Test
    public void mixedTypes() {
        SchemaDescriptor schema = new SchemaDescriptor(tableId, 42,
            new Column[] {
                new Column("keyShortCol", INT16, false),
                new Column("keyStrCol", STRING, false)
            },
            new Column[] {
                new Column("valIntCol", INT32, true),
                new Column("valStrCol", STRING, true)
            });

        assertRowBytesEquals(new byte[] {
                42, 0, 26, 0, -110, -109, 94, -68,
                12, 0, 0, 0, 33, 0, 107, 101, 121, 115, 116, 114,
                15, 0, 0, 0, 0, 73, 0, 0, 0, 118, 97, 108, 115, 116, 114},
            new RowAssembler(schema, 0, 1, 1)
                .appendShort((short)33)
                .appendString("keystr")
                .appendInt(73)
                .appendString("valstr")
                .build());

        // Null value.
        assertRowBytesEquals(new byte[] {
                42, 0, 26, 0, 32, 99, 115, -49,
                13, 0, 0, 0, 33, 0, 107, 101, 121, 115, 116, 114, 50,
                5, 0, 0, 0, 3},
            new RowAssembler(schema, 0, 1, 0)
                .appendShort((short)33)
                .appendString("keystr2")
                .appendNull()
                .appendNull()
                .build());

        // No value.
        assertRowBytesEquals(new byte[] {
                42, 0, 27, 0, -110, -109, 94, -68,
                12, 0, 0, 0, 33, 0, 107, 101, 121, 115, 116, 114},
            new RowAssembler(schema, 0, 1, 0)
                .appendShort((short)33)
                .appendString("keystr")
                .build());
    }

    /**
     * @param expected Expected row bytes.
     * @param actual Actual row bytes.
     */
    private void assertRowBytesEquals(byte[] expected, byte[] actual) {
        assertArrayEquals(expected, actual, Arrays.toString(actual));
    }
}
