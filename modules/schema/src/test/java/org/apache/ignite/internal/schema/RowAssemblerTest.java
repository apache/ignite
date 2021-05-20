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

import static org.apache.ignite.internal.schema.NativeTypes.BYTE;
import static org.apache.ignite.internal.schema.NativeTypes.BYTES;
import static org.apache.ignite.internal.schema.NativeTypes.INTEGER;
import static org.apache.ignite.internal.schema.NativeTypes.SHORT;
import static org.apache.ignite.internal.schema.NativeTypes.STRING;
import static org.apache.ignite.internal.schema.NativeTypes.UUID;
import static org.junit.jupiter.api.Assertions.assertArrayEquals;

/**
 * Validate row layout for different schema configurations.
 */
public class RowAssemblerTest {
    /** Uuid test value. */
    public final java.util.UUID uuidVal = new UUID(-5204230847775358097L, 4916207022290092939L);

    /** Table ID test value. */
    public final java.util.UUID tableId = java.util.UUID.randomUUID();

    @Test
    public void fixedKeyFixedNullableValue() {
        Column[] keyCols = new Column[] {new Column("keyIntCol", INTEGER, false)};
        Column[] valCols = new Column[] {new Column("valIntCol", INTEGER, true)};

        SchemaDescriptor schema = new SchemaDescriptor(tableId, 42, keyCols, valCols);

        {
            RowAssembler asm = new RowAssembler(schema, 0, 0, 0);

            asm.appendInt(33);
            asm.appendInt(-71);

            assertRowBytesEquals(new byte[] {42, 0, 26, 0, 0, 0, 0, 0, 8, 0, 0, 0, 33, 0, 0, 0, 9, 0, 0, 0, 0, -71, -1, -1, -1}, asm.build());
        }

        { // Null value.
            RowAssembler asm = new RowAssembler(schema, 0, 0, 0);

            asm.appendInt(-33);
            asm.appendNull();

            assertRowBytesEquals(new byte[] {42, 0, 26, 0, 0, 0, 0, 0, 8, 0, 0, 0, -33, -1, -1, -1, 5, 0, 0, 0, 1}, asm.build());
        }

        { // No value.
            RowAssembler asm = new RowAssembler(schema, 0, 0, 0);

            asm.appendInt(-33);

            assertRowBytesEquals(new byte[] {42, 0, 27, 0, 0, 0, 0, 0, 8, 0, 0, 0, -33, -1, -1, -1}, asm.build());
        }
    }

    @Test
    public void fixedKeyFixedValue() {
        Column[] keyCols = new Column[] {new Column("keyShortCol", SHORT, false)};
        Column[] valCols = new Column[] {new Column("valShortCol", SHORT, false)};

        SchemaDescriptor schema = new SchemaDescriptor(tableId, 42, keyCols, valCols);

        { // With value.
            RowAssembler asm = new RowAssembler(schema, 0, 0, 0);

            asm.appendShort((short)33);
            asm.appendShort((short)71L);

            assertRowBytesEquals(new byte[] {42, 0, 30, 0, 0, 0, 0, 0, 6, 0, 0, 0, 33, 0, 6, 0, 0, 0, 71, 0}, asm.build());
        }

        { // No value.
            RowAssembler asm = new RowAssembler(schema, 0, 0, 0);

            asm.appendShort((short)-33);

            assertRowBytesEquals(new byte[] {42, 0, 31, 0, 0, 0, 0, 0, 6, 0, 0, 0, -33, -1}, asm.build());
        }
    }

    @Test
    public void fixedKeyVarlenNullableValue() {
        Column[] keyCols = new Column[] {new Column("keyShortCol", SHORT, false)};
        Column[] valCols = new Column[] {new Column("valStrCol", STRING, true)};

        SchemaDescriptor schema = new SchemaDescriptor(tableId, 42, keyCols, valCols);

        {
            RowAssembler asm = new RowAssembler(schema, 0, 0, 1);

            asm.appendShort((short)-33);
            asm.appendString("val");

            assertRowBytesEquals(new byte[] {42, 0, 10, 0, 0, 0, 0, 0, 6, 0, 0, 0, -33, -1, 12, 0, 0, 0, 0, 1, 0, 9, 0, 118, 97, 108}, asm.build());
        }

        { // Null value.
            RowAssembler asm = new RowAssembler(schema, 0, 0, 0);

            asm.appendShort((short)33);
            asm.appendNull();

            assertRowBytesEquals(new byte[] {42, 0, 26, 0, 0, 0, 0, 0, 6, 0, 0, 0, 33, 0, 5, 0, 0, 0, 1}, asm.build());
        }

        { // No value.
            RowAssembler asm = new RowAssembler(schema, 0, 0, 0);

            asm.appendShort((short)33);

            assertRowBytesEquals(new byte[] {42, 0, 27, 0, 0, 0, 0, 0, 6, 0, 0, 0, 33, 0}, asm.build());
        }
    }

    @Test
    public void fixedKeyVarlenValue() {
        Column[] keyCols = new Column[] {new Column("keyShortCol", SHORT, false)};
        Column[] valCols = new Column[] {new Column("valStrCol", STRING, false)};

        SchemaDescriptor schema = new SchemaDescriptor(tableId, 42, keyCols, valCols);

        {
            RowAssembler asm = new RowAssembler(schema, 0, 0, 1);

            asm.appendShort((short)-33);
            asm.appendString("val");

            assertRowBytesEquals(new byte[] {42, 0, 14, 0, 0, 0, 0, 0, 6, 0, 0, 0, -33, -1, 11, 0, 0, 0, 1, 0, 8, 0, 118, 97, 108}, asm.build());
        }

        { // No value.
            RowAssembler asm = new RowAssembler(schema, 0, 0, 0);

            asm.appendShort((short)33);

            assertRowBytesEquals(new byte[] {42, 0, 31, 0, 0, 0, 0, 0, 6, 0, 0, 0, 33, 0}, asm.build());
        }
    }

    @Test
    public void fixedNullableKeyFixedValue() {
        Column[] keyCols = new Column[] {new Column("keyShortCol", SHORT, true)};
        Column[] valCols = new Column[] {new Column("valByteCol", BYTE, false)};

        SchemaDescriptor schema = new SchemaDescriptor(tableId, 42, keyCols, valCols);

        {
            RowAssembler asm = new RowAssembler(schema, 0, 0, 0);

            asm.appendShort((short)-33);
            asm.appendByte((byte)71);

            assertRowBytesEquals(new byte[] {42, 0, 28, 0, 0, 0, 0, 0, 7, 0, 0, 0, 0, -33, -1, 5, 0, 0, 0, 71}, asm.build());
        }

        { // Null key.
            RowAssembler asm = new RowAssembler(schema, 0, 0, 0);

            asm.appendNull();
            asm.appendByte((byte)-71);

            assertRowBytesEquals(new byte[] {42, 0, 28, 0, 0, 0, 0, 0, 5, 0, 0, 0, 1, 5, 0, 0, 0, -71}, asm.build());
        }

        { // No value.
            RowAssembler asm = new RowAssembler(schema, 0, 0, 0);

            asm.appendShort((short)33);

            assertRowBytesEquals(new byte[] {42, 0, 29, 0, 0, 0, 0, 0, 7, 0, 0, 0, 0, 33, 0}, asm.build());
        }
    }

    @Test
    public void fixedNullableKeyFixedNullableValue() {
        Column[] keyCols = new Column[] {new Column("keyShortCol", SHORT, true)};
        Column[] valCols = new Column[] {new Column("valShortCol", SHORT, true)};

        SchemaDescriptor schema = new SchemaDescriptor(tableId, 42, keyCols, valCols);

        {
            RowAssembler asm = new RowAssembler(schema, 0, 0, 0);

            asm.appendShort((short)-1133);
            asm.appendShort((short)-1071);

            assertRowBytesEquals(new byte[] {42, 0, 24, 0, 0, 0, 0, 0, 7, 0, 0, 0, 0, -109, -5, 7, 0, 0, 0, 0, -47, -5}, asm.build());
        }

        { // Null key.
            RowAssembler asm = new RowAssembler(schema, 0, 0, 0);

            asm.appendNull();
            asm.appendShort((short)1171);

            assertRowBytesEquals(new byte[] {42, 0, 24, 0, 0, 0, 0, 0, 5, 0, 0, 0, 1, 7, 0, 0, 0, 0, -109, 4}, asm.build());
        }

        { // Null value.
            RowAssembler asm = new RowAssembler(schema, 0, 0, 0);

            asm.appendShort((short)1133);
            asm.appendNull();

            assertRowBytesEquals(new byte[] {42, 0, 24, 0, 0, 0, 0, 0, 7, 0, 0, 0, 0, 109, 4, 5, 0, 0, 0, 1}, asm.build());
        }

        { // Null both.
            RowAssembler asm = new RowAssembler(schema, 0, 0, 0);

            asm.appendNull();
            asm.appendNull();

            assertRowBytesEquals(new byte[] {42, 0, 24, 0, 0, 0, 0, 0, 5, 0, 0, 0, 1, 5, 0, 0, 0, 1}, asm.build());
        }

        { // No value.
            RowAssembler asm = new RowAssembler(schema, 0, 0, 0);

            asm.appendShort((short)1133);

            assertRowBytesEquals(new byte[] {42, 0, 25, 0, 0, 0, 0, 0, 7, 0, 0, 0, 0, 109, 4}, asm.build());
        }
    }

    @Test
    public void fixedNullableKeyVarlenNullableValue() {
        Column[] keyCols = new Column[] {new Column("keyIntCol", INTEGER, true)};
        Column[] valCols = new Column[] {new Column("valStrCol", STRING, true)};

        SchemaDescriptor schema = new SchemaDescriptor(tableId, 42, keyCols, valCols);

        {
            RowAssembler asm = new RowAssembler(schema, 0, 0, 1);

            asm.appendInt(-33);
            asm.appendString("val");

            assertRowBytesEquals(new byte[] {42, 0, 8, 0, 0, 0, 0, 0, 9, 0, 0, 0, 0, -33, -1, -1, -1, 12, 0, 0, 0, 0, 1, 0, 9, 0, 118, 97, 108}, asm.build());
        }

        { // Null key.
            RowAssembler asm = new RowAssembler(schema, 0, 0, 1);

            asm.appendNull();
            asm.appendString("val");

            assertRowBytesEquals(new byte[] {42, 0, 8, 0, 0, 0, 0, 0, 5, 0, 0, 0, 1, 12, 0, 0, 0, 0, 1, 0, 9, 0, 118, 97, 108}, asm.build());
        }

        { // Null value.
            RowAssembler asm = new RowAssembler(schema, 0, 0, 0);

            asm.appendInt(33);
            asm.appendNull();

            assertRowBytesEquals(new byte[] {42, 0, 24, 0, 0, 0, 0, 0, 9, 0, 0, 0, 0, 33, 0, 0, 0, 5, 0, 0, 0, 1}, asm.build());
        }

        { // Null both.
            RowAssembler asm = new RowAssembler(schema, 0, 0, 0);

            asm.appendNull();
            asm.appendNull();

            assertRowBytesEquals(new byte[] {42, 0, 24, 0, 0, 0, 0, 0, 5, 0, 0, 0, 1, 5, 0, 0, 0, 1}, asm.build());
        }

        { // No value.
            RowAssembler asm = new RowAssembler(schema, 0, 0, 0);

            asm.appendInt(33);

            assertRowBytesEquals(new byte[] {42, 0, 25, 0, 0, 0, 0, 0, 9, 0, 0, 0, 0, 33, 0, 0, 0}, asm.build());
        }
    }

    @Test
    public void fixedNullableKeyVarlenValue() {
        Column[] keyCols = new Column[] {new Column("keyByteCol", BYTE, true)};
        Column[] valCols = new Column[] {new Column("valStrCol", STRING, false)};

        SchemaDescriptor schema = new SchemaDescriptor(tableId, 42, keyCols, valCols);

        {
            RowAssembler asm = new RowAssembler(schema, 0, 0, 1);

            asm.appendByte((byte)-33);
            asm.appendString("val");

            assertRowBytesEquals(new byte[] {42, 0, 12, 0, 0, 0, 0, 0, 6, 0, 0, 0, 0, -33, 11, 0, 0, 0, 1, 0, 8, 0, 118, 97, 108}, asm.build());
        }

        { // Null key.
            RowAssembler asm = new RowAssembler(schema, 0, 0, 1);

            asm.appendNull();
            asm.appendString("val");

            assertRowBytesEquals(new byte[] {42, 0, 12, 0, 0, 0, 0, 0, 5, 0, 0, 0, 1, 11, 0, 0, 0, 1, 0, 8, 0, 118, 97, 108}, asm.build());
        }

        { // No value.
            RowAssembler asm = new RowAssembler(schema, 0, 0, 0);

            asm.appendByte((byte)33);

            assertRowBytesEquals(new byte[] {42, 0, 29, 0, 0, 0, 0, 0, 6, 0, 0, 0, 0, 33}, asm.build());
        }
    }

    @Test
    public void varlenKeyFixedNullableValue() {
        Column[] keyCols = new Column[] {new Column("keyStrCol", STRING, false)};
        Column[] valCols = new Column[] {new Column("valUuidCol", UUID, true)};

        SchemaDescriptor schema = new SchemaDescriptor(tableId, 42, keyCols, valCols);

        {
            RowAssembler asm = new RowAssembler(schema, 0, 1, 0);

            asm.appendString("key");
            asm.appendUuid(uuidVal);

            assertRowBytesEquals(new byte[] {
                42, 0, 18, 0, 0, 0, 0, 0, 11, 0, 0, 0, 1, 0, 8, 0, 107, 101, 121,
                21, 0, 0, 0, 0, -117, -61, -31, 85, 61, -32, 57, 68, 111, 67, 56, -3, -99, -37, -58, -73}, asm.build());
        }

        { // Null value.
            RowAssembler asm = new RowAssembler(schema, 0, 1, 0);

            asm.appendString("key");
            asm.appendNull();

            assertRowBytesEquals(new byte[] {42, 0, 18, 0, 0, 0, 0, 0, 11, 0, 0, 0, 1, 0, 8, 0, 107, 101, 121, 5, 0, 0, 0, 1}, asm.build());
        }

        { // No value.
            RowAssembler asm = new RowAssembler(schema, 0, 1, 0);

            asm.appendString("key");

            assertRowBytesEquals(new byte[] {42, 0, 19, 0, 0, 0, 0, 0, 11, 0, 0, 0, 1, 0, 8, 0, 107, 101, 121}, asm.build());
        }
    }

    @Test
    public void varlenKeyFixedValue() {
        Column[] keyCols = new Column[] {new Column("keyStrCol", STRING, false)};
        Column[] valCols = new Column[] {new Column("valUuidCol", UUID, false)};

        SchemaDescriptor schema = new SchemaDescriptor(tableId, 42, keyCols, valCols);

        {
            RowAssembler asm = new RowAssembler(schema, 0, 1, 0);

            asm.appendString("key");
            asm.appendUuid(uuidVal);

            assertRowBytesEquals(new byte[] {
                42, 0, 22, 0, 0, 0, 0, 0, 11, 0, 0, 0, 1, 0, 8, 0, 107, 101, 121,
                20, 0, 0, 0, -117, -61, -31, 85, 61, -32, 57, 68, 111, 67, 56, -3, -99, -37, -58, -73}, asm.build());
        }

        { // No value.
            RowAssembler asm = new RowAssembler(schema, 0, 1, 0);

            asm.appendString("key");

            assertRowBytesEquals(new byte[] {42, 0, 23, 0, 0, 0, 0, 0, 11, 0, 0, 0, 1, 0, 8, 0, 107, 101, 121}, asm.build());
        }
    }

    @Test
    public void varlenKeyVarlenNullableValue() {
        Column[] keyCols = new Column[] {new Column("keyStrCol", STRING, false)};
        Column[] valCols = new Column[] {new Column("valBytesCol", BYTES, true)};

        SchemaDescriptor schema = new SchemaDescriptor(tableId, 42, keyCols, valCols);

        {
            RowAssembler asm = new RowAssembler(schema, 0, 1, 1);

            asm.appendString("key");
            asm.appendBytes(new byte[] {-1, 1, 0, 120});

            assertRowBytesEquals(new byte[] {42, 0, 2, 0, 0, 0, 0, 0, 11, 0, 0, 0, 1, 0, 8, 0, 107, 101, 121, 13, 0, 0, 0, 0, 1, 0, 9, 0, -1, 1, 0, 120}, asm.build());
        }

        { // Null value.
            RowAssembler asm = new RowAssembler(schema, 0, 1, 0);

            asm.appendString("key");
            asm.appendNull();

            assertRowBytesEquals(new byte[] {42, 0, 18, 0, 0, 0, 0, 0, 11, 0, 0, 0, 1, 0, 8, 0, 107, 101, 121, 5, 0, 0, 0, 1}, asm.build());
        }

        { // No value.
            RowAssembler asm = new RowAssembler(schema, 0, 1, 0);

            asm.appendString("key");

            assertRowBytesEquals(new byte[] {42, 0, 19, 0, 0, 0, 0, 0, 11, 0, 0, 0, 1, 0, 8, 0, 107, 101, 121}, asm.build());
        }
    }

    @Test
    public void varlenKeyVarlenValue() {
        Column[] keyCols = new Column[] {new Column("keyStrCol", STRING, false)};
        Column[] valCols = new Column[] {new Column("valBytesCol", BYTES, false)};

        SchemaDescriptor schema = new SchemaDescriptor(tableId, 42, keyCols, valCols);

        {
            RowAssembler asm = new RowAssembler(schema, 0, 1, 1);

            asm.appendString("key");
            asm.appendBytes(new byte[] {-1, 1, 0, 120});

            assertRowBytesEquals(new byte[] {42, 0, 6, 0, 0, 0, 0, 0, 11, 0, 0, 0, 1, 0, 8, 0, 107, 101, 121, 12, 0, 0, 0, 1, 0, 8, 0, -1, 1, 0, 120}, asm.build());
        }

        { // No value.
            RowAssembler asm = new RowAssembler(schema, 0, 1, 0);

            asm.appendString("key");

            assertRowBytesEquals(new byte[] {42, 0, 23, 0, 0, 0, 0, 0, 11, 0, 0, 0, 1, 0, 8, 0, 107, 101, 121}, asm.build());
        }
    }

    @Test
    public void varlenNullableKeyFixedNullableValue() {
        Column[] keyCols = new Column[] {new Column("keyStrCol", STRING, true)};
        Column[] valCols = new Column[] {new Column("valShortCol", SHORT, true)};

        SchemaDescriptor schema = new SchemaDescriptor(tableId, 42, keyCols, valCols);

        {
            RowAssembler asm = new RowAssembler(schema, 0, 1, 0);

            asm.appendString("key");
            asm.appendShort((short)-71);

            assertRowBytesEquals(new byte[] {42, 0, 16, 0, 0, 0, 0, 0, 12, 0, 0, 0, 0, 1, 0, 9, 0, 107, 101, 121, 7, 0, 0, 0, 0, -71, -1}, asm.build());
        }

        { // Null key.
            RowAssembler asm = new RowAssembler(schema, 0, 0, 0);

            asm.appendNull();
            asm.appendShort((short)71);

            assertRowBytesEquals(new byte[] {42, 0, 24, 0, 0, 0, 0, 0, 5, 0, 0, 0, 1, 7, 0, 0, 0, 0, 71, 0}, asm.build());
        }

        { // Null value.
            RowAssembler asm = new RowAssembler(schema, 0, 1, 0);

            asm.appendString("key");
            asm.appendNull();

            assertRowBytesEquals(new byte[] {42, 0, 16, 0, 0, 0, 0, 0, 12, 0, 0, 0, 0, 1, 0, 9, 0, 107, 101, 121, 5, 0, 0, 0, 1}, asm.build());
        }

        { // Null both.
            RowAssembler asm = new RowAssembler(schema, 0, 0, 0);

            asm.appendNull();
            asm.appendNull();

            assertRowBytesEquals(new byte[] {42, 0, 24, 0, 0, 0, 0, 0, 5, 0, 0, 0, 1, 5, 0, 0, 0, 1}, asm.build());
        }

        { // No value.
            RowAssembler asm = new RowAssembler(schema, 0, 1, 0);

            asm.appendString("key");

            assertRowBytesEquals(new byte[] {42, 0, 17, 0, 0, 0, 0, 0, 12, 0, 0, 0, 0, 1, 0, 9, 0, 107, 101, 121}, asm.build());
        }
    }

    @Test
    public void varlenNullableKeyFixedValue() {
        Column[] keyCols = new Column[] {new Column("keyStrCol", STRING, true)};
        Column[] valCols = new Column[] {new Column("valShortCol", SHORT, false)};

        SchemaDescriptor schema = new SchemaDescriptor(tableId, 42, keyCols, valCols);

        {
            RowAssembler asm = new RowAssembler(schema, 0, 1, 0);

            asm.appendString("key");
            asm.appendShort((short)-71L);

            assertRowBytesEquals(new byte[] {42, 0, 20, 0, 0, 0, 0, 0, 12, 0, 0, 0, 0, 1, 0, 9, 0, 107, 101, 121, 6, 0, 0, 0, -71, -1}, asm.build());
        }

        { // Null key.
            RowAssembler asm = new RowAssembler(schema, 0, 0, 0);

            asm.appendNull();
            asm.appendShort((short)71);

            assertRowBytesEquals(new byte[] {42, 0, 28, 0, 0, 0, 0, 0, 5, 0, 0, 0, 1, 6, 0, 0, 0, 71, 0}, asm.build());
        }

        { // No value.
            RowAssembler asm = new RowAssembler(schema, 0, 1, 0);

            asm.appendString("key");

            assertRowBytesEquals(new byte[] {42, 0, 21, 0, 0, 0, 0, 0, 12, 0, 0, 0, 0, 1, 0, 9, 0, 107, 101, 121}, asm.build());
        }
    }

    @Test
    public void varlenNullableKeyVarlenNullableValue() {
        Column[] keyCols = new Column[] {new Column("keyStrCol", STRING, true)};
        Column[] valCols = new Column[] {new Column("valBytesCol", BYTES, true)};

        SchemaDescriptor schema = new SchemaDescriptor(tableId, 42, keyCols, valCols);

        {
            RowAssembler asm = new RowAssembler(schema, 0, 1, 1);

            asm.appendString("key");
            asm.appendBytes(new byte[] {-1, 1, 0, 120});

            assertRowBytesEquals(new byte[] {42, 0, 0, 0, 0, 0, 0, 0, 12, 0, 0, 0, 0, 1, 0, 9, 0, 107, 101, 121, 13, 0, 0, 0, 0, 1, 0, 9, 0, -1, 1, 0, 120}, asm.build());
        }

        { // Null key.
            RowAssembler asm = new RowAssembler(schema, 0, 0, 1);

            asm.appendNull();
            asm.appendBytes(new byte[] {-1, 1, 0, 120});

            assertRowBytesEquals(new byte[] {42, 0, 8, 0, 0, 0, 0, 0, 5, 0, 0, 0, 1, 13, 0, 0, 0, 0, 1, 0, 9, 0, -1, 1, 0, 120}, asm.build());
        }

        { // Null value.
            RowAssembler asm = new RowAssembler(schema, 0, 1, 0);

            asm.appendString("key");
            asm.appendNull();

            assertRowBytesEquals(new byte[] {42, 0, 16, 0, 0, 0, 0, 0, 12, 0, 0, 0, 0, 1, 0, 9, 0, 107, 101, 121, 5, 0, 0, 0, 1}, asm.build());
        }

        { // Null both.
            RowAssembler asm = new RowAssembler(schema, 0, 0, 0);

            asm.appendNull();
            asm.appendNull();

            assertRowBytesEquals(new byte[] {42, 0, 24, 0, 0, 0, 0, 0, 5, 0, 0, 0, 1, 5, 0, 0, 0, 1}, asm.build());
        }

        { // No value.
            RowAssembler asm = new RowAssembler(schema, 0, 1, 0);

            asm.appendString("key");

            assertRowBytesEquals(new byte[] {42, 0, 17, 0, 0, 0, 0, 0, 12, 0, 0, 0, 0, 1, 0, 9, 0, 107, 101, 121}, asm.build());
        }
    }

    @Test
    public void varlenNullableKeyVarlenValue() {
        Column[] keyCols = new Column[] {new Column("keyStrCol", STRING, true)};
        Column[] valCols = new Column[] {new Column("valBytesCol", BYTES, false)};

        SchemaDescriptor schema = new SchemaDescriptor(tableId, 42, keyCols, valCols);

        {
            RowAssembler asm = new RowAssembler(schema, 0, 1, 1);

            asm.appendString("key");
            asm.appendBytes(new byte[] {-1, 1, 0, 120});

            assertRowBytesEquals(new byte[] {42, 0, 4, 0, 0, 0, 0, 0, 12, 0, 0, 0, 0, 1, 0, 9, 0, 107, 101, 121, 12, 0, 0, 0, 1, 0, 8, 0, -1, 1, 0, 120}, asm.build());
        }

        { // Null key.
            RowAssembler asm = new RowAssembler(schema, 0, 0, 1);

            asm.appendNull();
            asm.appendBytes(new byte[] {-1, 1, 0, 120});

            assertRowBytesEquals(new byte[] {42, 0, 12, 0, 0, 0, 0, 0, 5, 0, 0, 0, 1, 12, 0, 0, 0, 1, 0, 8, 0, -1, 1, 0, 120}, asm.build());
        }

        { // No value.
            RowAssembler asm = new RowAssembler(schema, 0, 1, 0);

            asm.appendString("key");

            assertRowBytesEquals(new byte[] {42, 0, 21, 0, 0, 0, 0, 0, 12, 0, 0, 0, 0, 1, 0, 9, 0, 107, 101, 121}, asm.build());
        }
    }

    /**
     * @param expected Expected row bytes.
     * @param actual Actual row bytes.
     */
    private void assertRowBytesEquals(byte[] expected, byte[] actual) {
        assertArrayEquals(expected, actual, Arrays.toString(actual));
    }
}
