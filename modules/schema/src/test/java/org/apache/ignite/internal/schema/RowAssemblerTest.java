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

import static org.apache.ignite.internal.schema.NativeType.BYTE;
import static org.apache.ignite.internal.schema.NativeType.BYTES;
import static org.apache.ignite.internal.schema.NativeType.INTEGER;
import static org.apache.ignite.internal.schema.NativeType.SHORT;
import static org.apache.ignite.internal.schema.NativeType.STRING;
import static org.apache.ignite.internal.schema.NativeType.UUID;
import static org.junit.jupiter.api.Assertions.assertArrayEquals;

/**
 * Validate row layout for different schema configurations.
 */
public class RowAssemblerTest {
    /** Uuid test value. */
    public final java.util.UUID uuidVal = new UUID(-5204230847775358097L, 4916207022290092939L);

    /**
     * Validate row layout for schema of fix-len non-null key and fix-len nullable value.
     */
    @Test
    public void testFixedKeyFixedNullableValue() {
        Column[] keyCols = new Column[] {new Column("keyIntCol", INTEGER, false)};
        Column[] valCols = new Column[] {new Column("valIntCol", INTEGER, true)};

        SchemaDescriptor schema = new SchemaDescriptor(42, keyCols, valCols);

        {
            RowAssembler asm = new RowAssembler(schema, 0, 0, 0);

            asm.appendInt(33);
            asm.appendInt(-71);

            assertRowBytesEquals(new byte[] {42, 0, 0, 0, 0, 0, 0, 0, 11, 0, 0, 0, 0, 0, 0, 33, 0, 0, 0, 11, 0, 0, 0, 0, 0, 0, -71, -1, -1, -1}, asm.build());
        }

        { // Null value.
            RowAssembler asm = new RowAssembler(schema, 0, 0, 0);

            asm.appendInt(-33);
            asm.appendNull();

            assertRowBytesEquals(new byte[] {42, 0, 0, 0, 0, 0, 0, 0, 11, 0, 0, 0, 0, 0, 0, -33, -1, -1, -1, 7, 0, 0, 0, 1, 0, 0}, asm.build());
        }

        { // No value.
            RowAssembler asm = new RowAssembler(schema, 0, 0, 0);

            asm.appendInt(-33);

            assertRowBytesEquals(new byte[] {42, 0, 2, 0, 0, 0, 0, 0, 11, 0, 0, 0, 0, 0, 0, -33, -1, -1, -1}, asm.build());
        }
    }

    /**
     * Validate row layout for schema of fix-len non-null key and fix-len non-null value.
     */
    @Test
    public void testFixedKeyFixedValue() {
        Column[] keyCols = new Column[] {new Column("keyShortCol", SHORT, false)};
        Column[] valCols = new Column[] {new Column("valShortCol", SHORT, false)};

        SchemaDescriptor schema = new SchemaDescriptor(42, keyCols, valCols);

        { // With value.
            RowAssembler asm = new RowAssembler(schema, 0, 0, 0);

            asm.appendShort((short)33);
            asm.appendShort((short)71L);

            assertRowBytesEquals(new byte[] {42, 0, 0, 0, 0, 0, 0, 0, 9, 0, 0, 0, 0, 0, 0, 33, 0, 9, 0, 0, 0, 0, 0, 0, 71, 0}, asm.build());
        }

        { // No value.
            RowAssembler asm = new RowAssembler(schema, 0, 0, 0);

            asm.appendShort((short)-33);

            assertRowBytesEquals(new byte[] {42, 0, 2, 0, 0, 0, 0, 0, 9, 0, 0, 0, 0, 0, 0, -33, -1}, asm.build());
        }
    }

    /**
     * Validate row layout for schema of fix-len non-null key and var-len nullable value.
     */
    @Test
    public void testFixedKeyVarlenNullableValue() {
        Column[] keyCols = new Column[] {new Column("keyShortCol", SHORT, false)};
        Column[] valCols = new Column[] {new Column("valStrCol", STRING, true)};

        SchemaDescriptor schema = new SchemaDescriptor(42, keyCols, valCols);

        {
            RowAssembler asm = new RowAssembler(schema, 0, 0, 1);

            asm.appendShort((short)-33);
            asm.appendString("val");

            assertRowBytesEquals(new byte[] {42, 0, 0, 0, 0, 0, 0, 0, 9, 0, 0, 0, 0, 0, 0, -33, -1, 12, 0, 0, 0, 0, 1, 0, 9, 0, 118, 97, 108}, asm.build());
        }

        { // Null value.
            RowAssembler asm = new RowAssembler(schema, 0, 0, 0);

            asm.appendShort((short)33);
            asm.appendNull();

            assertRowBytesEquals(new byte[] {42, 0, 0, 0, 0, 0, 0, 0, 9, 0, 0, 0, 0, 0, 0, 33, 0, 7, 0, 0, 0, 1, 0, 0}, asm.build());
        }

        { // No value.
            RowAssembler asm = new RowAssembler(schema, 0, 0, 0);

            asm.appendShort((short)33);

            assertRowBytesEquals(new byte[] {42, 0, 2, 0, 0, 0, 0, 0, 9, 0, 0, 0, 0, 0, 0, 33, 0}, asm.build());
        }
    }

    /**
     * Validate row layout for schema of fix-len non-null key and var-len non-null value.
     */
    @Test
    public void testFixedKeyVarlenValue() {
        Column[] keyCols = new Column[] {new Column("keyShortCol", SHORT, false)};
        Column[] valCols = new Column[] {new Column("valStrCol", STRING, false)};

        SchemaDescriptor schema = new SchemaDescriptor(42, keyCols, valCols);

        {
            RowAssembler asm = new RowAssembler(schema, 0, 0, 1);

            asm.appendShort((short)-33);
            asm.appendString("val");

            assertRowBytesEquals(new byte[] {42, 0, 0, 0, 0, 0, 0, 0, 9, 0, 0, 0, 0, 0, 0, -33, -1, 12, 0, 0, 0, 0, 1, 0, 9, 0, 118, 97, 108}, asm.build());
        }

        { // No value.
            RowAssembler asm = new RowAssembler(schema, 0, 0, 0);

            asm.appendShort((short)33);

            assertRowBytesEquals(new byte[] {42, 0, 2, 0, 0, 0, 0, 0, 9, 0, 0, 0, 0, 0, 0, 33, 0}, asm.build());
        }
    }

    /**
     * Validate row layout for schema of fix-len nullable key and fix-len non-null value.
     */
    @Test
    public void testFixedNullableKeyFixedValue() {
        Column[] keyCols = new Column[] {new Column("keyShortCol", SHORT, true)};
        Column[] valCols = new Column[] {new Column("valByteCol", BYTE, false)};

        SchemaDescriptor schema = new SchemaDescriptor(42, keyCols, valCols);

        {
            RowAssembler asm = new RowAssembler(schema, 0, 0, 0);

            asm.appendShort((short)-33);
            asm.appendByte((byte)71);

            assertRowBytesEquals(new byte[] {42, 0, 0, 0, 0, 0, 0, 0, 9, 0, 0, 0, 0, 0, 0, -33, -1, 8, 0, 0, 0, 0, 0, 0, 71}, asm.build());
        }

        { // Null key.
            RowAssembler asm = new RowAssembler(schema, 0, 0, 0);

            asm.appendNull();
            asm.appendByte((byte)-71);

            assertRowBytesEquals(new byte[] {42, 0, 0, 0, 0, 0, 0, 0, 7, 0, 0, 0, 1, 0, 0, 8, 0, 0, 0, 0, 0, 0, -71}, asm.build());
        }

        { // No value.
            RowAssembler asm = new RowAssembler(schema, 0, 0, 0);

            asm.appendShort((short)33);

            assertRowBytesEquals(new byte[] {42, 0, 2, 0, 0, 0, 0, 0, 9, 0, 0, 0, 0, 0, 0, 33, 0}, asm.build());
        }
    }

    /**
     * Validate row layout for schema of fix-len nullable key and fix-len nullable value.
     */
    @Test
    public void testFixedNullableKeyFixedNullableValue() {
        Column[] keyCols = new Column[] {new Column("keyShortCol", SHORT, true)};
        Column[] valCols = new Column[] {new Column("valShortCol", SHORT, true)};

        SchemaDescriptor schema = new SchemaDescriptor(42, keyCols, valCols);

        {
            RowAssembler asm = new RowAssembler(schema, 0, 0, 0);

            asm.appendShort((short)-1133);
            asm.appendShort((short)-1071);

            assertRowBytesEquals(new byte[] {42, 0, 0, 0, 0, 0, 0, 0, 9, 0, 0, 0, 0, 0, 0, -109, -5, 9, 0, 0, 0, 0, 0, 0, -47, -5}, asm.build());
        }

        { // Null key.
            RowAssembler asm = new RowAssembler(schema, 0, 0, 0);

            asm.appendNull();
            asm.appendShort((short)1171);

            assertRowBytesEquals(new byte[] {42, 0, 0, 0, 0, 0, 0, 0, 7, 0, 0, 0, 1, 0, 0, 9, 0, 0, 0, 0, 0, 0, -109, 4}, asm.build());
        }

        { // Null value.
            RowAssembler asm = new RowAssembler(schema, 0, 0, 0);

            asm.appendShort((short)1133);
            asm.appendNull();

            assertRowBytesEquals(new byte[] {42, 0, 0, 0, 0, 0, 0, 0, 9, 0, 0, 0, 0, 0, 0, 109, 4, 7, 0, 0, 0, 1, 0, 0}, asm.build());
        }

        { // Null both.
            RowAssembler asm = new RowAssembler(schema, 0, 0, 0);

            asm.appendNull();
            asm.appendNull();

            assertRowBytesEquals(new byte[] {42, 0, 0, 0, 0, 0, 0, 0, 7, 0, 0, 0, 1, 0, 0, 7, 0, 0, 0, 1, 0, 0}, asm.build());
        }

        { // No value.
            RowAssembler asm = new RowAssembler(schema, 0, 0, 0);

            asm.appendShort((short)1133);

            assertRowBytesEquals(new byte[] {42, 0, 2, 0, 0, 0, 0, 0, 9, 0, 0, 0, 0, 0, 0, 109, 4}, asm.build());
        }
    }

    /**
     * Validate row layout for schema of fix-len nullable key and var-len nullable value.
     */
    @Test
    public void testFixedNullableKeyVarlenNullableValue() {
        Column[] keyCols = new Column[] {new Column("keyIntCol", INTEGER, true)};
        Column[] valCols = new Column[] {new Column("valStrCol", STRING, true)};

        SchemaDescriptor schema = new SchemaDescriptor(42, keyCols, valCols);

        {
            RowAssembler asm = new RowAssembler(schema, 0, 0, 1);

            asm.appendInt(-33);
            asm.appendString("val");

            assertRowBytesEquals(new byte[] {42, 0, 0, 0, 0, 0, 0, 0, 11, 0, 0, 0, 0, 0, 0, -33, -1, -1, -1, 12, 0, 0, 0, 0, 1, 0, 9, 0, 118, 97, 108}, asm.build());
        }

        { // Null key.
            RowAssembler asm = new RowAssembler(schema, 0, 0, 1);

            asm.appendNull();
            asm.appendString("val");

            assertRowBytesEquals(new byte[] {42, 0, 0, 0, 0, 0, 0, 0, 7, 0, 0, 0, 1, 0, 0, 12, 0, 0, 0, 0, 1, 0, 9, 0, 118, 97, 108}, asm.build());
        }

        { // Null value.
            RowAssembler asm = new RowAssembler(schema, 0, 0, 0);

            asm.appendInt(33);
            asm.appendNull();

            assertRowBytesEquals(new byte[] {42, 0, 0, 0, 0, 0, 0, 0, 11, 0, 0, 0, 0, 0, 0, 33, 0, 0, 0, 7, 0, 0, 0, 1, 0, 0}, asm.build());
        }

        { // Null both.
            RowAssembler asm = new RowAssembler(schema, 0, 0, 0);

            asm.appendNull();
            asm.appendNull();

            assertRowBytesEquals(new byte[] {42, 0, 0, 0, 0, 0, 0, 0, 7, 0, 0, 0, 1, 0, 0, 7, 0, 0, 0, 1, 0, 0}, asm.build());
        }

        { // No value.
            RowAssembler asm = new RowAssembler(schema, 0, 0, 0);

            asm.appendInt(33);

            assertRowBytesEquals(new byte[] {42, 0, 2, 0, 0, 0, 0, 0, 11, 0, 0, 0, 0, 0, 0, 33, 0, 0, 0}, asm.build());
        }
    }

    /**
     * Validate row layout for schema of fix-len nullable key and var-len non-null value.
     */
    @Test
    public void testFixedNullableKeyVarlenValue() {
        Column[] keyCols = new Column[] {new Column("keyByteCol", BYTE, true)};
        Column[] valCols = new Column[] {new Column("valStrCol", STRING, false)};

        SchemaDescriptor schema = new SchemaDescriptor(42, keyCols, valCols);

        {
            RowAssembler asm = new RowAssembler(schema, 0, 0, 1);

            asm.appendByte((byte)-33);
            asm.appendString("val");

            assertRowBytesEquals(new byte[] {42, 0, 0, 0, 0, 0, 0, 0, 8, 0, 0, 0, 0, 0, 0, -33, 12, 0, 0, 0, 0, 1, 0, 9, 0, 118, 97, 108}, asm.build());
        }

        { // Null key.
            RowAssembler asm = new RowAssembler(schema, 0, 0, 1);

            asm.appendNull();
            asm.appendString("val");

            assertRowBytesEquals(new byte[] {42, 0, 0, 0, 0, 0, 0, 0, 7, 0, 0, 0, 1, 0, 0, 12, 0, 0, 0, 0, 1, 0, 9, 0, 118, 97, 108}, asm.build());
        }

        { // No value.
            RowAssembler asm = new RowAssembler(schema, 0, 0, 0);

            asm.appendByte((byte)33);

            assertRowBytesEquals(new byte[] {42, 0, 2, 0, 0, 0, 0, 0, 8, 0, 0, 0, 0, 0, 0, 33}, asm.build());
        }
    }

    /**
     * Validate row layout for schema of var-len non-null key and fix-len nullable value.
     */
    @Test
    public void testVarlenKeyFixedNullableValue() {
        Column[] keyCols = new Column[] {new Column("keyStrCol", STRING, false)};
        Column[] valCols = new Column[] {new Column("valUuidCol", UUID, true)};

        SchemaDescriptor schema = new SchemaDescriptor(42, keyCols, valCols);

        {
            RowAssembler asm = new RowAssembler(schema, 0, 1, 0);

            asm.appendString("key");
            asm.appendUuid(uuidVal);

            assertRowBytesEquals(new byte[] {
                42, 0, 0, 0, 0, 0, 0, 0, 12, 0, 0, 0, 0, 1, 0, 9, 0, 107, 101, 121,
                23, 0, 0, 0, 0, 0, 0, -117, -61, -31, 85, 61, -32, 57, 68, 111, 67, 56, -3, -99, -37, -58, -73}, asm.build());
        }

        { // Null value.
            RowAssembler asm = new RowAssembler(schema, 0, 1, 0);

            asm.appendString("key");
            asm.appendNull();

            assertRowBytesEquals(new byte[] {42, 0, 0, 0, 0, 0, 0, 0, 12, 0, 0, 0, 0, 1, 0, 9, 0, 107, 101, 121, 7, 0, 0, 0, 1, 0, 0}, asm.build());
        }

        { // No value.
            RowAssembler asm = new RowAssembler(schema, 0, 1, 0);

            asm.appendString("key");

            assertRowBytesEquals(new byte[] {42, 0, 2, 0, 0, 0, 0, 0, 12, 0, 0, 0, 0, 1, 0, 9, 0, 107, 101, 121}, asm.build());
        }
    }
    
    /**
     * Validate row layout for schema of var-len non-null key and fix-len non-null value.
     */
    @Test
    public void testVarlenKeyFixedValue() {
        Column[] keyCols = new Column[] {new Column("keyStrCol", STRING, false)};
        Column[] valCols = new Column[] {new Column("valUuidCol", UUID, false)};

        SchemaDescriptor schema = new SchemaDescriptor(42, keyCols, valCols);

        {
            RowAssembler asm = new RowAssembler(schema, 0, 1, 0);

            asm.appendString("key");
            asm.appendUuid(uuidVal);

            assertRowBytesEquals(new byte[] {
                42, 0, 0, 0, 0, 0, 0, 0, 12, 0, 0, 0, 0, 1, 0, 9, 0, 107, 101, 121,
                23, 0, 0, 0, 0, 0, 0, -117, -61, -31, 85, 61, -32, 57, 68, 111, 67, 56, -3, -99, -37, -58, -73}, asm.build());
        }

        { // No value.
            RowAssembler asm = new RowAssembler(schema, 0, 1, 0);

            asm.appendString("key");

            assertRowBytesEquals(new byte[] {42, 0, 2, 0, 0, 0, 0, 0, 12, 0, 0, 0, 0, 1, 0, 9, 0, 107, 101, 121}, asm.build());
        }
    }

    /**
     * Validate row layout for schema of var-len non-null key and var-len nullable value.
     */
    @Test
    public void testVarlenKeyVarlenNullableValue() {
        Column[] keyCols = new Column[] {new Column("keyStrCol", STRING, false)};
        Column[] valCols = new Column[] {new Column("valBytesCol", BYTES, true)};

        SchemaDescriptor schema = new SchemaDescriptor(42, keyCols, valCols);

        {
            RowAssembler asm = new RowAssembler(schema, 0, 1, 1);

            asm.appendString("key");
            asm.appendBytes(new byte[] {-1, 1, 0, 120});

            assertRowBytesEquals(new byte[] {42, 0, 0, 0, 0, 0, 0, 0, 12, 0, 0, 0, 0, 1, 0, 9, 0, 107, 101, 121, 13, 0, 0, 0, 0, 1, 0, 9, 0, -1, 1, 0, 120}, asm.build());
        }

        { // Null value.
            RowAssembler asm = new RowAssembler(schema, 0, 1, 0);

            asm.appendString("key");
            asm.appendNull();

            assertRowBytesEquals(new byte[] {42, 0, 0, 0, 0, 0, 0, 0, 12, 0, 0, 0, 0, 1, 0, 9, 0, 107, 101, 121, 7, 0, 0, 0, 1, 0, 0}, asm.build());
        }

        { // No value.
            RowAssembler asm = new RowAssembler(schema, 0, 1, 0);

            asm.appendString("key");

            assertRowBytesEquals(new byte[] {42, 0, 2, 0, 0, 0, 0, 0, 12, 0, 0, 0, 0, 1, 0, 9, 0, 107, 101, 121}, asm.build());
        }
    }

    /**
     * Validate row layout for schema of var-len non-null key and var-len non-null value.
     */
    @Test
    public void testVarlenKeyVarlenValue() {
        Column[] keyCols = new Column[] {new Column("keyStrCol", STRING, false)};
        Column[] valCols = new Column[] {new Column("valBytesCol", BYTES, false)};

        SchemaDescriptor schema = new SchemaDescriptor(42, keyCols, valCols);

        {
            RowAssembler asm = new RowAssembler(schema, 0, 1, 1);

            asm.appendString("key");
            asm.appendBytes(new byte[] {-1, 1, 0, 120});

            assertRowBytesEquals(new byte[] {42, 0, 0, 0, 0, 0, 0, 0, 12, 0, 0, 0, 0, 1, 0, 9, 0, 107, 101, 121, 13, 0, 0, 0, 0, 1, 0, 9, 0, -1, 1, 0, 120}, asm.build());
        }

        { // No value.
            RowAssembler asm = new RowAssembler(schema, 0, 1, 0);

            asm.appendString("key");

            assertRowBytesEquals(new byte[] {42, 0, 2, 0, 0, 0, 0, 0, 12, 0, 0, 0, 0, 1, 0, 9, 0, 107, 101, 121}, asm.build());
        }
    }

    /**
     * Validate row layout for schema of var-len nullable key and fix-len nullable value.
     */
    @Test
    public void testVarlenNullableKeyFixedNullableValue() {
        Column[] keyCols = new Column[] {new Column("keyStrCol", STRING, true)};
        Column[] valCols = new Column[] {new Column("valShortCol", SHORT, true)};

        SchemaDescriptor schema = new SchemaDescriptor(42, keyCols, valCols);

        {
            RowAssembler asm = new RowAssembler(schema, 0, 1, 0);

            asm.appendString("key");
            asm.appendShort((short)-71);

            assertRowBytesEquals(new byte[] {42, 0, 0, 0, 0, 0, 0, 0, 12, 0, 0, 0, 0, 1, 0, 9, 0, 107, 101, 121, 9, 0, 0, 0, 0, 0, 0, -71, -1}, asm.build());
        }

        { // Null key.
            RowAssembler asm = new RowAssembler(schema, 0, 0, 0);

            asm.appendNull();
            asm.appendShort((short)71);

            assertRowBytesEquals(new byte[] {42, 0, 0, 0, 0, 0, 0, 0, 7, 0, 0, 0, 1, 0, 0, 9, 0, 0, 0, 0, 0, 0, 71, 0}, asm.build());
        }

        { // Null value.
            RowAssembler asm = new RowAssembler(schema, 0, 1, 0);

            asm.appendString("key");
            asm.appendNull();

            assertRowBytesEquals(new byte[] {42, 0, 0, 0, 0, 0, 0, 0, 12, 0, 0, 0, 0, 1, 0, 9, 0, 107, 101, 121, 7, 0, 0, 0, 1, 0, 0}, asm.build());
        }

        { // Null both.
            RowAssembler asm = new RowAssembler(schema, 0, 0, 0);

            asm.appendNull();
            asm.appendNull();

            assertRowBytesEquals(new byte[] {42, 0, 0, 0, 0, 0, 0, 0, 7, 0, 0, 0, 1, 0, 0, 7, 0, 0, 0, 1, 0, 0}, asm.build());
        }

        { // No value.
            RowAssembler asm = new RowAssembler(schema, 0, 1, 0);

            asm.appendString("key");

            assertRowBytesEquals(new byte[] {42, 0, 2, 0, 0, 0, 0, 0, 12, 0, 0, 0, 0, 1, 0, 9, 0, 107, 101, 121}, asm.build());
        }
    }

    /**
     * Validate row layout for schema of var-len nullable key and fix-len non-null value.
     */
    @Test
    public void testVarlenNullableKeyFixedValue() {
        Column[] keyCols = new Column[] {new Column("keyStrCol", STRING, true)};
        Column[] valCols = new Column[] {new Column("valShortCol", SHORT, false)};

        SchemaDescriptor schema = new SchemaDescriptor(42, keyCols, valCols);

        {
            RowAssembler asm = new RowAssembler(schema, 0, 1, 0);

            asm.appendString("key");
            asm.appendShort((short)-71L);

            assertRowBytesEquals(new byte[] {42, 0, 0, 0, 0, 0, 0, 0, 12, 0, 0, 0, 0, 1, 0, 9, 0, 107, 101, 121, 9, 0, 0, 0, 0, 0, 0, -71, -1}, asm.build());
        }

        { // Null key.
            RowAssembler asm = new RowAssembler(schema, 0, 0, 0);

            asm.appendNull();
            asm.appendShort((short)71);

            assertRowBytesEquals(new byte[] {42, 0, 0, 0, 0, 0, 0, 0, 7, 0, 0, 0, 1, 0, 0, 9, 0, 0, 0, 0, 0, 0, 71, 0}, asm.build());
        }

        { // No value.
            RowAssembler asm = new RowAssembler(schema, 0, 1, 0);

            asm.appendString("key");

            assertRowBytesEquals(new byte[] {42, 0, 2, 0, 0, 0, 0, 0, 12, 0, 0, 0, 0, 1, 0, 9, 0, 107, 101, 121}, asm.build());
        }
    }

    /**
     * Validate row layout for schema of var-len nullable key and var-len nullable value.
     */
    @Test
    public void testVarlenNullableKeyVarlenNullableValue() {
        Column[] keyCols = new Column[] {new Column("keyStrCol", STRING, true)};
        Column[] valCols = new Column[] {new Column("valBytesCol", BYTES, true)};

        SchemaDescriptor schema = new SchemaDescriptor(42, keyCols, valCols);

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

            assertRowBytesEquals(new byte[] {42, 0, 0, 0, 0, 0, 0, 0, 7, 0, 0, 0, 1, 0, 0, 13, 0, 0, 0, 0, 1, 0, 9, 0, -1, 1, 0, 120}, asm.build());
        }

        { // Null value.
            RowAssembler asm = new RowAssembler(schema, 0, 1, 0);

            asm.appendString("key");
            asm.appendNull();

            assertRowBytesEquals(new byte[] {42, 0, 0, 0, 0, 0, 0, 0, 12, 0, 0, 0, 0, 1, 0, 9, 0, 107, 101, 121, 7, 0, 0, 0, 1, 0, 0}, asm.build());
        }

        { // Null both.
            RowAssembler asm = new RowAssembler(schema, 0, 0, 0);

            asm.appendNull();
            asm.appendNull();

            assertRowBytesEquals(new byte[] {42, 0, 0, 0, 0, 0, 0, 0, 7, 0, 0, 0, 1, 0, 0, 7, 0, 0, 0, 1, 0, 0}, asm.build());
        }

        { // No value.
            RowAssembler asm = new RowAssembler(schema, 0, 1, 0);

            asm.appendString("key");

            assertRowBytesEquals(new byte[] {42, 0, 2, 0, 0, 0, 0, 0, 12, 0, 0, 0, 0, 1, 0, 9, 0, 107, 101, 121}, asm.build());
        }
    }

    /**
     * Validate row layout for schema of var-len nullable key and var-len non-null value.
     */
    @Test
    public void testVarlenNullableKeyVarlenValue() {
        Column[] keyCols = new Column[] {new Column("keyStrCol", STRING, true)};
        Column[] valCols = new Column[] {new Column("valBytesCol", BYTES, false)};

        SchemaDescriptor schema = new SchemaDescriptor(42, keyCols, valCols);

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

            assertRowBytesEquals(new byte[] {42, 0, 0, 0, 0, 0, 0, 0, 7, 0, 0, 0, 1, 0, 0, 13, 0, 0, 0, 0, 1, 0, 9, 0, -1, 1, 0, 120}, asm.build());
        }

        { // No value.
            RowAssembler asm = new RowAssembler(schema, 0, 1, 0);

            asm.appendString("key");

            assertRowBytesEquals(new byte[] {42, 0, 2, 0, 0, 0, 0, 0, 12, 0, 0, 0, 0, 1, 0, 9, 0, 107, 101, 121,}, asm.build());
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
