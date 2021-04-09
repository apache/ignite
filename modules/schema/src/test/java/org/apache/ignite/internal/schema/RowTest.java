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
import java.util.BitSet;
import java.util.Collections;
import java.util.Random;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import static org.apache.ignite.internal.schema.NativeType.BYTE;
import static org.apache.ignite.internal.schema.NativeType.BYTES;
import static org.apache.ignite.internal.schema.NativeType.DOUBLE;
import static org.apache.ignite.internal.schema.NativeType.FLOAT;
import static org.apache.ignite.internal.schema.NativeType.INTEGER;
import static org.apache.ignite.internal.schema.NativeType.LONG;
import static org.apache.ignite.internal.schema.NativeType.SHORT;
import static org.apache.ignite.internal.schema.NativeType.STRING;
import static org.apache.ignite.internal.schema.NativeType.UUID;
import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;

/**
 * Tests row assembling and reading.
 */
public class RowTest {
    /** */
    private Random rnd;

    /**
     *
     */
    @BeforeEach
    public void initRandom() {
        long seed = System.currentTimeMillis();

        System.out.println("Using seed: " + seed + "L; //");

        rnd = new Random(seed);
    }

    /**
     *
     */
    @Test
    public void testFixedSizes() {
        Column[] keyCols = new Column[] {
            new Column("keyByteCol", BYTE, true),
            new Column("keyShortCol", SHORT, true),
            new Column("keyIntCol", INTEGER, true),
            new Column("keyLongCol", LONG, true),
            new Column("keyFloatCol", FLOAT, true),
            new Column("keyDoubleCol", DOUBLE, true),
            new Column("keyUuidCol", UUID, true),
            new Column("keyBitmask1Col", Bitmask.of(4), true),
            new Column("keyBitmask2Col", Bitmask.of(22), true)
        };

        Column[] valCols = new Column[] {
            new Column("valByteCol", BYTE, true),
            new Column("valShortCol", SHORT, true),
            new Column("valIntCol", INTEGER, true),
            new Column("valLongCol", LONG, true),
            new Column("valFloatCol", FLOAT, true),
            new Column("valDoubleCol", DOUBLE, true),
            new Column("valUuidCol", UUID, true),
            new Column("valBitmask1Col", Bitmask.of(4), true),
            new Column("valBitmask2Col", Bitmask.of(22), true)
        };

        checkSchema(keyCols, valCols);
    }

    /**
     *
     */
    @Test
    public void testVariableSizes() {
        Column[] keyCols = new Column[] {
            new Column("keyByteCol", BYTE, true),
            new Column("keyShortCol", SHORT, true),
            new Column("keyIntCol", INTEGER, true),
            new Column("keyLongCol", LONG, true),
            new Column("keyBytesCol", BYTES, true),
            new Column("keyStringCol", STRING, true),
        };

        Column[] valCols = new Column[] {
            new Column("keyByteCol", BYTE, true),
            new Column("keyShortCol", SHORT, true),
            new Column("keyIntCol", INTEGER, true),
            new Column("keyLongCol", LONG, true),
            new Column("valBytesCol", BYTES, true),
            new Column("valStringCol", STRING, true),
        };

        checkSchema(keyCols, valCols);
    }

    /**
     *
     */
    @Test
    public void testMixedSizes() {
        Column[] keyCols = new Column[] {
            new Column("keyBytesCol", BYTES, true),
            new Column("keyStringCol", STRING, true),
        };

        Column[] valCols = new Column[] {
            new Column("valBytesCol", BYTES, true),
            new Column("valStringCol", STRING, true),
        };

        checkSchema(keyCols, valCols);
    }

    /**
     *
     */
    private void checkSchema(Column[] keyCols, Column[] valCols) {
        checkSchemaShuffled(keyCols, valCols);

        shuffle(keyCols);
        shuffle(valCols);

        checkSchemaShuffled(keyCols, valCols);
    }

    /**
     *
     */
    private void checkSchemaShuffled(Column[] keyCols, Column[] valCols) {
        SchemaDescriptor sch = new SchemaDescriptor(1, keyCols, valCols);

        Object[] checkArr = sequence(sch);

        checkValues(sch, checkArr);

        while (checkArr[0] != null) {
            int idx = 0;

            Object prev = checkArr[idx];
            checkArr[idx] = null;

            checkValues(sch, checkArr);

            while (idx < checkArr.length - 1 && checkArr[idx + 1] != null) {
                checkArr[idx] = prev;
                prev = checkArr[idx + 1];
                checkArr[idx + 1] = null;
                idx++;

                checkValues(sch, checkArr);
            }
        }
    }

    /**
     *
     */
    private Object[] sequence(SchemaDescriptor schema) {
        Object[] res = new Object[schema.length()];

        for (int i = 0; i < res.length; i++) {
            NativeType type = schema.column(i).type();

            res[i] = generateRandomValue(type);
        }

        return res;
    }

    /**
     *
     */
    private Object generateRandomValue(NativeType type) {
        return TestUtils.generateRandomValue(rnd, type);
    }

    /**
     *
     */
    private void checkValues(SchemaDescriptor schema, Object... vals) {
        assertEquals(schema.keyColumns().length() + schema.valueColumns().length(), vals.length);

        int nonNullVarLenKeyCols = 0;
        int nonNullVarLenValCols = 0;
        int nonNullVarLenKeySize = 0;
        int nonNullVarLenValSize = 0;

        for (int i = 0; i < vals.length; i++) {
            NativeTypeSpec type = schema.column(i).type().spec();

            if (vals[i] != null && !type.fixedLength()) {
                if (type == NativeTypeSpec.BYTES) {
                    byte[] val = (byte[])vals[i];
                    if (schema.isKeyColumn(i)) {
                        nonNullVarLenKeyCols++;
                        nonNullVarLenKeySize += val.length;
                    }
                    else {
                        nonNullVarLenValCols++;
                        nonNullVarLenValSize += val.length;
                    }
                }
                else if (type == NativeTypeSpec.STRING) {
                    if (schema.isKeyColumn(i)) {
                        nonNullVarLenKeyCols++;
                        nonNullVarLenKeySize += RowAssembler.utf8EncodedLength((CharSequence)vals[i]);
                    }
                    else {
                        nonNullVarLenValCols++;
                        nonNullVarLenValSize += RowAssembler.utf8EncodedLength((CharSequence)vals[i]);
                    }
                }
                else
                    throw new IllegalStateException("Unsupported variable-length type: " + type);
            }
        }

        int size = RowAssembler.rowChunkSize(
            schema.keyColumns(), nonNullVarLenKeyCols, nonNullVarLenKeySize,
            schema.valueColumns(), nonNullVarLenValCols, nonNullVarLenValSize);

        RowAssembler asm = new RowAssembler(schema, size, nonNullVarLenKeyCols, nonNullVarLenValCols);

        for (int i = 0; i < vals.length; i++) {
            if (vals[i] == null)
                asm.appendNull();
            else {
                NativeType type = schema.column(i).type();

                switch (type.spec()) {
                    case BYTE:
                        asm.appendByte((Byte)vals[i]);
                        break;

                    case SHORT:
                        asm.appendShort((Short)vals[i]);
                        break;

                    case INTEGER:
                        asm.appendInt((Integer)vals[i]);
                        break;

                    case LONG:
                        asm.appendLong((Long)vals[i]);
                        break;

                    case FLOAT:
                        asm.appendFloat((Float)vals[i]);
                        break;

                    case DOUBLE:
                        asm.appendDouble((Double)vals[i]);
                        break;

                    case UUID:
                        asm.appendUuid((java.util.UUID)vals[i]);
                        break;

                    case STRING:
                        asm.appendString((String)vals[i]);
                        break;

                    case BYTES:
                        asm.appendBytes((byte[])vals[i]);
                        break;

                    case BITMASK:
                        asm.appendBitmask((BitSet)vals[i]);
                        break;

                    default:
                        throw new IllegalStateException("Unsupported test type: " + type);
                }
            }
        }

        byte[] data = asm.build();

        Row tup = new Row(schema, new ByteBufferRow(data));

        for (int i = 0; i < vals.length; i++) {
            Column col = schema.column(i);

            NativeTypeSpec type = col.type().spec();

            if (type == NativeTypeSpec.BYTES)
                assertArrayEquals((byte[])vals[i], (byte[])NativeTypeSpec.BYTES.objectValue(tup, i),
                    "Failed for column: " + col);
            else
                assertEquals(vals[i], type.objectValue(tup, i), "Failed for column: " + col);
        }
    }

    /**
     *
     */
    private void shuffle(Column[] cols) {
        Collections.shuffle(Arrays.asList(cols));
    }
}
