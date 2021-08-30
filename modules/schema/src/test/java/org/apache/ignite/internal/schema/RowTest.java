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

import java.math.BigDecimal;
import java.math.BigInteger;
import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.util.Arrays;
import java.util.BitSet;
import java.util.Collections;
import java.util.Random;
import java.util.function.Function;
import java.util.stream.IntStream;
import org.apache.ignite.internal.schema.row.Row;
import org.apache.ignite.internal.schema.row.RowAssembler;
import org.apache.ignite.internal.util.Constants;
import org.apache.ignite.lang.IgniteLogger;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import static org.apache.ignite.internal.schema.NativeTypes.BYTES;
import static org.apache.ignite.internal.schema.NativeTypes.DATE;
import static org.apache.ignite.internal.schema.NativeTypes.DOUBLE;
import static org.apache.ignite.internal.schema.NativeTypes.FLOAT;
import static org.apache.ignite.internal.schema.NativeTypes.INT16;
import static org.apache.ignite.internal.schema.NativeTypes.INT32;
import static org.apache.ignite.internal.schema.NativeTypes.INT64;
import static org.apache.ignite.internal.schema.NativeTypes.INT8;
import static org.apache.ignite.internal.schema.NativeTypes.STRING;
import static org.apache.ignite.internal.schema.NativeTypes.UUID;
import static org.apache.ignite.internal.schema.NativeTypes.datetime;
import static org.apache.ignite.internal.schema.NativeTypes.time;
import static org.apache.ignite.internal.schema.NativeTypes.timestamp;
import static org.apache.ignite.internal.testframework.IgniteTestUtils.randomBytes;
import static org.apache.ignite.internal.testframework.IgniteTestUtils.randomString;
import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;

/**
 * Tests row assembling and reading.
 */
public class RowTest {
    /** Random. */
    private Random rnd;

    /**
     * Initialization.
     */
    @BeforeEach
    public void initRandom() {
        long seed = System.currentTimeMillis();

        IgniteLogger.forClass(RowTest.class).info("Using seed: " + seed + "L; //");

        rnd = new Random(seed);
    }

    /**
     * Check row serialization for schema with nullable fix-sized columns only.
     */
    @Test
    public void nullableFixSizedColumns() {
        Column[] keyCols = new Column[]{
            new Column("keyByteCol", INT8, false),
            new Column("keyShortCol", INT16, false),
            new Column("keyIntCol", INT32, false),
            new Column("keyLongCol", INT64, false),
            new Column("keyFloatCol", FLOAT, false),
            new Column("keyDoubleCol", DOUBLE, false),
            new Column("keyUuidCol", UUID, false),
            new Column("keyDateCol", DATE, false),
            new Column("keyTimeCol", time(), false),
            new Column("keyDateTimeCol", datetime(), false),
            new Column("keyTimeStampCol", timestamp(), false),
            new Column("keyBitmask1Col", NativeTypes.bitmaskOf(4), false),
            new Column("keyBitmask2Col", NativeTypes.bitmaskOf(22), false)
        };

        Column[] valCols = new Column[]{
            new Column("valByteCol", INT8, false),
            new Column("valShortCol", INT16, false),
            new Column("valIntCol", INT32, false),
            new Column("valLongCol", INT64, false),
            new Column("valFloatCol", FLOAT, false),
            new Column("valDoubleCol", DOUBLE, false),
            new Column("valUuidCol", UUID, false),
            new Column("valDateCol", DATE, false),
            new Column("valTimeCol", time(), false),
            new Column("valDateTimeCol", datetime(), false),
            new Column("valTimeStampCol", timestamp(), false),
            new Column("valBitmask1Col", NativeTypes.bitmaskOf(4), false),
            new Column("valBitmask2Col", NativeTypes.bitmaskOf(22), false)
        };

        checkSchema(keyCols, valCols);
    }

    /**
     * Check row serialization for schema with non-nullable fix-sized columns only.
     */
    @Test
    public void fixSizedColumns() {
        Column[] keyCols = new Column[]{
            new Column("keyByteCol", INT8, true),
            new Column("keyShortCol", INT16, true),
            new Column("keyIntCol", INT32, true),
            new Column("keyLongCol", INT64, true),
            new Column("keyFloatCol", FLOAT, true),
            new Column("keyDoubleCol", DOUBLE, true),
            new Column("keyUuidCol", UUID, true),
            new Column("keyDateCol", DATE, true),
            new Column("keyTimeCol", time(), true),
            new Column("keyDateTimeCol", datetime(), true),
            new Column("keyTimeStampCol", timestamp(), true),
            new Column("keyBitmask1Col", NativeTypes.bitmaskOf(4), true),
            new Column("keyBitmask2Col", NativeTypes.bitmaskOf(22), true),
        };

        Column[] valCols = new Column[]{
            new Column("valByteCol", INT8, true),
            new Column("valShortCol", INT16, true),
            new Column("valIntCol", INT32, true),
            new Column("valLongCol", INT64, true),
            new Column("valFloatCol", FLOAT, true),
            new Column("valDoubleCol", DOUBLE, true),
            new Column("valUuidCol", UUID, true),
            new Column("valDateCol", DATE, true),
            new Column("valTimeCol", time(), true),
            new Column("valDateTimeCol", datetime(), true),
            new Column("valTimeStampCol", timestamp(), true),
            new Column("valBitmask1Col", NativeTypes.bitmaskOf(4), true),
            new Column("valBitmask2Col", NativeTypes.bitmaskOf(22), true),
        };

        checkSchema(keyCols, valCols);
    }

    /**
     * Check row serialization for schema with various columns.
     */
    @Test
    public void mixedColumns() {
        Column[] keyCols = new Column[]{
            new Column("keyByteCol", INT8, false),
            new Column("keyShortCol", INT16, false),
            new Column("keyIntCol", INT32, false),
            new Column("keyLongCol", INT64, false),
            new Column("keyDateTimeCol", datetime(), false),
            new Column("keyBytesCol", BYTES, false),
            new Column("keyStringCol", STRING, false),
            new Column("keyNumberCol", NativeTypes.numberOf(9), false),
            new Column("keyDecimalCol", NativeTypes.decimalOf(20, 3), false),
        };

        Column[] valCols = new Column[]{
            new Column("valByteCol", INT8, true),
            new Column("valShortCol", INT16, true),
            new Column("valIntCol", INT32, true),
            new Column("valLongCol", INT64, true),
            new Column("valDateTimeCol", datetime(), true),
            new Column("valBytesCol", BYTES, true),
            new Column("valStringCol", STRING, true),
            new Column("valNumberCol", NativeTypes.numberOf(9), true),
            new Column("valDecimalCol", NativeTypes.decimalOf(20, 3), true),
        };

        checkSchema(keyCols, valCols);
    }

    /**
     * Check row serialization for schema with various columns.
     */
    @Test
    public void temporalColumns() {
        Column[] keyCols = new Column[]{
            new Column("keyTimestampCol1", timestamp(0), false),
            new Column("keyTimestampCol2", timestamp(1), false),
            new Column("keyTimestampCol3", timestamp(4), false),
            new Column("keyTimestampCol4", timestamp(9), false),
        };

        Column[] valCols = new Column[]{
            new Column("valDateTimeCol1", datetime(0), false),
            new Column("valDateTimeCol2", datetime(1), true),
            new Column("valDateTimeCol3", datetime(4), false),
            new Column("valDateTimeCol4", datetime(9), true),
            new Column("valTimeCol1", time(0), true),
            new Column("valTimeCol2", time(1), true),
            new Column("valTimeCol3", time(4), false),
            new Column("valTimeCol4", time(9), false),
        };

        checkSchema(keyCols, valCols);
    }

    /**
     * Check row serialization for schema with non-nullable varlen columns only.
     */
    @Test
    public void varlenColumns() {
        Column[] keyCols = new Column[]{
            new Column("keyBytesCol", BYTES, false),
            new Column("keyStringCol", STRING, false),
            new Column("keyNumberCol", NativeTypes.numberOf(9), false),
            new Column("keyDecimalCol", NativeTypes.decimalOf(20, 3), false),
        };

        Column[] valCols = new Column[]{
            new Column("valBytesCol", BYTES, false),
            new Column("valStringCol", STRING, false),
            new Column("valNumberCol", NativeTypes.numberOf(9), false),
            new Column("valDecimalCol", NativeTypes.decimalOf(20, 3), false),
        };

        checkSchema(keyCols, valCols);
    }

    /**
     * Check row serialization for schema with nullable varlen columns only.
     */
    @Test
    public void nullableVarlenColumns() {
        Column[] keyCols = new Column[]{
            new Column("keyBytesCol", BYTES, true),
            new Column("keyStringCol", STRING, true),
        };

        Column[] valCols = new Column[]{
            new Column("valBytesCol", BYTES, true),
            new Column("valStringCol", STRING, true),
            new Column("valNumberCol", NativeTypes.numberOf(9), true),
            new Column("valDecimalCol", NativeTypes.decimalOf(20, 3), true),
        };

        checkSchema(keyCols, valCols);
    }

    /**
     * Check row serialization for schema with large varlen columns (64Kb+).
     */
    @Test
    public void largeVarlenColumns() {
        Column[] keyCols = new Column[] {
            new Column("keyBytesCol", BYTES, false),
            new Column("keyStringCol", STRING, false),
        };

        Column[] valCols = new Column[] {
            new Column("valBytesCol", BYTES, true),
            new Column("valStringCol", STRING, true),
        };

        SchemaDescriptor sch = new SchemaDescriptor(java.util.UUID.randomUUID(), 1, keyCols, valCols);

        Object[] checkArr = generateRowValues(sch, t -> (t.spec() == NativeTypeSpec.BYTES) ?
            randomBytes(rnd, rnd.nextInt(Constants.MiB) + (2 << 16)) :
            randomString(rnd, rnd.nextInt(Constants.MiB) + (2 << 16)));

        checkValues(sch, checkArr);

        for (int idx = 0; idx < checkArr.length; idx++) {
            if (!sch.column(idx).nullable())
                continue;

            Object prev = checkArr[idx];
            checkArr[idx] = null;

            checkValues(sch, checkArr);

            checkArr[idx] = prev;
        }
    }

    /**
     * Check row serialization for 256+ fixsized columns.
     */
    @Test
    public void mediumLenWithFixSizedColumns() {
        Column[] keyCols = IntStream.range(0, 300)
            .mapToObj(i -> new Column("keyCol" + i, INT8, false))
            .toArray(Column[]::new);
        Column[] valCols = IntStream.range(0, 330)
            .mapToObj(i -> new Column("valCol" + i, INT8, true))
            .toArray(Column[]::new);

        SchemaDescriptor sch = new SchemaDescriptor(java.util.UUID.randomUUID(), 1, keyCols, valCols);

        Object[] checkArr = generateRowValues(sch);

        checkValues(sch, checkArr);
    }

    /**
     * Check row serialization for 256+ varlen columns.
     */
    @Test
    public void mediumLenWithVarlenColumns() {
        Column[] keyCols = IntStream.range(0, 300)
            .mapToObj(i -> new Column("keyCol" + i, STRING, false))
            .toArray(Column[]::new);
        Column[] valCols = IntStream.range(0, 300)
            .mapToObj(i -> new Column("valCol" + i, STRING, true))
            .toArray(Column[]::new);

        SchemaDescriptor sch = new SchemaDescriptor(java.util.UUID.randomUUID(), 1, keyCols, valCols);

        Object[] checkArr = generateRowValues(sch, t -> randomString(rnd, rnd.nextInt(5)));

        checkValues(sch, checkArr);
    }

    /**
     * Check row serialization for 64K+ fixlen columns.
     */
    @Test
    public void largeLenWithFixSizedColumns() {
        Column[] keyCols = IntStream.range(0, (2 << 16) + rnd.nextInt(20))
            .mapToObj(i -> new Column("keyCol" + i, INT8, false))
            .toArray(Column[]::new);
        Column[] valCols = IntStream.range(0, (2 << 16) + rnd.nextInt(20))
            .mapToObj(i -> new Column("valCol" + i, INT8, true))
            .toArray(Column[]::new);

        SchemaDescriptor sch = new SchemaDescriptor(java.util.UUID.randomUUID(), 1, keyCols, valCols);

        Object[] checkArr = generateRowValues(sch);

        checkValues(sch, checkArr);
    }

    /**
     * Check row serialization for 1K+ varlen columns with total chunk length of 64k+.
     */
    @Test
    public void largeLenWithVarlenColumns() {
        Column[] keyCols = IntStream.range(0, 1000 + rnd.nextInt(20))
            .mapToObj(i -> new Column("keyCol" + i, STRING, false))
            .toArray(Column[]::new);

        Column[] valCols = IntStream.range(0, 1000 + rnd.nextInt(20))
            .mapToObj(i -> new Column("valCol" + i, STRING, true))
            .toArray(Column[]::new);

        SchemaDescriptor sch = new SchemaDescriptor(java.util.UUID.randomUUID(), 1, keyCols, valCols);

        Object[] checkArr = generateRowValues(sch, t -> randomString(rnd, 65 + rnd.nextInt(500)));

        checkValues(sch, checkArr);
    }

    /**
     * Checks schema is independent from prodived column order.
     *
     * @param keyCols Key columns.
     * @param valCols Value columns.
     */
    private void checkSchema(Column[] keyCols, Column[] valCols) {
        checkSchemaShuffled(keyCols, valCols);

        shuffle(keyCols);
        shuffle(valCols);

        checkSchemaShuffled(keyCols, valCols);
    }

    /**
     * Checks schema for given columns.
     *
     * @param keyCols Key columns.
     * @param valCols Value columns.
     */
    private void checkSchemaShuffled(Column[] keyCols, Column[] valCols) {
        SchemaDescriptor sch = new SchemaDescriptor(java.util.UUID.randomUUID(), 1, keyCols, valCols);

        Object[] checkArr = generateRowValues(sch);

        checkValues(sch, checkArr);

        for (int idx = 0; idx < checkArr.length; idx++) {
            if (!sch.column(idx).nullable())
                continue;

            Object prev = checkArr[idx];
            checkArr[idx] = null;

            checkValues(sch, checkArr);

            checkArr[idx] = prev;
        }
    }

    /**
     * Generate row values for given row schema.
     *
     * @param schema Row schema.
     * @return Row values.
     */
    private Object[] generateRowValues(SchemaDescriptor schema) {
        return generateRowValues(schema, this::generateRandomValue);
    }

    /**
     * Generate row values for given row schema.
     *
     * @param schema Row schema.
     * @param rnd Function that returns random value for the type.
     * @return Row values.
     */
    private Object[] generateRowValues(SchemaDescriptor schema, Function<NativeType, Object> rnd) {
        Object[] res = new Object[schema.length()];

        for (int i = 0; i < res.length; i++) {
            NativeType type = schema.column(i).type();

            res[i] = rnd.apply(type);
        }

        return res;
    }

    /**
     * Generates random value of a given type.
     *
     * @param type Value type.
     * @return Random value of requested type.
     */
    private Object generateRandomValue(NativeType type) {
        return TestUtils.generateRandomValue(rnd, type);
    }

    /**
     * Validates row values after serialization-then-deserialization.
     *
     * @param schema Row schema.
     * @param vals Row values.
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
                else if (type == NativeTypeSpec.NUMBER) {
                    if (schema.isKeyColumn(i)) {
                        nonNullVarLenKeyCols++;
                        nonNullVarLenKeySize += RowAssembler.sizeInBytes((BigInteger)vals[i]);
                    }
                    else {
                        nonNullVarLenValCols++;
                        nonNullVarLenValSize += RowAssembler.sizeInBytes((BigInteger)vals[i]);
                    }
                }
                else if (type == NativeTypeSpec.DECIMAL) {
                    if (schema.isKeyColumn(i)) {
                        nonNullVarLenKeyCols++;
                        nonNullVarLenKeySize += RowAssembler.sizeInBytes((BigDecimal)vals[i]);
                    }
                    else {
                        nonNullVarLenValCols++;
                        nonNullVarLenValSize += RowAssembler.sizeInBytes((BigDecimal)vals[i]);
                    }
                }
                else
                    throw new IllegalStateException("Unsupported variable-length type: " + type);
            }
        }

        RowAssembler asm = new RowAssembler(
            schema,
            nonNullVarLenKeySize,
            nonNullVarLenKeyCols,
            nonNullVarLenValSize,
            nonNullVarLenValCols);

        for (int i = 0; i < vals.length; i++) {
            if (vals[i] == null)
                asm.appendNull();
            else {
                NativeType type = schema.column(i).type();

                switch (type.spec()) {
                    case INT8:
                        asm.appendByte((Byte)vals[i]);
                        break;

                    case INT16:
                        asm.appendShort((Short)vals[i]);
                        break;

                    case INT32:
                        asm.appendInt((Integer)vals[i]);
                        break;

                    case INT64:
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

                    case NUMBER:
                        asm.appendNumber((BigInteger)vals[i]);
                        break;

                    case DECIMAL:
                        asm.appendDecimal((BigDecimal)vals[i]);
                        break;

                    case BYTES:
                        asm.appendBytes((byte[])vals[i]);
                        break;

                    case BITMASK:
                        asm.appendBitmask((BitSet)vals[i]);
                        break;

                    case DATE:
                        asm.appendDate((LocalDate)vals[i]);
                        break;

                    case TIME:
                        asm.appendTime((LocalTime)vals[i]);
                        break;

                    case DATETIME:
                        asm.appendDateTime((LocalDateTime)vals[i]);
                        break;

                    case TIMESTAMP:
                        asm.appendTimestamp((Instant)vals[i]);
                        break;

                    default:
                        throw new IllegalStateException("Unsupported test type: " + type);
                }
            }
        }

        byte[] data = asm.toBytes();

        Row row = new Row(schema, new ByteBufferRow(data));

        for (int i = 0; i < vals.length; i++) {
            Column col = schema.column(i);

            NativeTypeSpec type = col.type().spec();

            if (type == NativeTypeSpec.BYTES)
                assertArrayEquals((byte[])vals[i], (byte[])NativeTypeSpec.BYTES.objectValue(row, i),
                    "Failed for column: " + col);
            else
                assertEquals(vals[i], type.objectValue(row, i), "Failed for column: " + col);
        }
    }

    /**
     * Shuffle columns.
     */
    private void shuffle(Column[] cols) {
        Collections.shuffle(Arrays.asList(cols));
    }
}
