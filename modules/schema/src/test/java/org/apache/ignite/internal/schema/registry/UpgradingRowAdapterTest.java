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

package org.apache.ignite.internal.schema.registry;

import static org.apache.ignite.internal.schema.NativeTypes.BYTES;
import static org.apache.ignite.internal.schema.NativeTypes.DATE;
import static org.apache.ignite.internal.schema.NativeTypes.DOUBLE;
import static org.apache.ignite.internal.schema.NativeTypes.FLOAT;
import static org.apache.ignite.internal.schema.NativeTypes.INT16;
import static org.apache.ignite.internal.schema.NativeTypes.INT32;
import static org.apache.ignite.internal.schema.NativeTypes.INT64;
import static org.apache.ignite.internal.schema.NativeTypes.INT8;
import static org.apache.ignite.internal.schema.NativeTypes.STRING;
import static org.apache.ignite.internal.schema.NativeTypes.datetime;
import static org.apache.ignite.internal.schema.NativeTypes.time;
import static org.apache.ignite.internal.schema.NativeTypes.timestamp;
import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.util.ArrayList;
import java.util.BitSet;
import java.util.List;
import java.util.Random;
import org.apache.ignite.internal.schema.ByteBufferRow;
import org.apache.ignite.internal.schema.Column;
import org.apache.ignite.internal.schema.NativeType;
import org.apache.ignite.internal.schema.NativeTypeSpec;
import org.apache.ignite.internal.schema.NativeTypes;
import org.apache.ignite.internal.schema.SchemaDescriptor;
import org.apache.ignite.internal.schema.TestUtils;
import org.apache.ignite.internal.schema.mapping.ColumnMapper;
import org.apache.ignite.internal.schema.row.Row;
import org.apache.ignite.internal.schema.row.RowAssembler;
import org.apache.ignite.lang.IgniteLogger;
import org.jetbrains.annotations.NotNull;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/**
 * Tests row assembling and reading.
 */
public class UpgradingRowAdapterTest {
    /** Random. */
    private Random rnd;

    /**
     * Initialization.
     */
    @BeforeEach
    public void initRandom() {
        long seed = System.currentTimeMillis();

        IgniteLogger.forClass(UpgradingRowAdapterTest.class).info("Using seed: " + seed + "L; //");

        rnd = new Random(seed);
    }

    @Test
    public void testVariousColumnTypes() {
        SchemaDescriptor schema = new SchemaDescriptor(1,
                new Column[]{new Column("keyUuidCol", NativeTypes.UUID, true)},
                new Column[]{
                        new Column("valByteCol", INT8, true),
                        new Column("valShortCol", INT16, true),
                        new Column("valIntCol", INT32, true),
                        new Column("valLongCol", INT64, true),
                        new Column("valFloatCol", FLOAT, true),
                        new Column("valDoubleCol", DOUBLE, true),
                        new Column("valDateCol", DATE, true),
                        new Column("valTimeCol", time(), true),
                        new Column("valDateTimeCol", datetime(), true),
                        new Column("valTimeStampCol", timestamp(), true),
                        new Column("valBitmask1Col", NativeTypes.bitmaskOf(22), true),
                        new Column("valBytesCol", BYTES, false),
                        new Column("valStringCol", STRING, false),
                        new Column("valNumberCol", NativeTypes.numberOf(20), false),
                        new Column("valDecimalCol", NativeTypes.decimalOf(25, 5), false),
                }
        );

        SchemaDescriptor schema2 = new SchemaDescriptor(2,
                new Column[]{new Column("keyUuidCol", NativeTypes.UUID, true)},
                new Column[]{
                        new Column("added", INT8, true),
                        new Column("valByteCol", INT8, true),
                        new Column("valShortCol", INT16, true),
                        new Column("valIntCol", INT32, true),
                        new Column("valLongCol", INT64, true),
                        new Column("valFloatCol", FLOAT, true),
                        new Column("valDoubleCol", DOUBLE, true),
                        new Column("valDateCol", DATE, true),
                        new Column("valTimeCol", time(), true),
                        new Column("valDateTimeCol", datetime(), true),
                        new Column("valTimeStampCol", timestamp(), true),
                        new Column("valBitmask1Col", NativeTypes.bitmaskOf(22), true),
                        new Column("valBytesCol", BYTES, false),
                        new Column("valStringCol", STRING, false),
                        new Column("valNumberCol", NativeTypes.numberOf(20), false),
                        new Column("valDecimalCol", NativeTypes.decimalOf(25, 5), false),
                }
        );

        int addedColumnIndex = schema2.column("added").schemaIndex();

        schema2.columnMapping(new ColumnMapper() {
            @Override
            public ColumnMapper add(@NotNull Column col) {
                return null;
            }

            @Override
            public ColumnMapper add(int from, int to) {
                return null;
            }

            @Override
            public int map(int idx) {
                return idx < addedColumnIndex ? idx : idx == addedColumnIndex ? -1 : idx - 1;
            }

            @Override
            public Column mappedColumn(int idx) {
                return idx == addedColumnIndex ? schema2.column(idx) : null;
            }
        });

        List<Object> values = generateRowValues(schema);

        ByteBufferRow row = new ByteBufferRow(serializeValuesToRow(schema, values));

        // Validate row.
        validateRow(values, new SchemaRegistryImpl(1, v -> v == 1 ? schema : schema2), row);

        // Validate upgraded row.
        values.add(addedColumnIndex, null);

        validateRow(values, new SchemaRegistryImpl(2, v -> v == 1 ? schema : schema2), row);
    }

    private void validateRow(List<Object> values, SchemaRegistryImpl schemaRegistry, ByteBufferRow binaryRow) {
        Row row = schemaRegistry.resolve(binaryRow);

        SchemaDescriptor schema = row.schema();

        for (int i = 0; i < values.size(); i++) {
            Column col = schema.column(i);

            NativeTypeSpec type = col.type().spec();

            if (type == NativeTypeSpec.BYTES) {
                assertArrayEquals((byte[]) values.get(i), (byte[]) NativeTypeSpec.BYTES.objectValue(row, col.schemaIndex()),
                        "Failed for column: " + col);
            } else {
                assertEquals(values.get(i), type.objectValue(row, col.schemaIndex()), "Failed for column: " + col);
            }
        }
    }

    /**
     * Generate row values for given row schema.
     *
     * @param schema Row schema.
     * @return Row values.
     */
    private List<Object> generateRowValues(SchemaDescriptor schema) {
        ArrayList<Object> res = new ArrayList<>(schema.length());

        for (int i = 0; i < schema.length(); i++) {
            NativeType type = schema.column(i).type();

            res.add(TestUtils.generateRandomValue(rnd, type));
        }

        return res;
    }

    /**
     * Validates row values after serialization-then-deserialization.
     *
     * @param schema Row schema.
     * @param vals   Row values.
     * @return Row bytes.
     */
    private byte[] serializeValuesToRow(SchemaDescriptor schema, List<Object> vals) {
        assertEquals(schema.keyColumns().length() + schema.valueColumns().length(), vals.size());

        int nonNullVarLenKeyCols = 0;
        int nonNullVarLenValCols = 0;
        int nonNullVarLenKeySize = 0;
        int nonNullVarLenValSize = 0;

        for (int i = 0; i < vals.size(); i++) {
            NativeTypeSpec type = schema.column(i).type().spec();

            if (vals.get(i) != null && !type.fixedLength()) {
                if (type == NativeTypeSpec.BYTES) {
                    byte[] val = (byte[]) vals.get(i);
                    if (schema.isKeyColumn(i)) {
                        nonNullVarLenKeyCols++;
                        nonNullVarLenKeySize += val.length;
                    } else {
                        nonNullVarLenValCols++;
                        nonNullVarLenValSize += val.length;
                    }
                } else if (type == NativeTypeSpec.STRING) {
                    if (schema.isKeyColumn(i)) {
                        nonNullVarLenKeyCols++;
                        nonNullVarLenKeySize += RowAssembler.utf8EncodedLength((CharSequence) vals.get(i));
                    } else {
                        nonNullVarLenValCols++;
                        nonNullVarLenValSize += RowAssembler.utf8EncodedLength((CharSequence) vals.get(i));
                    }
                } else if (type == NativeTypeSpec.NUMBER) {
                    if (schema.isKeyColumn(i)) {
                        nonNullVarLenKeyCols++;
                        nonNullVarLenKeySize += RowAssembler.sizeInBytes((BigInteger) vals.get(i));
                    } else {
                        nonNullVarLenValCols++;
                        nonNullVarLenValSize += RowAssembler.sizeInBytes((BigInteger) vals.get(i));
                    }
                } else if (type == NativeTypeSpec.DECIMAL) {
                    if (schema.isKeyColumn(i)) {
                        nonNullVarLenKeyCols++;
                        nonNullVarLenKeySize += RowAssembler.sizeInBytes((BigDecimal) vals.get(i));
                    } else {
                        nonNullVarLenValCols++;
                        nonNullVarLenValSize += RowAssembler.sizeInBytes((BigDecimal) vals.get(i));
                    }
                } else {
                    throw new IllegalStateException("Unsupported variable-length type: " + type);
                }
            }
        }

        RowAssembler asm = new RowAssembler(
                schema,
                nonNullVarLenKeySize,
                nonNullVarLenKeyCols,
                nonNullVarLenValSize,
                nonNullVarLenValCols);

        for (int i = 0; i < vals.size(); i++) {
            if (vals.get(i) == null) {
                asm.appendNull();
            } else {
                NativeType type = schema.column(i).type();

                switch (type.spec()) {
                    case INT8:
                        asm.appendByte((Byte) vals.get(i));
                        break;

                    case INT16:
                        asm.appendShort((Short) vals.get(i));
                        break;

                    case INT32:
                        asm.appendInt((Integer) vals.get(i));
                        break;

                    case INT64:
                        asm.appendLong((Long) vals.get(i));
                        break;

                    case FLOAT:
                        asm.appendFloat((Float) vals.get(i));
                        break;

                    case DOUBLE:
                        asm.appendDouble((Double) vals.get(i));
                        break;

                    case UUID:
                        asm.appendUuid((java.util.UUID) vals.get(i));
                        break;

                    case STRING:
                        asm.appendString((String) vals.get(i));
                        break;

                    case NUMBER:
                        asm.appendNumber((BigInteger) vals.get(i));
                        break;

                    case DECIMAL:
                        asm.appendDecimal((BigDecimal) vals.get(i));
                        break;

                    case BYTES:
                        asm.appendBytes((byte[]) vals.get(i));
                        break;

                    case BITMASK:
                        asm.appendBitmask((BitSet) vals.get(i));
                        break;

                    case DATE:
                        asm.appendDate((LocalDate) vals.get(i));
                        break;

                    case TIME:
                        asm.appendTime((LocalTime) vals.get(i));
                        break;

                    case DATETIME:
                        asm.appendDateTime((LocalDateTime) vals.get(i));
                        break;

                    case TIMESTAMP:
                        asm.appendTimestamp((Instant) vals.get(i));
                        break;

                    default:
                        throw new IllegalStateException("Unsupported test type: " + type);
                }
            }
        }

        return asm.toBytes();
    }
}
