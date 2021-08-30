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

package org.apache.ignite.internal.table.type;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.math.RoundingMode;
import java.util.Arrays;
import java.util.List;
import java.util.Random;
import java.util.UUID;
import org.apache.ignite.internal.schema.Column;
import org.apache.ignite.internal.schema.InvalidTypeException;
import org.apache.ignite.internal.schema.NativeTypes;
import org.apache.ignite.internal.schema.SchemaDescriptor;
import org.apache.ignite.internal.schema.marshaller.TupleMarshaller;
import org.apache.ignite.internal.schema.row.Row;
import org.apache.ignite.internal.table.InternalTable;
import org.apache.ignite.internal.table.TupleMarshallerImpl;
import org.apache.ignite.internal.table.impl.DummySchemaManagerImpl;
import org.apache.ignite.internal.util.Pair;
import org.apache.ignite.schema.SchemaMode;
import org.apache.ignite.table.Tuple;
import org.jetbrains.annotations.NotNull;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;
import org.mockito.Mockito;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

/**
 * Check numeric typed columns serialization.
 */
public class NumericTypesSerializerTest {
    /** Random. */
    private Random rnd = new Random();

    /** Schema descriptor. */
    private SchemaDescriptor schema;

    /** Mocked table. */
    private InternalTable tbl = Mockito.when(Mockito.mock(InternalTable.class).schemaMode()).thenReturn(SchemaMode.STRICT_SCHEMA).getMock();

    /**
     * @return List of BigInteger pairs for test.
     */
    private static List<Pair<BigInteger, BigInteger>> numbers() {
        return Arrays.asList(
            new Pair<>(BigInteger.valueOf(10L), BigInteger.valueOf(10)),
            new Pair<>(BigInteger.valueOf(-10L), BigInteger.valueOf(-10)),
            new Pair<>(new BigInteger("10"), BigInteger.valueOf(10)),
            new Pair<>(new BigInteger("1000").divide(BigInteger.TEN), BigInteger.valueOf(10).multiply(BigInteger.TEN)),
            new Pair<>(new BigInteger("999999999"), BigInteger.valueOf(999999999L)),
            new Pair<>(new BigInteger("+999999999"), BigInteger.valueOf(999999999L)),
            new Pair<>(new BigInteger("-999999999"), BigInteger.valueOf(-999999999L))
        );
    }

    /**
     * @return List of string decimal representations for test.
     */
    private static String[] stringDecimalRepresentation() {
        return new String[] {
            "0", "0.00", "123", "-123", "1.23E3", "1.23E+3", "12.3E+7", "12.0", "12.3", "0.00123",
            "-1.23E-12", "1234.5E-4", "0E+7", "-0", "123456789.0123", "123456789.1", "123456789.112312315413",
            "123456789.0123", "123.123456789", "123456789.3210"};
    }

    /**
     * @return List of pairs to compare byte representation.
     */
    private static List<Pair<BigDecimal, BigDecimal>> sameDecimals() {
        return Arrays.asList(
            new Pair<>(new BigDecimal("10"), BigDecimal.valueOf(10)),
            new Pair<>(new BigDecimal("10.00"), BigDecimal.valueOf(10)),
            new Pair<>(new BigDecimal("999999999"), BigDecimal.valueOf(999999999L)),
            new Pair<>(new BigDecimal("-999999999"), BigDecimal.valueOf(-999999999L)),
            new Pair<>(new BigDecimal("1E3"), BigDecimal.valueOf(1000)),
            new Pair<>(new BigDecimal("1E-3"), new BigDecimal("0.001")),
            new Pair<>(new BigDecimal("0E-3"), new BigDecimal("0.00000")),
            new Pair<>(new BigDecimal("0E-3"), new BigDecimal("0E+3")),
            new Pair<>(new BigDecimal("123.3211"), new BigDecimal("123.321"))
        );
    }

    @BeforeEach
    public void setup() {
        long seed = System.currentTimeMillis();

        rnd = new Random(seed);
    }

    /**
     *
     */
    @ParameterizedTest
    @MethodSource("numbers")
    public void testNumber(Pair<BigInteger, BigInteger> pair) {
        schema = new SchemaDescriptor(
            UUID.randomUUID(),
            42,
            new Column[] {new Column("key", NativeTypes.INT64, false)},
            new Column[] {
                new Column("number1", NativeTypes.numberOf(19), false),
                new Column("number2", NativeTypes.numberOf(10), false)
            }
        );

        TupleMarshaller marshaller = new TupleMarshallerImpl(null, tbl, new DummySchemaManagerImpl(schema));

        final Tuple tup = createTuple().set("key", rnd.nextLong()).set("number1", pair.getFirst()).set("number2", pair.getSecond());

        final Row row = marshaller.marshal(tup);

        assertEquals(row.numberValue(1), row.numberValue(2));
    }

    /**
     *
     */
    @Test
    public void testPrecisionRestrictionsForNumbers() {
        schema = new SchemaDescriptor(
            UUID.randomUUID(),
            42,
            new Column[] {new Column("key", NativeTypes.INT64, false)},
            new Column[] {new Column("number1", NativeTypes.numberOf(5), false)}
        );

        TupleMarshaller marshaller = new TupleMarshallerImpl(null, tbl, new DummySchemaManagerImpl(schema));

        final Tuple badTup = createTuple().set("key", rnd.nextLong());

        assertThrows(InvalidTypeException.class, () -> marshaller.marshal(badTup.set("number1", BigInteger.valueOf(999991L))), "Column's type mismatch");
        assertThrows(InvalidTypeException.class, () -> marshaller.marshal(badTup.set("number1", new BigInteger("111111"))), "Column's type mismatch");
        assertThrows(InvalidTypeException.class, () -> marshaller.marshal(badTup.set("number1", BigInteger.valueOf(-999991L))), "Column's type mismatch");
        assertThrows(InvalidTypeException.class, () -> marshaller.marshal(badTup.set("number1", new BigInteger("-111111"))), "Column's type mismatch");
    }

    /**
     *
     */
    @Test
    public void testPrecisionRestrictionsForDecimal() {
        schema = new SchemaDescriptor(
            UUID.randomUUID(),
            42,
            new Column[] {new Column("key", NativeTypes.INT64, false)},
            new Column[] {
                new Column("decimalCol", NativeTypes.decimalOf(9, 3), false),
            }
        );

        final Tuple badTup = createTuple().set("key", rnd.nextLong());

        TupleMarshaller marshaller = new TupleMarshallerImpl(null, tbl, new DummySchemaManagerImpl(schema));

        assertThrows(IllegalArgumentException.class,
            () -> marshaller.marshal(badTup.set("decimalCol", new BigDecimal("123456789.0123"))),
            "Failed to set decimal value for column"
        );
        assertThrows(IllegalArgumentException.class,
            () -> marshaller.marshal(badTup.set("decimalCol", new BigDecimal("-1234567890123"))),
            "Failed to set decimal value for column"
        );
        assertThrows(IllegalArgumentException.class,
            () -> marshaller.marshal(badTup.set("decimalCol", new BigDecimal("1234567"))),
            "Failed to set decimal value for column"
        );
        assertThrows(IllegalArgumentException.class,
            () -> marshaller.marshal(badTup.set("decimalCol", new BigDecimal("12345678.9"))),
            "Failed to set decimal value for column"
        );
    }

    /**
     *
     */
    @Test
    public void testStringDecimalSpecialCase() {
        schema = new SchemaDescriptor(
            UUID.randomUUID(),
            42,
            new Column[] {new Column("key", NativeTypes.INT64, false)},
            new Column[] {
                new Column("decimalCol", NativeTypes.decimalOf(1, 0), false),
            }
        );

        //representation of "0000" value.
        final Tuple tup = createTuple().set("key", rnd.nextLong()).set("decimalCol", new BigDecimal("0E+3"));

        TupleMarshaller marshaller = new TupleMarshallerImpl(null, tbl, new DummySchemaManagerImpl(schema));

        final Row row = marshaller.marshal(tup);

        assertEquals(row.decimalValue(1), BigDecimal.ZERO);
    }

    /**
     *
     */
    @ParameterizedTest
    @MethodSource("stringDecimalRepresentation")
    public void testUpscaleForDecimal(String decimalStr) {
        schema = new SchemaDescriptor(
            UUID.randomUUID(),
            42,
            new Column[] {new Column("key", NativeTypes.INT64, false)},
            new Column[] {
                new Column("decimalCol1", NativeTypes.decimalOf(9, 0), false)
            }
        );

        final Tuple tup = createTuple()
            .set("key", rnd.nextLong())
            .set("decimalCol1", new BigDecimal(decimalStr));

        TupleMarshaller marshaller = new TupleMarshallerImpl(null, tbl, new DummySchemaManagerImpl(schema));

        final Row row = marshaller.marshal(tup);

        assertEquals(row.decimalValue(1), new BigDecimal(decimalStr).setScale(0, RoundingMode.HALF_UP));
    }

    /**
     *
     */
    @Test
    public void testDecimalMaxScale() {
        schema = new SchemaDescriptor(
            UUID.randomUUID(),
            42,
            new Column[] {new Column("key", NativeTypes.INT64, false)},
            new Column[] {
                new Column("decimalCol", NativeTypes.decimalOf(Integer.MAX_VALUE, Integer.MAX_VALUE), false),
            }
        );

        final Tuple tup = createTuple()
            .set("key", rnd.nextLong())
            .set("decimalCol", BigDecimal.valueOf(123, Integer.MAX_VALUE));

        TupleMarshaller marshaller = new TupleMarshallerImpl(null, tbl, new DummySchemaManagerImpl(schema));

        final Row row = marshaller.marshal(tup);

        assertEquals(row.decimalValue(1), BigDecimal.valueOf(123, Integer.MAX_VALUE));
    }

    /**
     *
     */
    @ParameterizedTest
    @MethodSource("sameDecimals")
    public void testSameBinaryRepresentation(Pair<BigInteger, BigInteger> pair) throws IOException {
        schema = new SchemaDescriptor(
            UUID.randomUUID(),
            42,
            new Column[] {new Column("key", NativeTypes.INT64, false)},
            new Column[] {
                new Column("decimalCol", NativeTypes.decimalOf(19, 3), false),
            }
        );

        TupleMarshaller marshaller = new TupleMarshallerImpl(null, tbl, new DummySchemaManagerImpl(schema));

        long randomKey = rnd.nextLong();

        final Tuple firstTup = createTuple().set("key", randomKey).set("decimalCol", pair.getFirst());
        final Tuple secondTup = createTuple().set("key", randomKey).set("decimalCol", pair.getSecond());

        final Row firstRow = marshaller.marshal(firstTup);
        final Row secondRow = marshaller.marshal(secondTup);

        ByteArrayOutputStream stream = new ByteArrayOutputStream();

        firstRow.writeTo(stream);

        byte[] firstRowInBytes = stream.toByteArray();

        stream.reset();

        secondRow.writeTo(stream);

        byte[] secondRowInBytes = stream.toByteArray();

        assertArrayEquals(firstRowInBytes, secondRowInBytes);
    }

    /**
     * @return Tuple.
     */
    @NotNull private Tuple createTuple() {
        return Tuple.create();
    }
}
