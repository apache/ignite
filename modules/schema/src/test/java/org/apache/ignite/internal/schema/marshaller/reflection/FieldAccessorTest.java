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

package org.apache.ignite.internal.schema.marshaller.reflection;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.BitSet;
import java.util.Objects;
import java.util.Random;
import java.util.UUID;
import org.apache.ignite.internal.schema.Column;
import org.apache.ignite.internal.schema.NativeTypes;
import org.apache.ignite.internal.schema.TestUtils;
import org.apache.ignite.internal.schema.marshaller.BinaryMode;
import org.apache.ignite.internal.schema.marshaller.SerializationException;
import org.apache.ignite.internal.schema.row.Row;
import org.apache.ignite.internal.schema.row.RowAssembler;
import org.apache.ignite.internal.testframework.IgniteTestUtils;
import org.apache.ignite.internal.util.Pair;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;

import static org.apache.ignite.internal.schema.NativeTypes.BYTES;
import static org.apache.ignite.internal.schema.NativeTypes.DOUBLE;
import static org.apache.ignite.internal.schema.NativeTypes.FLOAT;
import static org.apache.ignite.internal.schema.NativeTypes.INT16;
import static org.apache.ignite.internal.schema.NativeTypes.INT32;
import static org.apache.ignite.internal.schema.NativeTypes.INT64;
import static org.apache.ignite.internal.schema.NativeTypes.INT8;
import static org.apache.ignite.internal.schema.NativeTypes.STRING;
import static org.apache.ignite.internal.schema.NativeTypes.UUID;
import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

/**
 * Check field accessor correctness.
 */
public class FieldAccessorTest {
    /** Random. */
    private Random rnd;

    /**
     *
     */
    @BeforeEach
    public void initRandom() {
        long seed = System.currentTimeMillis();

        System.out.println("Using seed: " + seed + "L;");

        rnd = new Random(seed);
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void fieldAccessor() throws Exception {
        Column[] cols = new Column[] {
            new Column("pByteCol", INT8, false),
            new Column("pShortCol", INT16, false),
            new Column("pIntCol", INT32, false),
            new Column("pLongCol", INT64, false),
            new Column("pFloatCol", FLOAT, false),
            new Column("pDoubleCol", DOUBLE, false),

            new Column("byteCol", INT8, false),
            new Column("shortCol", INT16, false),
            new Column("intCol", INT32, false),
            new Column("longCol", INT64, false),
            new Column("floatCol", FLOAT, false),
            new Column("doubleCol", DOUBLE, false),

            new Column("uuidCol", UUID, false),
            new Column("bitmaskCol", NativeTypes.bitmaskOf(9), false),
            new Column("stringCol", STRING, false),
            new Column("bytesCol", BYTES, false),
            new Column("numberCol", NativeTypes.numberOf(21), false),
            new Column("decimalCol", NativeTypes.decimalOf(19, 3), false),
        };

        final Pair<RowAssembler, Row> mocks = createMocks();

        final RowAssembler rowAssembler = mocks.getFirst();
        final Row row = mocks.getSecond();

        final TestObject obj = TestObject.randomObject(rnd);

        for (int i = 0; i < cols.length; i++) {
            FieldAccessor accessor = FieldAccessor.create(TestObject.class, cols[i], i);

            accessor.write(rowAssembler, obj);
        }

        final TestObject restoredObj = new TestObject();

        for (int i = 0; i < cols.length; i++) {
            FieldAccessor accessor = FieldAccessor.create(TestObject.class, cols[i], i);

            accessor.read(row, restoredObj);
        }

        assertEquals(obj.pByteCol, restoredObj.pByteCol);
        assertEquals(obj.pShortCol, restoredObj.pShortCol);
        assertEquals(obj.pIntCol, restoredObj.pIntCol);
        assertEquals(obj.pLongCol, restoredObj.pLongCol);
        assertEquals(obj.pFloatCol, restoredObj.pFloatCol);
        assertEquals(obj.pDoubleCol, restoredObj.pDoubleCol);

        assertEquals(obj.byteCol, restoredObj.byteCol);
        assertEquals(obj.shortCol, restoredObj.shortCol);
        assertEquals(obj.intCol, restoredObj.intCol);
        assertEquals(obj.longCol, restoredObj.longCol);
        assertEquals(obj.floatCol, restoredObj.floatCol);
        assertEquals(obj.doubleCol, restoredObj.doubleCol);

        assertEquals(obj.uuidCol, restoredObj.uuidCol);
        assertEquals(obj.bitmaskCol, restoredObj.bitmaskCol);
        assertEquals(obj.stringCol, restoredObj.stringCol);
        assertArrayEquals(obj.bytesCol, restoredObj.bytesCol);
        assertEquals(obj.numberCol, restoredObj.numberCol);
        assertEquals(obj.decimalCol, restoredObj.decimalCol);
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void nullableFieldsAccessor() throws Exception {
        Column[] cols = new Column[] {
            new Column("intCol", INT32, true),
            new Column("longCol", INT64, true),

            new Column("stringCol", STRING, true),
            new Column("bytesCol", BYTES, true),
        };

        final Pair<RowAssembler, Row> mocks = createMocks();

        final RowAssembler rowAssembler = mocks.getFirst();
        final Row row = mocks.getSecond();

        final TestSimpleObject obj = new TestSimpleObject();
        obj.longCol = rnd.nextLong();
        obj.stringCol = IgniteTestUtils.randomString(rnd, 255);

        for (int i = 0; i < cols.length; i++) {
            FieldAccessor accessor = FieldAccessor.create(TestSimpleObject.class, cols[i], i);

            accessor.write(rowAssembler, obj);
        }

        final TestSimpleObject restoredObj = new TestSimpleObject();

        for (int i = 0; i < cols.length; i++) {
            FieldAccessor accessor = FieldAccessor.create(TestSimpleObject.class, cols[i], i);

            accessor.read(row, restoredObj);
        }

        assertEquals(obj.intCol, restoredObj.intCol);
        assertEquals(obj.longCol, restoredObj.longCol);

        assertEquals(obj.stringCol, restoredObj.stringCol);
        assertArrayEquals(obj.bytesCol, restoredObj.bytesCol);
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void identityAccessor() throws Exception {
        final FieldAccessor accessor = FieldAccessor.createIdentityAccessor(
            new Column("col0", STRING, true),
            0,
            BinaryMode.STRING);

        assertEquals("Some string", accessor.value("Some string"));

        final Pair<RowAssembler, Row> mocks = createMocks();

        accessor.write(mocks.getFirst(), "Other string");
        assertEquals("Other string", accessor.read(mocks.getSecond()));
    }

    /**
     *
     */
    @Test
    public void wrongIdentityAccessor() {
        final FieldAccessor accessor = FieldAccessor.createIdentityAccessor(
            new Column("col0", STRING, true),
            42,
            BinaryMode.UUID);

        assertEquals("Some string", accessor.value("Some string"));

        final Pair<RowAssembler, Row> mocks = createMocks();

        assertThrows(
            SerializationException.class,
            () -> accessor.write(mocks.getFirst(), "Other string"),
            "Failed to write field [id=42]"
        );
    }

    /**
     * Creates mock pair for {@link Row} and {@link RowAssembler ).
     *
     * @return Pair of mocks.
     */
    private Pair<RowAssembler, Row> createMocks() {
        final ArrayList<Object> vals = new ArrayList<>();

        final RowAssembler mockedAsm = Mockito.mock(RowAssembler.class);
        final Row mockedRow = Mockito.mock(Row.class);

        final Answer<Void> asmAnswer = new Answer<>() {
            @Override public Void answer(InvocationOnMock invocation) {
                if ("appendNull".equals(invocation.getMethod().getName()))
                    vals.add(null);
                else
                    vals.add(invocation.getArguments()[0]);

                return null;
            }
        };

        final Answer<Object> rowAnswer = new Answer<>() {
            @Override public Object answer(InvocationOnMock invocation) {
                final int idx = invocation.getArgument(0, Integer.class);

                return vals.get(idx);
            }
        };

        Mockito.doAnswer(asmAnswer).when(mockedAsm).appendNull();
        Mockito.doAnswer(asmAnswer).when(mockedAsm).appendByte(Mockito.anyByte());
        Mockito.doAnswer(asmAnswer).when(mockedAsm).appendShort(Mockito.anyShort());
        Mockito.doAnswer(asmAnswer).when(mockedAsm).appendInt(Mockito.anyInt());
        Mockito.doAnswer(asmAnswer).when(mockedAsm).appendLong(Mockito.anyLong());
        Mockito.doAnswer(asmAnswer).when(mockedAsm).appendFloat(Mockito.anyFloat());
        Mockito.doAnswer(asmAnswer).when(mockedAsm).appendDouble(Mockito.anyDouble());

        Mockito.doAnswer(asmAnswer).when(mockedAsm).appendUuid(Mockito.any(java.util.UUID.class));
        Mockito.doAnswer(asmAnswer).when(mockedAsm).appendBitmask(Mockito.any(BitSet.class));
        Mockito.doAnswer(asmAnswer).when(mockedAsm).appendString(Mockito.anyString());
        Mockito.doAnswer(asmAnswer).when(mockedAsm).appendBytes(Mockito.any(byte[].class));
        Mockito.doAnswer(asmAnswer).when(mockedAsm).appendNumber(Mockito.any(BigInteger.class));
        Mockito.doAnswer(asmAnswer).when(mockedAsm).appendDecimal(Mockito.any(BigDecimal.class));

        Mockito.doAnswer(rowAnswer).when(mockedRow).byteValue(Mockito.anyInt());
        Mockito.doAnswer(rowAnswer).when(mockedRow).byteValueBoxed(Mockito.anyInt());
        Mockito.doAnswer(rowAnswer).when(mockedRow).shortValue(Mockito.anyInt());
        Mockito.doAnswer(rowAnswer).when(mockedRow).shortValueBoxed(Mockito.anyInt());
        Mockito.doAnswer(rowAnswer).when(mockedRow).intValue(Mockito.anyInt());
        Mockito.doAnswer(rowAnswer).when(mockedRow).intValueBoxed(Mockito.anyInt());
        Mockito.doAnswer(rowAnswer).when(mockedRow).longValue(Mockito.anyInt());
        Mockito.doAnswer(rowAnswer).when(mockedRow).longValueBoxed(Mockito.anyInt());
        Mockito.doAnswer(rowAnswer).when(mockedRow).floatValue(Mockito.anyInt());
        Mockito.doAnswer(rowAnswer).when(mockedRow).floatValueBoxed(Mockito.anyInt());
        Mockito.doAnswer(rowAnswer).when(mockedRow).doubleValue(Mockito.anyInt());
        Mockito.doAnswer(rowAnswer).when(mockedRow).doubleValueBoxed(Mockito.anyInt());

        Mockito.doAnswer(rowAnswer).when(mockedRow).uuidValue(Mockito.anyInt());
        Mockito.doAnswer(rowAnswer).when(mockedRow).bitmaskValue(Mockito.anyInt());
        Mockito.doAnswer(rowAnswer).when(mockedRow).stringValue(Mockito.anyInt());
        Mockito.doAnswer(rowAnswer).when(mockedRow).bytesValue(Mockito.anyInt());
        Mockito.doAnswer(rowAnswer).when(mockedRow).numberValue(Mockito.anyInt());
        Mockito.doAnswer(rowAnswer).when(mockedRow).decimalValue(Mockito.anyInt());

        return new Pair<>(mockedAsm, mockedRow);
    }

    /**
     * Test object.
     */
    @SuppressWarnings("InstanceVariableMayNotBeInitialized")
    private static class TestObject {
        /**
         * @return Random TestObject.
         */
        public static TestObject randomObject(Random rnd) {
            final TestObject obj = new TestObject();

            obj.pByteCol = (byte)rnd.nextInt(255);
            obj.pShortCol = (short)rnd.nextInt(65535);
            obj.pIntCol = rnd.nextInt();
            obj.pLongCol = rnd.nextLong();
            obj.pFloatCol = rnd.nextFloat();
            obj.pDoubleCol = rnd.nextDouble();

            obj.byteCol = (byte)rnd.nextInt(255);
            obj.shortCol = (short)rnd.nextInt(65535);
            obj.intCol = rnd.nextInt();
            obj.longCol = rnd.nextLong();
            obj.floatCol = rnd.nextFloat();
            obj.doubleCol = rnd.nextDouble();

            obj.uuidCol = new UUID(rnd.nextLong(), rnd.nextLong());
            obj.bitmaskCol = IgniteTestUtils.randomBitSet(rnd, rnd.nextInt(42));
            obj.stringCol = IgniteTestUtils.randomString(rnd, rnd.nextInt(255));
            obj.bytesCol = IgniteTestUtils.randomBytes(rnd, rnd.nextInt(255));
            obj.numberCol = (BigInteger)TestUtils.generateRandomValue(rnd, NativeTypes.numberOf(12));
            obj.decimalCol = (BigDecimal) TestUtils.generateRandomValue(rnd, NativeTypes.decimalOf(19, 3));

            return obj;
        }

        // Primitive typed
        private byte pByteCol;

        private short pShortCol;

        private int pIntCol;

        private long pLongCol;

        private float pFloatCol;

        private double pDoubleCol;

        // Reference typed
        private Byte byteCol;

        private Short shortCol;

        private Integer intCol;

        private Long longCol;

        private Float floatCol;

        private Double doubleCol;

        private UUID uuidCol;

        private BitSet bitmaskCol;

        private String stringCol;

        private byte[] bytesCol;

        private BigInteger numberCol;

        private BigDecimal decimalCol;

        /** {@inheritDoc} */
        @Override public boolean equals(Object o) {
            if (this == o)
                return true;
            if (o == null || getClass() != o.getClass())
                return false;
            TestObject object = (TestObject)o;
            return pByteCol == object.pByteCol &&
                pShortCol == object.pShortCol &&
                pIntCol == object.pIntCol &&
                pLongCol == object.pLongCol &&
                Float.compare(object.pFloatCol, pFloatCol) == 0 &&
                Double.compare(object.pDoubleCol, pDoubleCol) == 0 &&
                Objects.equals(byteCol, object.byteCol) &&
                Objects.equals(shortCol, object.shortCol) &&
                Objects.equals(intCol, object.intCol) &&
                Objects.equals(longCol, object.longCol) &&
                Objects.equals(floatCol, object.floatCol) &&
                Objects.equals(doubleCol, object.doubleCol) &&
                Objects.equals(uuidCol, object.uuidCol) &&
                Objects.equals(bitmaskCol, object.bitmaskCol) &&
                Objects.equals(stringCol, object.stringCol) &&
                Arrays.equals(bytesCol, object.bytesCol) &&
                Objects.equals(numberCol, object.numberCol) &&
                Objects.equals(decimalCol, object.decimalCol);
        }

        /** {@inheritDoc} */
        @Override public int hashCode() {
            return 73;
        }
    }

    /**
     * Test object.
     */
    @SuppressWarnings("InstanceVariableMayNotBeInitialized")
    private static class TestSimpleObject {
        Long longCol;

        Integer intCol;

        byte[] bytesCol;

        String stringCol;

        /** {@inheritDoc} */
        @Override public boolean equals(Object o) {
            if (this == o)
                return true;

            if (o == null || getClass() != o.getClass())
                return false;

            TestSimpleObject object = (TestSimpleObject)o;

            return Objects.equals(longCol, object.longCol) &&
                Objects.equals(intCol, object.intCol) &&
                Arrays.equals(bytesCol, object.bytesCol) &&
                Objects.equals(stringCol, object.stringCol);
        }

        /** {@inheritDoc} */
        @Override public int hashCode() {
            return 42;
        }
    }
}
