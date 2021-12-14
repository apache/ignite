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
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.util.ArrayList;
import java.util.BitSet;
import java.util.Objects;
import java.util.Random;
import org.apache.ignite.internal.schema.Column;
import org.apache.ignite.internal.schema.NativeTypes;
import org.apache.ignite.internal.schema.marshaller.MarshallerException;
import org.apache.ignite.internal.schema.row.Row;
import org.apache.ignite.internal.schema.row.RowAssembler;
import org.apache.ignite.internal.schema.testobjects.TestObjectWithAllTypes;
import org.apache.ignite.internal.schema.testobjects.TestSimpleObject;
import org.apache.ignite.internal.util.Pair;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;

/**
 * Check field binding correctness.
 */
public class ColumnBindingTest {
    /** Random. */
    private Random rnd;

    /**
     * Init random and print seed before each test.
     */
    @BeforeEach
    public void initRandom() {
        long seed = System.currentTimeMillis();

        System.out.println("Using seed: " + seed + "L;");

        rnd = new Random(seed);
    }

    /**
     * All native types field binding.
     *
     * @throws Exception If failed.
     */
    @Test
    public void fieldBinding() throws Exception {
        Column[] cols = new Column[]{
                new Column("primitiveByteCol", INT8, false),
                new Column("primitiveShortCol", INT16, false),
                new Column("primitiveIntCol", INT32, false),
                new Column("primitiveLongCol", INT64, false),
                new Column("primitiveFloatCol", FLOAT, false),
                new Column("primitiveDoubleCol", DOUBLE, false),

                new Column("byteCol", INT8, false),
                new Column("shortCol", INT16, false),
                new Column("intCol", INT32, false),
                new Column("longCol", INT64, false),
                new Column("floatCol", FLOAT, false),
                new Column("doubleCol", DOUBLE, false),

                new Column("dateCol", DATE, false),
                new Column("timeCol", time(), false),
                new Column("dateTimeCol", datetime(), false),
                new Column("timestampCol", timestamp(), false),

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

        final TestObjectWithAllTypes obj = TestObjectWithAllTypes.randomObject(rnd);

        for (int i = 0; i < cols.length; i++) {
            ColumnBinding.createFieldBinding(cols[i].copy(i), TestObjectWithAllTypes.class, cols[i].name(), null).write(rowAssembler, obj);
        }

        final TestObjectWithAllTypes restoredObj = new TestObjectWithAllTypes();

        for (int i = 0; i < cols.length; i++) {
            ColumnBinding.createFieldBinding(cols[i].copy(i), TestObjectWithAllTypes.class, cols[i].name(), null).read(row, restoredObj);
        }

        assertEquals(obj, restoredObj);
    }

    /**
     * Nullable fields binding.
     *
     * @throws Exception If failed.
     */
    @Test
    public void nullableFieldsBinding() throws Exception {
        Column[] cols = new Column[]{
                new Column("intCol", INT32, true),
                new Column("longCol", INT64, true),

                new Column("stringCol", STRING, true),
                new Column("bytesCol", BYTES, true),
        };

        final Pair<RowAssembler, Row> mocks = createMocks();

        final RowAssembler rowAssembler = mocks.getFirst();
        final Row row = mocks.getSecond();

        final TestSimpleObject obj = TestSimpleObject.randomObject(rnd);

        for (int i = 0; i < cols.length; i++) {
            ColumnBinding.createFieldBinding(cols[i].copy(i), TestSimpleObject.class, cols[i].name(), null)
                    .write(rowAssembler, obj);
        }

        final TestSimpleObject restoredObj = new TestSimpleObject();

        for (int i = 0; i < cols.length; i++) {
            ColumnBinding.createFieldBinding(cols[i].copy(i), TestSimpleObject.class, cols[i].name(), null)
                    .read(row, restoredObj);
        }

        assertEquals(obj, restoredObj);
    }


    /**
     * Fields binding with converter.
     *
     * @throws Exception If failed.
     */
    @Test
    public void fieldsBindingWithConverter() throws Exception {
        Column[] cols = new Column[]{
                new Column("data", BYTES, true),
        };

        final Pair<RowAssembler, Row> mocks = createMocks();

        final RowAssembler rowAssembler = mocks.getFirst();
        final Row row = mocks.getSecond();

        TestObjectWrapper obj = new TestObjectWrapper();
        obj.data = TestSimpleObject.randomObject(rnd);

        for (int i = 0; i < cols.length; i++) {
            ColumnBinding.createFieldBinding(cols[i].copy(i), TestObjectWrapper.class, cols[i].name(), new SerializingConverter())
                    .write(rowAssembler, obj);
        }

        TestObjectWrapper restoredObj = new TestObjectWrapper();

        for (int i = 0; i < cols.length; i++) {
            ColumnBinding.createFieldBinding(cols[i].copy(i), TestObjectWrapper.class, cols[i].name(), new SerializingConverter())
                    .read(row, restoredObj);
        }

        assertEquals(obj, restoredObj);
    }

    /**
     * Identity binding.
     *
     * @throws Exception If failed.
     */
    @Test
    public void identityBinding() throws Exception {
        final ColumnBinding binding = ColumnBinding.createIdentityBinding(
                new Column("val", STRING, true).copy(0),
                String.class,
                null
        );

        assertEquals("Some string", binding.value("Some string"));
        final Pair<RowAssembler, Row> mocks = createMocks();

        binding.write(mocks.getFirst(), "Other string");
        assertEquals("Other string", binding.columnValue(mocks.getSecond()));
    }

    /**
     * Identity binding with converter.
     *
     * @throws Exception If failed.
     */
    @Test
    public void identityBindingWithConverter() throws Exception {
        final ColumnBinding binding = ColumnBinding.createIdentityBinding(
                new Column("val", BYTES, true).copy(0),
                TestSimpleObject.class,
                new SerializingConverter());

        final Pair<RowAssembler, Row> mocks = createMocks();

        final RowAssembler rowAssembler = mocks.getFirst();
        final Row row = mocks.getSecond();

        final TestSimpleObject obj = TestSimpleObject.randomObject(rnd);

        binding.write(rowAssembler, obj);

        Object restoredObj = binding.columnValue(row);

        assertEquals(obj, restoredObj);
    }

    /**
     * Wrong identity binding.
     */
    @Test
    public void wrongIdentityBinding() {
        // Incompatible types.
        final ColumnBinding binding = ColumnBinding.createIdentityBinding(
                new Column("val", UUID, true).copy(0),
                java.util.UUID.class,
                null);

        assertThrows(
                MarshallerException.class,
                () -> binding.value("Some string"));
        final Pair<RowAssembler, Row> mocks = createMocks();

        assertThrows(
                MarshallerException.class,
                () -> binding.write(mocks.getFirst(), "Other string")
        );

        // Implicit serialization is not supported yet.
        assertThrows(
                IllegalArgumentException.class,
                () -> ColumnBinding.createIdentityBinding(
                        new Column("val", BYTES, true).copy(0),
                        TestSimpleObject.class,
                        null)
        );
    }

    /**
     * Wrong identity binding.
     */
    @Test
    public void wrongBinding() {
        // Incompatible types.
        assertThrows(
                IllegalArgumentException.class,
                () -> ColumnBinding.createFieldBinding(
                        new Column("val", UUID, true).copy(0),
                        TestObjectWrapper.class,
                        "data", null)
        );

        // Implicit serialization is not supported yet.
        assertThrows(
                IllegalArgumentException.class,
                () -> ColumnBinding.createFieldBinding(
                        new Column("val", BYTES, true).copy(0),
                        TestObjectWrapper.class,
                        "data", null)
        );
    }

    /**
     * Creates mock pair for {@link Row} and {@link RowAssembler}.
     *
     * @return Pair of mocks.
     */
    private Pair<RowAssembler, Row> createMocks() {
        final ArrayList<Object> vals = new ArrayList<>();

        final RowAssembler mockedAsm = Mockito.mock(RowAssembler.class);
        final Row mockedRow = Mockito.mock(Row.class);

        final Answer<Void> asmAnswer = new Answer<>() {
            @Override
            public Void answer(InvocationOnMock invocation) {
                if ("appendNull".equals(invocation.getMethod().getName())) {
                    vals.add(null);
                } else {
                    vals.add(invocation.getArguments()[0]);
                }

                return null;
            }
        };

        final Answer<Object> rowAnswer = new Answer<>() {
            @Override
            public Object answer(InvocationOnMock invocation) {
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

        Mockito.doAnswer(asmAnswer).when(mockedAsm).appendDate(Mockito.any(LocalDate.class));
        Mockito.doAnswer(asmAnswer).when(mockedAsm).appendDateTime(Mockito.any(LocalDateTime.class));
        Mockito.doAnswer(asmAnswer).when(mockedAsm).appendTime(Mockito.any(LocalTime.class));
        Mockito.doAnswer(asmAnswer).when(mockedAsm).appendTimestamp(Mockito.any(Instant.class));

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

        Mockito.doAnswer(rowAnswer).when(mockedRow).dateValue(Mockito.anyInt());
        Mockito.doAnswer(rowAnswer).when(mockedRow).timeValue(Mockito.anyInt());
        Mockito.doAnswer(rowAnswer).when(mockedRow).dateTimeValue(Mockito.anyInt());
        Mockito.doAnswer(rowAnswer).when(mockedRow).timestampValue(Mockito.anyInt());

        Mockito.doAnswer(rowAnswer).when(mockedRow).uuidValue(Mockito.anyInt());
        Mockito.doAnswer(rowAnswer).when(mockedRow).bitmaskValue(Mockito.anyInt());
        Mockito.doAnswer(rowAnswer).when(mockedRow).stringValue(Mockito.anyInt());
        Mockito.doAnswer(rowAnswer).when(mockedRow).bytesValue(Mockito.anyInt());
        Mockito.doAnswer(rowAnswer).when(mockedRow).numberValue(Mockito.anyInt());
        Mockito.doAnswer(rowAnswer).when(mockedRow).decimalValue(Mockito.anyInt());

        return new Pair<>(mockedAsm, mockedRow);
    }

    /**
     * Object wrapper.
     */
    static class TestObjectWrapper {
        TestSimpleObject data;

        @Override
        public boolean equals(Object o) {
            if (this == o) {
                return true;
            }
            if (o == null || getClass() != o.getClass()) {
                return false;
            }
            TestObjectWrapper that = (TestObjectWrapper) o;
            return Objects.equals(data, that.data);
        }

        @Override
        public int hashCode() {
            return Objects.hash(data);
        }
    }
}
