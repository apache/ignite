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

package org.apache.ignite.internal.marshaller;

import static org.apache.ignite.internal.marshaller.BinaryMode.BITSET;
import static org.apache.ignite.internal.marshaller.BinaryMode.BYTE;
import static org.apache.ignite.internal.marshaller.BinaryMode.BYTE_ARR;
import static org.apache.ignite.internal.marshaller.BinaryMode.DATE;
import static org.apache.ignite.internal.marshaller.BinaryMode.DATETIME;
import static org.apache.ignite.internal.marshaller.BinaryMode.DECIMAL;
import static org.apache.ignite.internal.marshaller.BinaryMode.DOUBLE;
import static org.apache.ignite.internal.marshaller.BinaryMode.FLOAT;
import static org.apache.ignite.internal.marshaller.BinaryMode.INT;
import static org.apache.ignite.internal.marshaller.BinaryMode.LONG;
import static org.apache.ignite.internal.marshaller.BinaryMode.NUMBER;
import static org.apache.ignite.internal.marshaller.BinaryMode.SHORT;
import static org.apache.ignite.internal.marshaller.BinaryMode.STRING;
import static org.apache.ignite.internal.marshaller.BinaryMode.TIME;
import static org.apache.ignite.internal.marshaller.BinaryMode.TIMESTAMP;
import static org.apache.ignite.internal.marshaller.BinaryMode.UUID;
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
import java.util.Random;
import org.apache.ignite.internal.marshaller.testobjects.TestObjectWithAllTypes;
import org.apache.ignite.internal.marshaller.testobjects.TestSimpleObject;
import org.apache.ignite.internal.util.Pair;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;

/**
 * Check field accessor correctness.
 */
public class FieldAccessorTest {
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
     * FieldAccessor.
     * TODO Documentation https://issues.apache.org/jira/browse/IGNITE-15859
     *
     * @throws Exception If failed.
     */
    @Test
    public void fieldAccessor() throws Exception {
        MarshallerColumn[] cols = new MarshallerColumn[]{
                new MarshallerColumn("primitiveByteCol", BYTE),
                new MarshallerColumn("primitiveShortCol", SHORT),
                new MarshallerColumn("primitiveIntCol", INT),
                new MarshallerColumn("primitiveLongCol", LONG),
                new MarshallerColumn("primitiveFloatCol", FLOAT),
                new MarshallerColumn("primitiveDoubleCol", DOUBLE),

                new MarshallerColumn("byteCol", BYTE),
                new MarshallerColumn("shortCol", SHORT),
                new MarshallerColumn("intCol", INT),
                new MarshallerColumn("longCol", LONG),
                new MarshallerColumn("floatCol", FLOAT),
                new MarshallerColumn("doubleCol", DOUBLE),

                new MarshallerColumn("dateCol", DATE),
                new MarshallerColumn("timeCol", TIME),
                new MarshallerColumn("dateTimeCol", DATETIME),
                new MarshallerColumn("timestampCol", TIMESTAMP),

                new MarshallerColumn("uuidCol", UUID),
                new MarshallerColumn("bitmaskCol", BITSET),
                new MarshallerColumn("stringCol", STRING),
                new MarshallerColumn("bytesCol", BYTE_ARR),
                new MarshallerColumn("numberCol", NUMBER),
                new MarshallerColumn("decimalCol", DECIMAL),
        };

        final Pair<MarshallerWriter, MarshallerReader> mocks = createMocks();

        final MarshallerWriter writer = mocks.getFirst();
        final MarshallerReader reader = mocks.getSecond();

        final TestObjectWithAllTypes obj = TestObjectWithAllTypes.randomObject(rnd);

        for (int i = 0; i < cols.length; i++) {
            FieldAccessor accessor = FieldAccessor
                    .create(TestObjectWithAllTypes.class, cols[i].name(), cols[i], i);

            accessor.write(writer, obj);
        }

        final TestObjectWithAllTypes restoredObj = new TestObjectWithAllTypes();

        for (int i = 0; i < cols.length; i++) {
            FieldAccessor accessor = FieldAccessor
                    .create(TestObjectWithAllTypes.class, cols[i].name(), cols[i], i);

            accessor.read(reader, restoredObj);
        }

        assertEquals(obj, restoredObj);
    }

    /**
     * NullableFieldsAccessor.
     * TODO Documentation https://issues.apache.org/jira/browse/IGNITE-15859
     *
     * @throws Exception If failed.
     */
    @Test
    public void nullableFieldsAccessor() throws Exception {
        MarshallerColumn[] cols = new MarshallerColumn[]{
                new MarshallerColumn("intCol", INT),
                new MarshallerColumn("longCol", LONG),

                new MarshallerColumn("stringCol", STRING),
                new MarshallerColumn("bytesCol", BYTE_ARR),
        };

        final Pair<MarshallerWriter, MarshallerReader> mocks = createMocks();

        final MarshallerWriter writer = mocks.getFirst();
        final MarshallerReader reader = mocks.getSecond();

        final TestSimpleObject obj = TestSimpleObject.randomObject(rnd);

        for (int i = 0; i < cols.length; i++) {
            FieldAccessor accessor = FieldAccessor
                    .create(TestSimpleObject.class, cols[i].name(), cols[i], i);

            accessor.write(writer, obj);
        }

        final TestSimpleObject restoredObj = new TestSimpleObject();

        for (int i = 0; i < cols.length; i++) {
            FieldAccessor accessor = FieldAccessor
                    .create(TestSimpleObject.class, cols[i].name(), cols[i], i);

            accessor.read(reader, restoredObj);
        }

        assertEquals(obj, restoredObj);
    }

    /**
     * IdentityAccessor.
     * TODO Documentation https://issues.apache.org/jira/browse/IGNITE-15859
     *
     * @throws Exception If failed.
     */
    @Test
    public void identityAccessor() throws Exception {
        final FieldAccessor accessor = FieldAccessor.createIdentityAccessor(
                "col0",
                0,
                STRING);

        assertEquals("Some string", accessor.value("Some string"));

        final Pair<MarshallerWriter, MarshallerReader> mocks = createMocks();

        accessor.write(mocks.getFirst(), "Other string");
        assertEquals("Other string", accessor.read(mocks.getSecond()));
    }

    /**
     * WrongIdentityAccessor.
     * TODO Documentation https://issues.apache.org/jira/browse/IGNITE-15859
     */
    @Test
    public void wrongIdentityAccessor() {
        final FieldAccessor accessor = FieldAccessor.createIdentityAccessor(
                "col0",
                42,
                UUID);

        assertEquals("Some string", accessor.value("Some string"));

        final Pair<MarshallerWriter, MarshallerReader> mocks = createMocks();

        assertThrows(
                MarshallerException.class,
                () -> accessor.write(mocks.getFirst(), "Other string"),
                "Failed to write field [id=42]"
        );
    }

    /**
     * Creates mock pair for {@link MarshallerWriter} and {@link MarshallerReader}.
     *
     * @return Pair of mocks.
     */
    private Pair<MarshallerWriter, MarshallerReader> createMocks() {
        final ArrayList<Object> vals = new ArrayList<>();

        final MarshallerWriter mockedAsm = Mockito.mock(MarshallerWriter.class);
        final MarshallerReader mockedRow = Mockito.mock(MarshallerReader.class);

        final Answer<Void> asmAnswer = new Answer<>() {
            @Override
            public Void answer(InvocationOnMock invocation) {
                if ("writeNull".equals(invocation.getMethod().getName())) {
                    vals.add(null);
                } else {
                    vals.add(invocation.getArguments()[0]);
                }

                return null;
            }
        };

        final Answer<Object> rowAnswer = new Answer<>() {
            int idx;

            @Override
            public Object answer(InvocationOnMock invocation) {
                return vals.get(idx++);
            }
        };

        Mockito.doAnswer(asmAnswer).when(mockedAsm).writeNull();
        Mockito.doAnswer(asmAnswer).when(mockedAsm).writeByte(Mockito.anyByte());
        Mockito.doAnswer(asmAnswer).when(mockedAsm).writeShort(Mockito.anyShort());
        Mockito.doAnswer(asmAnswer).when(mockedAsm).writeInt(Mockito.anyInt());
        Mockito.doAnswer(asmAnswer).when(mockedAsm).writeLong(Mockito.anyLong());
        Mockito.doAnswer(asmAnswer).when(mockedAsm).writeFloat(Mockito.anyFloat());
        Mockito.doAnswer(asmAnswer).when(mockedAsm).writeDouble(Mockito.anyDouble());

        Mockito.doAnswer(asmAnswer).when(mockedAsm).writeUuid(Mockito.any(java.util.UUID.class));
        Mockito.doAnswer(asmAnswer).when(mockedAsm).writeBitSet(Mockito.any(BitSet.class));
        Mockito.doAnswer(asmAnswer).when(mockedAsm).writeString(Mockito.anyString());
        Mockito.doAnswer(asmAnswer).when(mockedAsm).writeBytes(Mockito.any(byte[].class));
        Mockito.doAnswer(asmAnswer).when(mockedAsm).writeBigInt(Mockito.any(BigInteger.class));
        Mockito.doAnswer(asmAnswer).when(mockedAsm).writeBigDecimal(Mockito.any(BigDecimal.class));

        Mockito.doAnswer(asmAnswer).when(mockedAsm).writeDate(Mockito.any(LocalDate.class));
        Mockito.doAnswer(asmAnswer).when(mockedAsm).writeDateTime(Mockito.any(LocalDateTime.class));
        Mockito.doAnswer(asmAnswer).when(mockedAsm).writeTime(Mockito.any(LocalTime.class));
        Mockito.doAnswer(asmAnswer).when(mockedAsm).writeTimestamp(Mockito.any(Instant.class));

        Mockito.doAnswer(rowAnswer).when(mockedRow).readByte();
        Mockito.doAnswer(rowAnswer).when(mockedRow).readByteBoxed();
        Mockito.doAnswer(rowAnswer).when(mockedRow).readShort();
        Mockito.doAnswer(rowAnswer).when(mockedRow).readShortBoxed();
        Mockito.doAnswer(rowAnswer).when(mockedRow).readInt();
        Mockito.doAnswer(rowAnswer).when(mockedRow).readIntBoxed();
        Mockito.doAnswer(rowAnswer).when(mockedRow).readLong();
        Mockito.doAnswer(rowAnswer).when(mockedRow).readLongBoxed();
        Mockito.doAnswer(rowAnswer).when(mockedRow).readFloat();
        Mockito.doAnswer(rowAnswer).when(mockedRow).readFloatBoxed();
        Mockito.doAnswer(rowAnswer).when(mockedRow).readDouble();
        Mockito.doAnswer(rowAnswer).when(mockedRow).readDoubleBoxed();

        Mockito.doAnswer(rowAnswer).when(mockedRow).readDate();
        Mockito.doAnswer(rowAnswer).when(mockedRow).readTime();
        Mockito.doAnswer(rowAnswer).when(mockedRow).readDateTime();
        Mockito.doAnswer(rowAnswer).when(mockedRow).readTimestamp();

        Mockito.doAnswer(rowAnswer).when(mockedRow).readUuid();
        Mockito.doAnswer(rowAnswer).when(mockedRow).readBitSet();
        Mockito.doAnswer(rowAnswer).when(mockedRow).readString();
        Mockito.doAnswer(rowAnswer).when(mockedRow).readBytes();
        Mockito.doAnswer(rowAnswer).when(mockedRow).readBigInt();
        Mockito.doAnswer(rowAnswer).when(mockedRow).readBigDecimal();

        return new Pair<>(mockedAsm, mockedRow);
    }
}
