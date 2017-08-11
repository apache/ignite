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

package org.apache.ignite.internal.binary;

import java.math.BigDecimal;
import java.sql.Time;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;
import java.util.UUID;
import org.apache.ignite.internal.binary.compression.BinaryCompression;
import org.apache.ignite.internal.util.typedef.internal.S;
import org.apache.ignite.testframework.GridTestUtils;

/**
 * Tests of data compression.
 */
public class AnnotatedFieldsMarshallingTest extends BinaryMarshallerSelfTest {

    /**
     * @throws Exception If failed.
     */
    public void testOneFieldClassCompression() throws Exception {
        TestsClassOneField sut = new TestsClassOneField();

        assertEquals(sut, marshalUnmarshal(sut));
    }

    /**
     * @throws Exception If failed.
     */
    public void testAnnotatedCompression() throws Exception {
        SimpleObjectAnnotatedFields sut = simpleAnnotatedObject();

        assertEquals(sut, marshalUnmarshal(sut));
    }

    /** Test class. */
    private static class TestsClassOneField {
        @BinaryCompression
        private String data = "абвгдеёжзийклмнопрстуфхцчшщъыьэюяАБВГДЕЁЖЗИЙКЛМНОПРСТУФХЦЧШЩЪЫЬЭЮЯabcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789/|()[]{}/*-+_&^%$#@!";

        /** {@inheritDoc} */
        @Override public boolean equals(Object o) {
            if (this == o)
                return true;
            if (o == null || getClass() != o.getClass())
                return false;

            TestsClassOneField fields = (TestsClassOneField)o;

            return data.equals(fields.data);
        }

        /** {@inheritDoc} */
        @Override public int hashCode() {
            return data.hashCode();
        }
    }

    /**
     * @return Initialized {@link SimpleObjectAnnotatedFields}
     */
    private static SimpleObjectAnnotatedFields simpleAnnotatedObject() {
        SimpleObjectAnnotatedFields inner = new SimpleObjectAnnotatedFields();

        inner.b = 1;
        inner.s = 1;
        inner.i = 1;
        inner.l = 1;
        inner.f = 1.1f;
        inner.d = 1.1d;
        inner.c = 1;
        inner.bool = true;
        inner.str = "str1";
        inner.uuid = UUID.randomUUID();
        inner.date = new Date();
        inner.ts = new Timestamp(System.currentTimeMillis());
        inner.bArr = new byte[] {1, 2, 3};
        inner.sArr = new short[] {1, 2, 3};
        inner.iArr = new int[] {1, 2, 3};
        inner.lArr = new long[] {1, 2, 3};
        inner.fArr = new float[] {1.1f, 2.2f, 3.3f};
        inner.dArr = new double[] {1.1d, 2.2d, 3.3d};
        inner.cArr = new char[] {1, 2, 3};
        inner.boolArr = new boolean[] {true, false, true};
        inner.strArr = new String[] {"str1", "str2", "str3"};
        inner.uuidArr = new UUID[] {UUID.randomUUID(), UUID.randomUUID(), UUID.randomUUID()};
        inner.dateArr = new Date[] {new Date(11111), new Date(22222), new Date(33333)};
        inner.objArr = new Object[] {UUID.randomUUID(), UUID.randomUUID(), UUID.randomUUID()};
        inner.col = new ArrayList<>();
        inner.map = new HashMap<>();
        inner.enumVal = TestEnum.A;
        inner.enumArr = new TestEnum[] {TestEnum.A, TestEnum.B};
        inner.bdArr = new BigDecimal[] {new BigDecimal(1000), BigDecimal.ONE};

        inner.col.add("str1");
        inner.col.add("str2");
        inner.col.add("str3");

        inner.map.put(1, "str1");
        inner.map.put(2, "str2");
        inner.map.put(3, "str3");

        SimpleObjectAnnotatedFields outer = new SimpleObjectAnnotatedFields();

        outer.b = 2;
        outer.s = 2;
        outer.i = 2;
        outer.l = 2;
        outer.f = 2.2f;
        outer.d = 2.2d;
        outer.c = 2;
        outer.bool = false;
        outer.str = "str2";
        outer.uuid = UUID.randomUUID();
        outer.date = new Date();
        outer.ts = new Timestamp(System.currentTimeMillis());
        outer.bArr = new byte[] {10, 20, 30};
        outer.sArr = new short[] {10, 20, 30};
        outer.iArr = new int[] {10, 20, 30};
        outer.lArr = new long[] {10, 20, 30};
        outer.fArr = new float[] {10.01f, 20.02f, 30.03f};
        outer.dArr = new double[] {10.01d, 20.02d, 30.03d};
        outer.cArr = new char[] {10, 20, 30};
        outer.boolArr = new boolean[] {false, true, false};
        outer.strArr = new String[] {"str10", "str20", "str30"};
        outer.uuidArr = new UUID[] {UUID.randomUUID(), UUID.randomUUID(), UUID.randomUUID()};
        outer.dateArr = new Date[] {new Date(44444), new Date(55555), new Date(66666)};
        outer.objArr = new Object[] {UUID.randomUUID(), UUID.randomUUID(), UUID.randomUUID()};
        outer.col = new ArrayList<>();
        outer.map = new HashMap<>();
        outer.enumVal = TestEnum.B;
        outer.enumArr = new TestEnum[] {TestEnum.B, TestEnum.C};
        outer.inner = inner;
        outer.bdArr = new BigDecimal[] {new BigDecimal(5000), BigDecimal.TEN};

        outer.col.add("str4");
        outer.col.add("str5");
        outer.col.add("str6");

        outer.map.put(4, "str4");
        outer.map.put(5, "str5");
        outer.map.put(6, "str6");

        return outer;
    }

    /** Tests class with annotated fields. */
    private static class SimpleObjectAnnotatedFields {
        /** */
        @BinaryCompression
        private byte b;

        /** */
        @BinaryCompression
        private short s;

        /** */
        @BinaryCompression
        private int i;

        /** */
        @BinaryCompression
        private long l;

        /** */
        @BinaryCompression
        private float f;

        /** */
        @BinaryCompression
        private double d;

        /** */
        @BinaryCompression
        private char c;

        /** */
        @BinaryCompression
        private boolean bool;

        /** */
        @BinaryCompression
        private String str;

        /** */
        @BinaryCompression
        private UUID uuid;

        /** */
        @BinaryCompression
        private Date date;

        /** */
        @BinaryCompression
        private Timestamp ts;

        /** */
        @BinaryCompression
        private Time time;

        /** */
        @BinaryCompression
        private byte[] bArr;

        /** */
        @BinaryCompression
        private short[] sArr;

        /** */
        @BinaryCompression
        private int[] iArr;

        /** */
        @BinaryCompression
        private long[] lArr;

        /** */
        @BinaryCompression
        private float[] fArr;

        /** */
        @BinaryCompression
        private double[] dArr;

        /** */
        @BinaryCompression
        private char[] cArr;

        /** */
        @BinaryCompression
        private boolean[] boolArr;

        /** */
        @BinaryCompression
        private String[] strArr;

        /** */
        @BinaryCompression
        private UUID[] uuidArr;

        /** */
        @BinaryCompression
        private Date[] dateArr;

        /** */
        @BinaryCompression
        private Time[] timeArr;

        /** */
        @BinaryCompression
        private Object[] objArr;

        /** */
        @BinaryCompression
        private BigDecimal[] bdArr;

        /** */
        @BinaryCompression
        private Collection<String> col;

        /** */
        @BinaryCompression
        private Map<Integer, String> map;

        /** */
        @BinaryCompression
        private TestEnum enumVal;

        /** */
        @BinaryCompression
        private TestEnum[] enumArr;

        /** */
        @BinaryCompression
        private SimpleObjectAnnotatedFields inner;

        /** {@inheritDoc} */
        @SuppressWarnings("FloatingPointEquality")
        @Override public boolean equals(Object other) {
            if (this == other)
                return true;

            if (other == null || getClass() != other.getClass())
                return false;

            SimpleObjectAnnotatedFields obj = (SimpleObjectAnnotatedFields)other;

            return GridTestUtils.deepEquals(this, obj);
        }

        /** {@inheritDoc} */
        @Override public String toString() {
            return S.toString(SimpleObjectAnnotatedFields.class, this);
        }
    }

    /** */
    private enum TestEnum {
        A, B, C, D, E
    }
}