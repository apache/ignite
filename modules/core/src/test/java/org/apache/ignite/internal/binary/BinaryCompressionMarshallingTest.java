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
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.binary.BinaryCachingMetadataHandler;
import org.apache.ignite.internal.binary.BinaryContext;
import org.apache.ignite.internal.binary.BinaryMarshaller;
import org.apache.ignite.internal.binary.BinaryMarshallerSelfTest;
import org.apache.ignite.internal.binary.compression.BinaryCompression;
import org.apache.ignite.internal.binary.compression.CompressionType;
import org.apache.ignite.internal.util.IgniteUtils;
import org.apache.ignite.internal.util.typedef.internal.S;
import org.apache.ignite.logger.NullLogger;
import org.apache.ignite.testframework.GridTestUtils;

/**
 * Tests of data compression.
 */
public class BinaryCompressionMarshallingTest extends BinaryMarshallerSelfTest {
    /** Test string line. */
    String line = "абвгдеёжзийклмнопрстуфхцчшщъыьэюяАБВГДЕЁЖЗИЙКЛМНОПРСТУФХЦЧШЩЪЫЬЭЮЯabcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ";

    /**
     * @throws Exception If failed.
     */
    public void testDefaultCompression() throws Exception {
        IgniteConfiguration igniteConfiguration = getConfiguration(getTestIgniteInstanceName());
        igniteConfiguration.setFullCompressionMode(true);

        BinaryContext ctx = new BinaryContext(BinaryCachingMetadataHandler.create(), igniteConfiguration, new NullLogger());
        BinaryMarshaller marshaller = binaryMarshaller();
        IgniteUtils.invoke(BinaryMarshaller.class, marshaller, "setBinaryContext", ctx, igniteConfiguration);

        assertEquals(line, marshalUnmarshal(line, marshaller));

        byte[] compressed = marshaller.marshal(line);
        assertEquals(line, marshaller.unmarshal(compressed, Thread.currentThread().getContextClassLoader()));
    }

    /**
     * @throws Exception If failed.
     */
    public void testObjectCompression() throws Exception {
        TestsClassAnnotatedStringFields sut = new TestsClassAnnotatedStringFields(line);

        BinaryMarshaller marshaller = binaryMarshaller();

        byte[] sutBytes = marshaller.marshal(sut);
        TestsClassAnnotatedStringFields unmSut = marshaller.unmarshal(sutBytes, null);

        assertEquals(sut, unmSut);
    }

    /**
     * @throws Exception If failed.
     */
    public void testOneFieldClassCompression() throws Exception {
        TestsClassOneField sut = new TestsClassOneField();

        BinaryMarshaller marshaller = binaryMarshaller();

        byte[] sutBytes = marshaller.marshal(sut);
        TestsClassOneField unmSut = marshaller.unmarshal(sutBytes, null);

        assertEquals(sut, unmSut);
    }

    /**
     * @throws Exception If failed.
     */
    public void testAnnotatedCompression() throws Exception {
        SimpleObjectAnnotatedFields sut = simpleAnnotatedObject();

        assertEquals(sut, marshalUnmarshal(sut));
    }

    /**
     * @throws Exception If failed.
     */
    private <T> T marshalUnmarshal(T data) throws Exception {
        BinaryMarshaller marshaller = binaryMarshaller();
        byte[] bytes = marshaller.marshal(data);
        return marshaller.unmarshal(bytes, null);
    }

    /** Test class. */
    private static class TestsClassAnnotatedStringFields {
        @BinaryCompression
        private String data_default;

        @BinaryCompression(type = CompressionType.GZIP)
        private String data_gzip;

        @BinaryCompression(type = CompressionType.DEFLATE)
        private String data_deflate;

        private TestsClassAnnotatedStringFields(String data) {
            this.data_default = data;
            this.data_gzip = data;
            this.data_deflate = data;
        }

        @Override public boolean equals(Object o) {
            if (this == o)
                return true;
            if (o == null || getClass() != o.getClass())
                return false;

            TestsClassAnnotatedStringFields test = (TestsClassAnnotatedStringFields)o;

            if (data_default != null ? !data_default.equals(test.data_default) : test.data_default != null)
                return false;
            if (data_gzip != null ? !data_gzip.equals(test.data_gzip) : test.data_gzip != null)
                return false;
            return data_deflate != null ? data_deflate.equals(test.data_deflate) : test.data_deflate == null;

        }

        @Override public int hashCode() {
            int result = data_default != null ? data_default.hashCode() : 0;
            result = 31 * result + (data_gzip != null ? data_gzip.hashCode() : 0);
            result = 31 * result + (data_deflate != null ? data_deflate.hashCode() : 0);
            return result;
        }
    }

    /** Test class. */
    private static class TestsClassOneField {
        @BinaryCompression
        private String data_default = "abc";

        @Override public boolean equals(Object o) {
            if (this == o)
                return true;
            if (o == null || getClass() != o.getClass())
                return false;

            TestsClassOneField field = (TestsClassOneField)o;

            return data_default != null ? data_default.equals(field.data_default) : field.data_default == null;
        }

        @Override public int hashCode() {
            return data_default != null ? data_default.hashCode() : 0;
        }
    }

    /** Test class. */
    private static class TestsClassOneCollectionField {
        @BinaryCompression
        private Collection<String> collection = new ArrayList<>();

        @Override public boolean equals(Object o) {
            if (this == o)
                return true;
            if (o == null || getClass() != o.getClass())
                return false;

            TestsClassOneCollectionField field = (TestsClassOneCollectionField)o;

            return collection != null ? collection.equals(field.collection) : field.collection == null;
        }

        @Override public int hashCode() {
            return collection != null ? collection.hashCode() : 0;
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
        @BinaryCompression(type = CompressionType.GZIP)
        private short s;

        /** */
        @BinaryCompression(type = CompressionType.DEFLATE)
        private int i;

        /** */
        @BinaryCompression
        private long l;

        /** */
        @BinaryCompression(type = CompressionType.GZIP)
        private float f;

        /** */
        @BinaryCompression(type = CompressionType.DEFLATE)
        private double d;

        /** */
        @BinaryCompression
        private char c;

        /** */
        @BinaryCompression(type = CompressionType.GZIP)
        private boolean bool;

        /** */
        @BinaryCompression(type = CompressionType.DEFLATE)
        private String str;

        /** */
        @BinaryCompression
        private UUID uuid;

        /** */
        @BinaryCompression(type = CompressionType.GZIP)
        private Date date;

        /** */
        @BinaryCompression(type = CompressionType.DEFLATE)
        private Timestamp ts;

        /** */
        @BinaryCompression
        private Time time;

        /** */
        @BinaryCompression(type = CompressionType.GZIP)
        private byte[] bArr;

        /** */
        @BinaryCompression(type = CompressionType.DEFLATE)
        private short[] sArr;

        /** */
        @BinaryCompression
        private int[] iArr;

        /** */
        @BinaryCompression(type = CompressionType.GZIP)
        private long[] lArr;

        /** */
        @BinaryCompression(type = CompressionType.DEFLATE)
        private float[] fArr;

        /** */
        @BinaryCompression
        private double[] dArr;

        /** */
        @BinaryCompression(type = CompressionType.GZIP)
        private char[] cArr;

        /** */
        @BinaryCompression(type = CompressionType.DEFLATE)
        private boolean[] boolArr;

        /** */
        @BinaryCompression
        private String[] strArr;

        /** */
        @BinaryCompression(type = CompressionType.GZIP)
        private UUID[] uuidArr;

        /** */
        @BinaryCompression(type = CompressionType.DEFLATE)
        private Date[] dateArr;

        /** */
        @BinaryCompression
        private Time[] timeArr;

        /** */
        @BinaryCompression(type = CompressionType.GZIP)
        private Object[] objArr;

        /** */
        @BinaryCompression(type = CompressionType.DEFLATE)
        private BigDecimal[] bdArr;

        /** */
        @BinaryCompression
        private Collection<String> col;

        /** */
        @BinaryCompression(type = CompressionType.GZIP)
        private Map<Integer, String> map;

        /** */
        @BinaryCompression(type = CompressionType.DEFLATE)
        private TestEnum enumVal;

        /** */
        @BinaryCompression
        private TestEnum[] enumArr;

        /** */
        @BinaryCompression(type = CompressionType.GZIP)
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