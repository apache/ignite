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

package org.apache.ignite.marshaller.optimized;

import org.apache.ignite.internal.processors.cache.*;
import org.apache.ignite.marshaller.*;
import org.apache.ignite.testframework.junits.common.*;

import java.io.*;
import java.lang.reflect.*;
import java.util.*;
import java.util.concurrent.*;

/**
 * Optimized marshaller self test.
 */
@GridCommonTest(group = "Marshaller")
public class OptimizedMarshallerExtSelfTest extends OptimizedMarshallerSelfTest {
    /** */
    private static ConcurrentHashMap<Integer, OptimizedObjectMetadata> META_BUF = new ConcurrentHashMap<>();

    /** */
    private static final OptimizedMarshallerMetaHandler META_HANDLER = new OptimizedMarshallerMetaHandler() {
        @Override public void addMeta(int typeId, OptimizedObjectMetadata meta) {
            META_BUF.putIfAbsent(typeId, meta);
        }

        @Override public OptimizedObjectMetadata metadata(int typeId) {
            return META_BUF.get(typeId);
        }
    };

    /** {@inheritDoc} */
    @Override protected Marshaller marshaller() {
        return new InternalMarshaller(false);
    }

    /**
     * @throws Exception In case of error.
     */
    public void testHasField() throws Exception {
        META_BUF.clear();

        OptimizedMarshallerExt marsh = (OptimizedMarshallerExt)OptimizedMarshallerExtSelfTest.marsh;

        assertTrue(marsh.enableFieldsIndexing(TestObject.class));

        assertEquals(1, META_BUF.size());

        TestObject testObj = new TestObject("World", 50);

        byte[] arr = marsh.marshal(testObj);

        assertTrue(marsh.hasField("o2", arr, 0, arr.length));
        assertTrue(marsh.hasField("str", arr, 0, arr.length));

        assertFalse(marsh.hasField("m", arr, 0, arr.length));
    }

    /**
     * @throws Exception In case of error.
     */
    public void testReadField() throws Exception {
        META_BUF.clear();

        OptimizedMarshallerExt marsh = (OptimizedMarshallerExt)OptimizedMarshallerExtSelfTest.marsh;

        assertTrue(marsh.enableFieldsIndexing(TestObject.class));
        assertEquals(1, META_BUF.size());

        TestObject testObj = new TestObject("World", 50);

        byte[] arr = marsh.marshal(testObj);

        // Simple field extraction.

        String text = marsh.readField("str", arr, 0, arr.length, null);

        assertEquals(testObj.str, text);

        // Serializable extraction (doesn't have meta, thus doesn't have footer)
        TestObject2 o2 = marsh.readField("o2", arr, 0, arr.length, null);

        assertEquals(testObj.o2, o2);

        // Add metadata for the enclosed object.
        assertTrue(marsh.enableFieldsIndexing(TestObject2.class));
        assertEquals(2, META_BUF.size());

        arr = marsh.marshal(testObj);

        // Must be returned in a wrapped form, since metadata was added enabling the footer.
        CacheIndexedObjectImpl cacheObject = marsh.readField("o2", arr, 0, arr.length, null);


        arr = cacheObject.valueBytes(null);

        Field start = cacheObject.getClass().getDeclaredField("start");
        start.setAccessible(true);

        Field len = cacheObject.getClass().getDeclaredField("len");
        len.setAccessible(true);

        // Check enclosed objects fields
        assertTrue(marsh.hasField("i", arr, start.getInt(cacheObject), len.getInt(cacheObject)));
        assertEquals(testObj.o2.i, (int)marsh.readField("i", arr, start.getInt(cacheObject), len.getInt(cacheObject),
            null));
    }

    /**
     * @throws Exception In case of error.
     */
    public void testHandles() throws Exception {
        META_BUF.clear();

        OptimizedMarshallerExt marsh = (OptimizedMarshallerExt)OptimizedMarshallerExtSelfTest.marsh;

        assertTrue(marsh.enableFieldsIndexing(SelfLinkObject.class));
        assertEquals(1, META_BUF.size());

        SelfLinkObject selfLinkObject = new SelfLinkObject();
        selfLinkObject.str1 = "Hello, world!";
        selfLinkObject.str2 = selfLinkObject.str1;
        selfLinkObject.link = selfLinkObject;

        byte[] arr = marsh.marshal(selfLinkObject);

        String str2 = marsh.readField("str2", arr, 0, arr.length, null);

        assertEquals(selfLinkObject.str1, str2);

        CacheIndexedObjectImpl cacheObj = marsh.readField("link", arr, 0, arr.length, null);

        arr = cacheObj.valueBytes(null);

        SelfLinkObject selfLinkObject2 = marsh.unmarshal(arr, null);

        assertEquals(selfLinkObject, selfLinkObject2);
    }


    /**
     * @throws Exception In case of error.
     */
    public void testMarshalAware() throws Exception {
        META_BUF.clear();

        OptimizedMarshallerExt marsh = (OptimizedMarshallerExt)OptimizedMarshallerExtSelfTest.marsh;

        assertTrue(marsh.enableFieldsIndexing(TestMarshalAware.class));
        assertTrue(marsh.enableFieldsIndexing(AnotherMarshalAware.class));
        assertEquals(0, META_BUF.size());

        assertTrue(marsh.enableFieldsIndexing(TestObject2.class));
        assertEquals(1, META_BUF.size());

        TestMarshalAware test = new TestMarshalAware(100, "MarshalAware");

        byte[] arr = marsh.marshal(test);

        assertEquals(3, META_BUF.size());

        // Working with fields
        String text = marsh.readField("text", arr, 0, arr.length, null);

        assertEquals(test.text, text);

        CacheIndexedObjectImpl cacheObj = marsh.readField("aware", arr, 0, arr.length, null);
        byte[] cacheObjArr = cacheObj.valueBytes(null);

        Field start = cacheObj.getClass().getDeclaredField("start");
        start.setAccessible(true);

        Field len = cacheObj.getClass().getDeclaredField("len");
        len.setAccessible(true);

        Date date = marsh.readField("date", cacheObjArr, start.getInt(cacheObj), len.getInt(cacheObj), null);

        assertEquals(test.aware.date, date);

        cacheObj = marsh.readField("testObject2", arr, 0, arr.length, null);
        cacheObjArr = cacheObj.valueBytes(null);

        start = cacheObj.getClass().getDeclaredField("start");
        start.setAccessible(true);

        len = cacheObj.getClass().getDeclaredField("len");
        len.setAccessible(true);

        int n = marsh.readField("i", cacheObjArr, start.getInt(cacheObj), len.getInt(cacheObj), null);

        assertEquals(test.testObject2.i, n);

        // Deserializing
        TestMarshalAware test2 = marsh.unmarshal(arr, null);

        assertEquals(test.number, test2.number);
        assertEquals(test.text, test2.text);
        assertEquals(test.testObject2, test2.testObject2);
        assertEquals(test.aware.date, test2.aware.date);
        assertNull(test2.aware.someObject);

        for (int i = 0; i < test.aware.arr.length; i++)
            assertEquals(test.aware.arr[i], test2.aware.arr[i]);
    }

    private static class InternalMarshaller extends OptimizedMarshallerExt {
        /**
         * Constructor.
         */
        public InternalMarshaller() {
        }

        /**
         * Constructor.
         * @param requireSer Requires serialiazable.
         */
        public InternalMarshaller(boolean requireSer) {
            super(requireSer);

            super.setMetadataHandler(META_HANDLER);
        }

        /** {@inheritDoc} */
        @Override public void setMetadataHandler(OptimizedMarshallerMetaHandler metaHandler) {
            // No-op
        }
    }

    /** */
    private static class TestObject2 {
        /** */
        private final int i;

        /**
         * Constructor for TestObject2 instances.
         *
         * @param i Integer value to hold.
         */
        private TestObject2(int i) {
            this.i = i;
        }

        /** {@inheritDoc} */
        @Override public boolean equals(Object o) {
            return i == ((TestObject2)o).i;
        }

        /** {@inheritDoc} */
        @Override public int hashCode() {
            return i;
        }
    }

    /**
     * Static nested class.
     */
    private static class TestObject {
        /** */
        private final TestObject2 o2;

        /** The only meaningful field in the class, used for {@link #equals(Object o)} and {@link #hashCode()}. */
        private final String str;

        /** */
        private TestObject t2;

        /**
         * @param str String to hold.
         * @param i Integer.
         */
        TestObject(String str, int i) {
            this.str = str;

            o2 = new TestObject2(i);
        }

        /**
         * Method for accessing value of the hold string after the object is created.
         *
         * @return Wrapped string.
         */
        public String string() {
            return str;
        }

        /**
         * @return Object held in this wrapped.
         */
        public TestObject2 obj() {
            return o2;
        }

        /** {@inheritDoc} */
        @Override public int hashCode() {
            return 31 * o2.hashCode() + str.hashCode();
        }

        /** {@inheritDoc} */
        @SuppressWarnings("RedundantIfStatement")
        @Override public boolean equals(Object o) {
            if (this == o)
                return true;

            if (o == null || getClass() != o.getClass())
                return false;

            TestObject obj = (TestObject)o;

            if (o2 != null ? !o2.equals(obj.o2) : obj.o2 != null)
                return false;

            if (str != null ? !str.equals(obj.str) : obj.str != null)
                return false;

            return true;
        }
    }

    /**
     *
     */
    private static class SelfLinkObject {
        /** */
        String str1;

        /** */
        String str2;

        /** */
        SelfLinkObject link;

        @Override public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;

            SelfLinkObject that = (SelfLinkObject)o;

            if (str1 != null ? !str1.equals(that.str1) : that.str1 != null) return false;
            if (str2 != null ? !str2.equals(that.str2) : that.str2 != null) return false;

            return true;
        }
    }

    /**
     *
     */
    private static class TestMarshalAware implements OptimizedMarshalAware {
        /** */
        private int number;

        /** */
        private String text;

        /** */
        private AnotherMarshalAware aware;

        /** */
        private TestObject2 testObject2;

        public TestMarshalAware() {
            // No-op
        }

        public TestMarshalAware(int i, String str) {
            this.number = i;
            this.text = str;

            aware = new AnotherMarshalAware(new int[] {10, 11, 23});
            testObject2 = new TestObject2(i * 10);
        }

        /** {@inheritDoc} */
        @Override public void writeFields(OptimizedFieldsWriter writer) throws IOException {
            writer.writeInt("number", number);
            writer.writeString("text", text);
            writer.writeObject("aware", aware);
            writer.writeObject("testObject2", testObject2);
        }

        /** {@inheritDoc} */
        @Override public void readFields(OptimizedFieldsReader reader) throws IOException {
            number = reader.readInt("number");
            text = reader.readString("text");
            aware = reader.readObject("aware");
            testObject2 = reader.readObject("testObject2");
        }
    }

    /**
     *
     */
    private static class AnotherMarshalAware implements OptimizedMarshalAware {
        /** */
        private Date date = new Date();

        /** */
        private Object someObject;

        /** */
        private int[] arr;

        public AnotherMarshalAware() {
            // No-op
        }

        public AnotherMarshalAware(int[] arr) {
            this.arr = arr;
            someObject = new Object();
        }

        @Override public void writeFields(OptimizedFieldsWriter writer) throws IOException {
            writer.writeIntArray("arr", arr);
            writer.writeObject("date", date);
        }

        @Override public void readFields(OptimizedFieldsReader reader) throws IOException {
            // Deliberately reading in reverse order.
            date = reader.readObject("date");
            arr = reader.readIntArray("arr");
        }
    }
}
