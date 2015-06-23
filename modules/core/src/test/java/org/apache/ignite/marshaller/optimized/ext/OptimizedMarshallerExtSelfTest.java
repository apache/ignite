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

package org.apache.ignite.marshaller.optimized.ext;

import org.apache.ignite.internal.processors.cache.*;
import org.apache.ignite.marshaller.*;
import org.apache.ignite.marshaller.optimized.*;
import org.apache.ignite.testframework.junits.common.*;

import java.util.concurrent.*;

/**
 * Optimized marshaller self test.
 */
@GridCommonTest(group = "Marshaller")
public class OptimizedMarshallerExtSelfTest extends OptimizedMarshallerSelfTest {
    /** */
    private static ConcurrentHashMap<Integer, OptimizedObjectMetadata> META_BUF = new ConcurrentHashMap<>();

    /** */
    private static final OptimizedMarshallerExtMetaHandler META_HANDLER = new OptimizedMarshallerExtMetaHandler() {
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

        // Check enclosed objects fields
        assertTrue(marsh.hasField("i", arr, 0, arr.length));
        assertEquals(testObj.o2.i, (int)marsh.readField("i", arr, 0, arr.length, null));
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
        @Override public void setMetadataHandler(OptimizedMarshallerExtMetaHandler metaHandler) {
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
}
