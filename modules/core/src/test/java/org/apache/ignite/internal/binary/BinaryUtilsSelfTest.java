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

import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.stream.Collectors;
import com.google.common.collect.Lists;
import org.apache.ignite.Ignite;
import org.apache.ignite.binary.BinaryObject;
import org.apache.ignite.configuration.BinaryConfiguration;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.binary.streams.BinaryHeapInputStream;
import org.apache.ignite.internal.processors.platform.utils.PlatformUtils;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.junit.Test;

import static org.apache.ignite.cache.CacheMode.REPLICATED;
import static org.junit.Assert.assertArrayEquals;

/**
 * Binary Utils tests.
 */
@SuppressWarnings({"ConstantConditions"})
public class BinaryUtilsSelfTest extends GridCommonAbstractTest {
    /** Object field name. */
    private static final String OBJECT_NAME = "object";

    /** Number of readings of the object. */
    private static final int REPEAT_READS = 1;

    /** Binary object. */
    private BinaryObjectImpl binaryObject;
    
    /** Class loader. */
    private ClassLoader ldr;
    
    /** Binary input stream. */
    private BinaryHeapInputStream in;
    
    /** Binary object reader. */
    private BinaryReaderExImpl binaryReaderEx;
    
    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String igniteInstanceName) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(igniteInstanceName);
        
        CacheConfiguration cacheCfg = new CacheConfiguration(DEFAULT_CACHE_NAME);
        cacheCfg.setCacheMode(REPLICATED);

        cfg.setCacheConfiguration(cacheCfg);
        
        BinaryConfiguration bCfg = new BinaryConfiguration();
        
        bCfg.setClassNames(Arrays.asList("org.apache.ignite.internal.binary.mutabletest.*"));
        
        cfg.setMarshaller(new BinaryMarshaller());
        
        return cfg;
    }
    
    /** {@inheritDoc} */
    @Override protected void beforeTestsStarted() throws Exception {
        startGrids(1);
    }
    
    /** {@inheritDoc} */
    @Override protected void afterTest() throws Exception {
        jcache(0).clear();
    }
    
    /** Test simple class. */
    public static class SimpleObject {
        /** */
        private int i;
        
        /** */
        public SimpleObject(int i) {
            this.i = i;
        }
    
        /** {@inheritDoc} */
        @Override public boolean equals(Object o) {
            if (this == o)
                return true;

            if (o == null || getClass() != o.getClass())
                return false;

            SimpleObject that = (SimpleObject)o;
            return i == that.i;
        }
    
        /** {@inheritDoc} */
        @Override public int hashCode() {
            return Objects.hash(i);
        }
    }
    
    /** Test class with object. */
    public static class TestObjectHolder {
        /** */
        private final Object object;
        
        /** */
        public TestObjectHolder(Object object) {
            this.object = object;
        }
        
        /** */
        public Object getObject() {
            return object;
        }
    }

    /** Test class with object. */
    public static class TestObjectHolderWithArray {
        /** */
        private final SimpleObject object;

        /** */
        private final SimpleObject[] objectArray;

        /** */
        public TestObjectHolderWithArray(SimpleObject object, SimpleObject[] objectArray) {
            this.object = object;
            this.objectArray = objectArray;
        }

        /** */
        public SimpleObject getObject() {
            return object;
        }

        /** */
        public SimpleObject[] getObjectArray() {
            return objectArray;
        }
    }

    /** Test class with object. */
    public static class TestObjectHolder2 {
        /** */
        private final SimpleObject object1;

        /** */
        private final SimpleObject object2;

        /** */
        public TestObjectHolder2(SimpleObject object1, SimpleObject object2) {
            this.object1 = object1;
            this.object2 = object2;
        }

        /** */
        public SimpleObject getObject1() {
            return object1;
        }

        /** */
        public SimpleObject getObject2() {
            return object2;
        }
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testDeserialized2() throws Exception {
        final String objectName1 = "object1";
        final String objectName2 = "object2";

        SimpleObject expObject = new SimpleObject(1453);

        TestObjectHolder2 exp = new TestObjectHolder2(expObject, expObject);

        createBinaryObject(ignite(0), exp);

        assertTrue(binaryReaderEx.findFieldByName(objectName1));

        BinaryObject actObject = (BinaryObject)
                BinaryUtils.unmarshal(in, binaryObject.context(), ldr, binaryReaderEx, false, false);

        assertEquals(expObject, actObject.deserialize());

        assertTrue(binaryReaderEx.findFieldByName(objectName2));

        Object o = BinaryUtils.unmarshal(in, binaryObject.context(), ldr, binaryReaderEx, false, true);

        assertEquals(expObject, o);
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testDeserializedArrayWithHaldle() throws Exception {
        final String objectName = "object";
        final String objectArrayName = "objectArray";

        SimpleObject expObject = new SimpleObject(1453);
        SimpleObject[] expArr = new SimpleObject[] {expObject};

        TestObjectHolderWithArray exp = new TestObjectHolderWithArray(expObject, expArr);

        createBinaryObject(ignite(0), exp);
//
        for (int i = 0; i < REPEAT_READS; i++) {
            assertTrue(binaryReaderEx.findFieldByName(objectName));

            BinaryObject actObject = (BinaryObject)
                    BinaryUtils.unmarshal(in, binaryObject.context(), ldr, binaryReaderEx);

            assertEquals(expObject, actObject.deserialize());

            assertTrue(binaryReaderEx.findFieldByName(objectArrayName));

            Object[] unmarshBinary =
                    (Object[])BinaryUtils.unmarshal(in, binaryObject.context(), ldr, binaryReaderEx);

            assertArrayEquals(expArr,
                    PlatformUtils.unwrapBinariesInArray(unmarshBinary));

//            assertEquals(expObject, PlatformUtils.unwrapBinariesInArray(unmarshBinary)[1]);
        }

//        testReadSameObjects(false);

        for (int i = 0; i < REPEAT_READS; i++) {
            assertTrue(binaryReaderEx.findFieldByName(objectName));

            SimpleObject actObject = (SimpleObject)
                    BinaryUtils.unmarshal(in, binaryObject.context(), ldr, binaryReaderEx, false, true);

            assertEquals(expObject, actObject);

            assertTrue(binaryReaderEx.findFieldByName(objectArrayName));

            SimpleObject[] unmarshBinary =
                    (SimpleObject[])BinaryUtils.unmarshal(in, binaryObject.context(), ldr, binaryReaderEx, false, true);

            assertArrayEquals(expArr, unmarshBinary);

//            assertEquals(expObject, unmarshBinary[1]);
        }

//        testReadSameObjects(true);
//        testNotSameObjects();
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testDeserializedAndUnmrshalledArray() throws Exception {
        SimpleObject[] expArr = new SimpleObject[] {new SimpleObject(Integer.MAX_VALUE), new SimpleObject(Integer.MIN_VALUE)};

        TestObjectHolder array = new TestObjectHolder(expArr);

        createBinaryObject(ignite(0), array);

        for (int i = 0; i < REPEAT_READS; i++) {
            assertTrue(binaryReaderEx.findFieldByName(OBJECT_NAME));

            Object[] unmarshBinary =
                    (Object[])BinaryUtils.unmarshal(in, binaryObject.context(), ldr, binaryReaderEx);

            assertArrayEquals(expArr,
                    PlatformUtils.unwrapBinariesInArray(unmarshBinary));
        }

        testReadSameObjects(false);

        for (int i = 0; i < REPEAT_READS; i++) {
            assertTrue(binaryReaderEx.findFieldByName(OBJECT_NAME));

            Object[] unmarshalRealObj = (Object[])
                    BinaryUtils.unmarshal(in, binaryObject.context(), ldr, binaryReaderEx, false, true);

            assertArrayEquals(expArr, unmarshalRealObj);
        }

        testReadSameObjects(true);
        testNotSameObjects();
    }
    
    /**
     * @throws Exception If failed.
     */
    @Test
    public void testDeserializedAndUnmrshalledCollection() throws Exception {
        List<SimpleObject> expList = Lists.newArrayList(
                new SimpleObject(Integer.MAX_VALUE),
                new SimpleObject(Integer.MIN_VALUE));

        TestObjectHolder obj = new TestObjectHolder(expList);

        createBinaryObject(ignite(0), obj);

        for (int i = 0; i < REPEAT_READS; i++) {
            assertTrue(binaryReaderEx.findFieldByName(OBJECT_NAME));

            List<Object> unmarshBinary =
                    (List<Object>)BinaryUtils.unmarshal(in, binaryObject.context(), ldr, binaryReaderEx);

            List<Object> collect = unmarshBinary.stream()
                    .map(e -> ((BinaryObject)e).deserialize()).collect(Collectors.toList());

            assertEquals(expList, collect);
        }

        testReadSameObjects(false);

        for (int i = 0; i < REPEAT_READS; i++) {
            assertTrue(binaryReaderEx.findFieldByName(OBJECT_NAME));

            List<Object> unmarshalRealObj = (List<Object>)
                    BinaryUtils.unmarshal(in, binaryObject.context(), ldr, binaryReaderEx, false, true);

            assertEquals(expList, unmarshalRealObj);
        }

        testReadSameObjects(true);
        testNotSameObjects();
    }
    
    /**
     * @throws Exception If failed.
     */
    @Test
    public void testDeserializedAndUnmrshalledMap() throws Exception {
        Map<Long, SimpleObject> expMap = new HashMap<>();

        expMap.put(1L, new SimpleObject(Integer.MAX_VALUE));
        expMap.put(-100L, new SimpleObject(Integer.MIN_VALUE));

        TestObjectHolder obj = new TestObjectHolder(expMap);

        createBinaryObject(ignite(0), obj);

        for (int i = 0; i < REPEAT_READS; i++) {
            assertTrue(binaryReaderEx.findFieldByName(OBJECT_NAME));

            Map<Long, Object> unmarshBinary =
                    (Map<Long, Object>)BinaryUtils.unmarshal(in, binaryObject.context(), ldr, binaryReaderEx);

            Map<Long, Object> mappedBinary = unmarshBinary.entrySet().stream().collect(
                    Collectors.toMap(Map.Entry::getKey,
                            e -> ((BinaryObject)e.getValue()).deserialize())
            );

            assertEquals(expMap, mappedBinary);
        }

        testReadSameObjects(false);

        for (int i = 0; i < REPEAT_READS; i++) {
            assertTrue(binaryReaderEx.findFieldByName(OBJECT_NAME));

            Map<Object, Object> unmarshalRealObj = (Map<Object, Object>)
                    BinaryUtils.unmarshal(in, binaryObject.context(), ldr, binaryReaderEx, false, true);

            assertEquals(expMap, unmarshalRealObj);
        }

        testReadSameObjects(true);
        testNotSameObjects();
    }
    
    /**
     * @throws Exception If failed.
     */
    @Test
    public void testDeserializedAndUnmrshalledObject() throws Exception {
        SimpleObject object = new SimpleObject(Integer.MAX_VALUE);
        TestObjectHolder obj = new TestObjectHolder(object);

        createBinaryObject(ignite(0), obj);

        for (int i = 0; i < REPEAT_READS; i++) {
            assertTrue(binaryReaderEx.findFieldByName(OBJECT_NAME));

            Object unmarshBinary =
                    BinaryUtils.unmarshal(in, binaryObject.context(), ldr, binaryReaderEx);

            assertEquals(object, ((BinaryObject)unmarshBinary).deserialize());
        }

        testReadSameObjects(false);

        for (int i = 0; i < REPEAT_READS; i++) {
            assertTrue(binaryReaderEx.findFieldByName(OBJECT_NAME));

            Object unmarshBinary =
                    BinaryUtils.unmarshal(in, binaryObject.context(), ldr, binaryReaderEx, false, true);

            assertEquals(object, unmarshBinary);
        }

        testReadSameObjects(true);
        testNotSameObjects();

    }
    
    /**
     * Creates necessary objects used in the BinaryUtils test.
     */
    private void createBinaryObject(Ignite ignite, Object obj) {
        binaryObject = ignite.binary().toBinary(obj);
        ldr = binaryObject.context().configuration().getClassLoader();
        in = BinaryHeapInputStream.create(binaryObject.array(), binaryObject.start());
        BinaryReaderHandles handles = new BinaryReaderHandles();
        binaryReaderEx = new BinaryReaderExImpl(binaryObject.context(),
                in,
                ldr,
                handles,
                false,
                true);
    }
    
    /**
     * Tests if an object can be handled in BinaryUtils.
     */
    private void testReadSameObjects(boolean deserialized) {
        assertTrue(binaryReaderEx.findFieldByName(OBJECT_NAME));
        
        Object first = BinaryUtils.unmarshal(in, binaryObject.context(), ldr, binaryReaderEx, false, deserialized);
        
        assertTrue(binaryReaderEx.findFieldByName(OBJECT_NAME));
        
        Object second = BinaryUtils.unmarshal(in, binaryObject.context(), ldr, binaryReaderEx, false, deserialized);
        
        assertSame(first, second);
    }
    
    /**
     * Tests that BinaryUtils handles serialized and deserialized objects in different ways.
     */
    private void testNotSameObjects() {
        assertTrue(binaryReaderEx.findFieldByName(OBJECT_NAME));
        
        Object first = BinaryUtils.unmarshal(in, binaryObject.context(), ldr, binaryReaderEx, false, true);
        
        assertTrue(binaryReaderEx.findFieldByName(OBJECT_NAME));
        
        Object second = BinaryUtils.unmarshal(in, binaryObject.context(), ldr, binaryReaderEx, false, false);
        
        assertNotSame(first, second);
    }
}
