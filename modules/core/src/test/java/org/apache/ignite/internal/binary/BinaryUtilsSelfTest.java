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
    private static final int REPEAT_READS = 10;

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

            assertEquals(object, ((BinaryObject)unmarshBinary).deserialize());
        }

        testReadSameObjects(true);
        testNotSameObjects();

    }
    
    /**
     * Creates necessary objects used in the BinaryUtils test.
     */
    private void createBinaryObject(Ignite ignite, TestObjectHolder obj) {
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
