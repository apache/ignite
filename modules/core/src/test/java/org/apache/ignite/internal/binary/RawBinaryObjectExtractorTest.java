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

import java.lang.reflect.Field;
import java.lang.reflect.InvocationHandler;
import java.lang.reflect.Method;
import java.lang.reflect.Proxy;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import org.apache.ignite.binary.BinaryObject;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.binary.builder.BinaryObjectBuilders;
import org.apache.ignite.internal.binary.mutabletest.GridBinaryTestClasses.TestObjectAllTypes;
import org.apache.ignite.internal.binary.streams.BinaryStreams;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.lang.IgnitePredicate;
import org.apache.ignite.marshaller.MarshallerContext;
import org.apache.ignite.marshaller.jdk.JdkMarshaller;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.junit.Test;

/** */
public class RawBinaryObjectExtractorTest extends GridCommonAbstractTest {
    /** */
    @Test
    public void test() throws Exception {
        BinaryContext ctx = createTestBinaryContext();

        Collection<Object> testObjects = createObjectsOfAllTypes(ctx);
        
        byte[] serializedTestObjectsBytes;

        try (BinaryWriterExImpl writer = new BinaryWriterExImpl(ctx)) {
            testObjects.forEach(writer::writeObject);

            serializedTestObjectsBytes = writer.array();
        }

        RawBinaryObjectExtractor rawReader = new RawBinaryObjectExtractor(BinaryStreams.inputStream(serializedTestObjectsBytes));

        for (Object testObj : testObjects) {
            byte[] objRawBytes = rawReader.extractObject();

            try (BinaryRawReaderEx binReader
                     = new BinaryReaderExImpl(ctx, BinaryStreams.inputStream(objRawBytes), null, false)) {
                Object deserializedObj = binReader.readObject();

                if (testObj instanceof Proxy)
                    assertEquals(String.valueOf(testObj), String.valueOf(deserializedObj));
                else
                    assertEqualsArraysAware(testObj, deserializedObj);

                assertEquals(objRawBytes.length, binReader.in().position());
            }
        }

        assertEquals(serializedTestObjectsBytes.length, rawReader.position());
    }

    /** */
    public static BinaryContext createTestBinaryContext() {
        BinaryContext ctx = new BinaryContext(BinaryCachingMetadataHandler.create(), new IgniteConfiguration(), null);

        BinaryMarshaller marsh = new BinaryMarshaller();

        marsh.setContext(new TestMarshallerContext());

        ctx.configure(marsh);

        return ctx;
    }

    /** */
    private static Collection<Object> createObjectsOfAllTypes(BinaryContext ctx) throws IllegalAccessException {
        Collection<Object> res = new ArrayList<>();

        TestObjectAllTypesEx allTypesObj = new TestObjectAllTypesEx(ctx);

        for (Field field : TestObjectAllTypesEx.class.getFields())
            res.add(field.get(allTypesObj));

        return res;
    }

    /** */
    private Object createTestObject() {
        TestObjectAllTypes res = new TestObjectAllTypes();

        res.setDefaultData();

        return res;
    }

    /** */
    private interface RegisteredClass { }

    /** */
    private interface UnregisteredClass { }

    /** */
    private static class TestMarshallerContext implements MarshallerContext {
        /** */
        Map<Integer, String> clsNamesByTypeId = new HashMap<>();

        /** {@inheritDoc} */
        @Override public boolean registerClassName(
            byte platformId,
            int typeId,
            String clsName
        ) {
            if (Objects.equals(clsName, UnregisteredClass.class.getName()))
                return false;

            clsNamesByTypeId.put(typeId, clsName);

            return true;
        }

        /** {@inheritDoc} */
        @Override public boolean registerClassNameLocally(byte platformId, int typeId, String clsName) {
            return registerClassName(platformId, typeId, clsName);
        }

        /** {@inheritDoc} */
        @Override public Class<?> getClass(int typeId, ClassLoader ldr) throws ClassNotFoundException {
            return U.forName(clsNamesByTypeId.get(typeId), ldr);
        }

        /** {@inheritDoc} */
        @Override public String getClassName(byte platformId, int typeId) {
            return clsNamesByTypeId.get(typeId);
        }

        /** {@inheritDoc} */
        @Override public boolean isSystemType(String typeName) {
            return false;
        }

        /** {@inheritDoc} */
        @Override public IgnitePredicate<String> classNameFilter() {
            return null;
        }

        /** {@inheritDoc} */
        @Override public JdkMarshaller jdkMarshaller() {
            return new JdkMarshaller();
        }
    }

    /** */
    public static class TestInvocationHandler implements InvocationHandler {
        /** */
        private final String proxyClsName;

        /** */
        public TestInvocationHandler(Class<?> proxyCls) {
            proxyClsName = proxyCls.getName();
        }

        /** {@inheritDoc} */
        @Override public Object invoke(Object p, Method method, Object[] methodArgs) throws Throwable {
            if ("toString".equals(method.getName()))
                return proxyClsName;

            throw new IllegalStateException();
        }
    }
    
    /** */
    public static class TestObjectAllTypesEx extends TestObjectAllTypes {
        /** */
        public Object nullObj;

        /** */
        public Object obj;

        /** */
        public Object[] emptyArr;

        /** */
        public Object[] objArr;

        /** */
        public Collection<Object> col;

        /** */
        public Map<Object, Object> map;

        /** */
        public Class<?> registeredClass;

        /** */
        public Class<?> unregisteredClass;

        /** */
        public Object registeredClsProxy;

        /** */
        public Object unregisteredClsProxy;

        /** */
        public BinaryObject binObj;

        /** */
        public TestObjectAllTypesEx(BinaryContext ctx) {
            setDefaultData();

            nullObj = null;
            obj = new TestObjectAllTypes();

            emptyArr = new Object[] {};
            objArr = new Object[] {"test", new TestObjectAllTypes(), new Object[]{new TestObjectAllTypes()}};

            col = new ArrayList<>();

            col.add(0);
            col.add(new TestObjectAllTypes());
            col.add(new ArrayList<>());
            col.add(new HashMap<>());

            map = new HashMap<>();

            map.put(0, 0);
            map.put(1, new TestObjectAllTypes());
            map.put(2, new ArrayList<>());
            map.put(3, new HashMap<>());

            registeredClass = RegisteredClass.class;
            unregisteredClass = UnregisteredClass.class;

            registeredClsProxy = Proxy.newProxyInstance(
                RawBinaryObjectExtractorTest.class.getClassLoader(),
                new Class[] { RegisteredClass.class },
                new TestInvocationHandler(RegisteredClass.class));

            unregisteredClsProxy = Proxy.newProxyInstance(
                RawBinaryObjectExtractorTest.class.getClassLoader(),
                new Class[] { UnregisteredClass.class },
                new TestInvocationHandler(UnregisteredClass.class));

            binObj = BinaryObjectBuilders.createBuilder(ctx, "TestBinaryType").setField("test-field", "test-value").build();
        }
    }
}
