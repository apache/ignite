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

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.Ignition;
import org.apache.ignite.binary.BinaryObjectException;
import org.apache.ignite.binary.BinaryRawReader;
import org.apache.ignite.binary.BinaryRawWriter;
import org.apache.ignite.binary.BinaryReader;
import org.apache.ignite.binary.BinaryWriter;
import org.apache.ignite.binary.Binarylizable;
import org.apache.ignite.client.ClientCache;
import org.apache.ignite.client.IgniteClient;
import org.apache.ignite.configuration.BinaryConfiguration;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.ClientConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.binary.mutabletest.GridBinaryTestClasses.TestObjectAllTypes;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

/** */
@RunWith(Parameterized.class)
public class CrossObjectReferenceSerializationTest extends GridCommonAbstractTest {
    /** */
    private static boolean prevFieldsSortedOrderFlag;

    /** */
    private static Ignite srv;

    /** */
    private static IgniteClient cli;

    /** */
    private static IgniteCache<Object, Object> srvCache;

    /** */
    private static ClientCache<Object, Object> cliCache;

    /** */
    @Parameterized.Parameter
    public ObjectType innerObjType;

    /** */
    @Parameterized.Parameter(1)
    public ObjectType outerObjType;

    /** */
    @Parameterized.Parameter(2)
    public boolean isCompactFooterEnabled;

    /** */
    @Parameterized.Parameter(3)
    public SerializationMode serializationMode;

    /** Test parameters. */
    @Parameterized.Parameters(name = "innerObjectType={0}, outerObjectType={1}, isCompactFooterEnabled={2}, serializationMode={3}")
    public static Iterable<Object[]> parameters() {
        List<Object[]> res = new ArrayList<>();

        for (ObjectType innerObjType : ObjectType.values()) {
            for (ObjectType outerObjType : ObjectType.values()) {
                for (boolean isCompactFooterEnabled : new boolean[] {true, false}) {
                    for (SerializationMode serializationMode : SerializationMode.values())
                        res.add(new Object[] {innerObjType, outerObjType, isCompactFooterEnabled, serializationMode});
                }
            }
        }

        return res;
    }

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String igniteInstanceName) throws Exception {
        return super.getConfiguration(igniteInstanceName)
            .setCacheConfiguration(new CacheConfiguration<>(DEFAULT_CACHE_NAME))
            .setBinaryConfiguration(new BinaryConfiguration()
                .setCompactFooter(isCompactFooterEnabled));
    }

    /** {@inheritDoc} */
    @Override protected void beforeTestsStarted() throws Exception {
        super.beforeTestsStarted();

        srv = startGrid(0);
        cli = Ignition.startClient(new ClientConfiguration()
            .setAddresses("127.0.0.1:10800")
            .setBinaryConfiguration(new BinaryConfiguration()
                .setCompactFooter(isCompactFooterEnabled)));

        srvCache = srv.cache(DEFAULT_CACHE_NAME);
        cliCache = cli.cache(DEFAULT_CACHE_NAME);

        prevFieldsSortedOrderFlag = BinaryUtils.FIELDS_SORTED_ORDER;

        BinaryUtils.FIELDS_SORTED_ORDER = true;
    }

    /** {@inheritDoc} */
    @Override protected void afterTestsStopped() throws Exception {
        BinaryUtils.FIELDS_SORTED_ORDER = prevFieldsSortedOrderFlag;

        super.afterTestsStopped();

        cli.close();
        srv.close();
    }

    /** */
    @Test
    public void testArray() {
        Object outerObj = createObject(outerObjType);

        Object[] arr = new Object[] {createReferencesHolder(outerObj), createReferencesHolder(outerObj)};

        checkPutGetRemove(arr, arr);
    }

    /** */
    @Test
    public void testInnerArray() {
        Object outerObj = createObject(outerObjType);

        Object[] innerArr = new Object[] {new TestObject(), createReferencesHolder(outerObj)};

        Object[] arr = new Object[] {createReferencesHolder(outerObj), innerArr};

        checkPutGetRemove(arr, arr);
    }

    /** */
    @Test
    public void testCollection() {
        Object outerObj = createObject(outerObjType);

        Collection<Object> col = new ArrayList<>();

        col.add(createReferencesHolder(outerObj));
        col.add(createReferencesHolder(outerObj));

        checkPutGetRemove(col, col);
    }

    /** */
    @Test
    public void testInnerCollection() {
        Object outerObj = createObject(outerObjType);

        Collection<Object> col = new ArrayList<>();

        Collection<Object> innerCol = new ArrayList<>();

        innerCol.add(new TestObject());
        innerCol.add(createReferencesHolder(outerObj));

        col.add(createReferencesHolder(outerObj));
        col.add(innerCol);

        checkPutGetRemove(col, col);
    }

    /** */
    @Test
    public void testMapReferenceBetweenKeyAndValue() {
        Object outerObj = createObject(outerObjType);

        Map<Object, Object> map = new HashMap<>();

        map.put(createReferencesHolder(outerObj), createReferencesHolder(outerObj));

        checkPutGetRemove(map, map);
    }

    /** */
    @Test
    public void testMapReferenceBetweenEntries() {
        Object outerObj = createObject(outerObjType);

        Map<Object, Object> map = new HashMap<>();

        map.put(0, createReferencesHolder(outerObj));
        map.put(1, createReferencesHolder(outerObj));

        checkPutGetRemove(map, map);
    }

    /** */
    @Test
    public void testMapInnerCollection() {
        Object outerObj = createObject(outerObjType);

        Collection<Object> col = new ArrayList<>();

        col.add(createReferencesHolder(outerObj));
        col.add(createReferencesHolder(outerObj));

        Map<Object, Object> map = new HashMap<>();

        map.put(0, col);

        checkPutGetRemove(map, map);
    }

    /** */
    @Test
    public void testMapInnerArray() {
        Object outerObj = createObject(outerObjType);

        Map<Object, Object> map = new HashMap<>();

        map.put(0, new Object[] {createReferencesHolder(outerObj), createReferencesHolder(outerObj)});

        checkPutGetRemove(0, map);
    }

    /** */
    @Test
    public void testConsecutiveCrossObjectReferences() {
        Object outerObj = createObject(outerObjType);

        Object holder = createReferencesHolder(outerObj);

        Object enclosingHolder = createReferencesHolder(holder);

        Object doubleEnclosingHolder = createReferencesHolder(enclosingHolder);

        Object[] arr = new Object[] {createReferencesHolder(outerObj), holder, enclosingHolder, doubleEnclosingHolder};

        checkPutGetRemove(arr, arr);
    }

    /** */
    @Test
    public void testMultipleCrossObjectReferences() {
        Object firstOuterObj = createObject(outerObjType);
        Object secondOuterObj = createObject(outerObjType);

        Object[] arr = new Object[] {
            createReferencesHolder(new Object[] {firstOuterObj, secondOuterObj}),
            createReferencesHolder(new Object[] {firstOuterObj, secondOuterObj})
        };

        checkPutGetRemove(arr, arr);
    }

    /** */
    private Object createReferencesHolder(Object outerObj) {
        switch (serializationMode) {
            case RAW:
                return new RawObject(createObject(innerObjType), outerObj);
            case MIXED:
                return new MixedObject(createObject(innerObjType), outerObj);
            case SCHEMA:
                return new SchemaObject(createObject(innerObjType), outerObj);
            default:
                throw new IllegalStateException();
        }
    }

    /** */
    private Object createObject(ObjectType type) {
        switch (type) {
            case OBJECT: {
                return new TestObject();
            }

            case ARRAY: {
                TestObjectAllTypes obj = new TestObject();

                return new Object[] {obj, obj};
            }

            case COLLECTION: {
                TestObjectAllTypes obj = new TestObject();

                Collection<Object> col = new ArrayList<>();

                col.add(obj);
                col.add(obj);

                return col;
            }

            case MAP: {
                TestObjectAllTypes obj = new TestObject();

                Map<Object, Object> map = new HashMap<>();

                map.put(0, obj);
                map.put(1, obj);

                return map;
            }

            default:
                throw new IllegalStateException();
        }
    }

    /** */
    private void checkPutGetRemove(Object key, Object val) {
        srvCache.put(key, val);

        assertDeepEquals(val, srvCache.get(key));
        assertDeepEquals(val, cliCache.get(key));

        srvCache.remove(key);

        assertNull(srvCache.get(key));
        assertNull(cliCache.get(key));

        cliCache.put(key, val);

        assertDeepEquals(val, cliCache.get(key));
        assertDeepEquals(val, srvCache.get(key));

        cliCache.remove(key);

        assertNull(srvCache.get(key));
        assertNull(cliCache.get(key));
    }

    /** */
    private static void assertDeepEquals(Object exp, Object actual) {
        assertTrue(deepEquals(exp, actual));
    }

    /** */
    private static boolean deepEquals(Object lhs, Object rhs) {
        if (lhs instanceof Map && rhs instanceof Map) {
            Map<Object, Object> lhsMap = (Map<Object, Object>)lhs;
            Map<Object, Object> rhsMap = (Map<Object, Object>)rhs;

            assertEquals(lhsMap.size(), rhsMap.size());

            return lhsMap.entrySet().stream().allMatch(e -> deepEquals(e.getValue(), rhsMap.get(e.getKey())));
        }
        else if (lhs instanceof List && rhs instanceof List) {
            List<Object> lhsList = (List<Object>)lhs;
            List<Object> rhsList = (List<Object>)rhs;

            assertEquals(lhsList.size(), rhsList.size());

            boolean res = true;

            for (int i = 0; i < lhsList.size(); i++) {
                if (!deepEquals(lhsList.get(i), rhsList.get(i))) {
                    res = false;

                    break;
                }
            }

            return res;
        }
        else
            return Objects.deepEquals(lhs, rhs);
    }

    /** */
    private static int hashCodeArraysAware(Object obj) {
        return obj != null && obj.getClass().isArray() ? Arrays.deepHashCode((Object[])obj) : Objects.hash(obj);
    }

    /** */
    public enum ObjectType {
        /** */
        OBJECT,

        /** */
        ARRAY,

        /** */
        COLLECTION,

        /** */
        MAP
    }

    /** */
    public enum SerializationMode {
        /** */
        RAW,

        /** */
        MIXED,

        /** */
        SCHEMA
    }

    /** */
    private static class MixedObject extends SchemaObject implements Binarylizable {
        /** */
        public MixedObject() {
            // No-op.
        }

        /** */
        public MixedObject(Object innerObj, Object outerObj) {
            super(innerObj, outerObj);
        }

        /** {@inheritDoc} */
        @Override public void writeBinary(BinaryWriter writer) throws BinaryObjectException {
            writer.writeObject("aWrapperOfOuterRefToReplaceWithObj", aWrapperOfOuterRefToReplaceWithObj);
            writer.writeObject("bInnerObj", bInnerObj);
            writer.writeObject("cRefToOuterObjToReplaceWithInnerRef", cRefToOuterObjToReplaceWithInnerRef);
            writer.writeObject("dRefToInnerObjToRecalculate", dRefToInnerObjToRecalculate);

            BinaryRawWriter rawWriter = writer.rawWriter();

            rawWriter.writeByte(eInnerBytePrimitive);
        }

        /** {@inheritDoc} */
        @Override public void readBinary(BinaryReader reader) throws BinaryObjectException {
            aWrapperOfOuterRefToReplaceWithObj = reader.readObject("aWrapperOfOuterRefToReplaceWithObj");
            bInnerObj = reader.readObject("bInnerObj");
            cRefToOuterObjToReplaceWithInnerRef = reader.readObject("cRefToOuterObjToReplaceWithInnerRef");
            dRefToInnerObjToRecalculate = reader.readObject("dRefToInnerObjToRecalculate");

            BinaryRawReader rawReader = reader.rawReader();

            eInnerBytePrimitive = rawReader.readByte();
        }
    }

    /** */
    private static class RawObject extends SchemaObject implements Binarylizable {
        /** */
        public RawObject() {
            // No-op.
        }
        
        /** */
        public RawObject(Object innerObj, Object outerObj) {
            super(innerObj, outerObj);

            // Reference recalculation for objects written with Raw Binary Writer currently is not supported.
            aWrapperOfOuterRefToReplaceWithObj = null;
            cRefToOuterObjToReplaceWithInnerRef = null;
        }

        /** {@inheritDoc} */
        @Override public void writeBinary(BinaryWriter writer) throws BinaryObjectException {
            BinaryRawWriter rawWriter = writer.rawWriter();
            
            rawWriter.writeObject(aWrapperOfOuterRefToReplaceWithObj);
            rawWriter.writeObject(bInnerObj);
            rawWriter.writeObject(cRefToOuterObjToReplaceWithInnerRef);
            rawWriter.writeObject(dRefToInnerObjToRecalculate);
            rawWriter.writeByte(eInnerBytePrimitive);
        }

        /** {@inheritDoc} */
        @Override public void readBinary(BinaryReader reader) throws BinaryObjectException {
            BinaryRawReader rawReader = reader.rawReader();
            
            aWrapperOfOuterRefToReplaceWithObj = rawReader.readObject();
            bInnerObj = rawReader.readObject();
            cRefToOuterObjToReplaceWithInnerRef = rawReader.readObject();
            dRefToInnerObjToRecalculate = rawReader.readObject();
            eInnerBytePrimitive = rawReader.readByte();
        }
    }

    /** */
    private static class SchemaObject {
        /** */
        protected Object aWrapperOfOuterRefToReplaceWithObj;

        /** */
        protected Object bInnerObj;

        /** */
        protected Object cRefToOuterObjToReplaceWithInnerRef;

        /** */
        protected Object dRefToInnerObjToRecalculate;

        /** */
        protected byte eInnerBytePrimitive;

        /** */
        public SchemaObject() {
            // No-op.
        }

        /** */
        public SchemaObject(Object innerObj, Object outerObj) {
            aWrapperOfOuterRefToReplaceWithObj = new ComplexWrapper(outerObj);

            bInnerObj = innerObj;

            cRefToOuterObjToReplaceWithInnerRef = outerObj;

            dRefToInnerObjToRecalculate = innerObj;

            eInnerBytePrimitive = 127;
        }

        /** {@inheritDoc} */
        @Override public boolean equals(Object o) {
            if (this == o)
                return true;

            if (!(o instanceof SchemaObject))
                return false;

            SchemaObject that = (SchemaObject)o;

            return deepEquals(aWrapperOfOuterRefToReplaceWithObj, that.aWrapperOfOuterRefToReplaceWithObj)
                && deepEquals(bInnerObj, that.bInnerObj)
                && deepEquals(cRefToOuterObjToReplaceWithInnerRef, that.cRefToOuterObjToReplaceWithInnerRef)
                && deepEquals(dRefToInnerObjToRecalculate, that.dRefToInnerObjToRecalculate)
                && eInnerBytePrimitive == that.eInnerBytePrimitive;
        }

        /** {@inheritDoc} */
        @Override public int hashCode() {
            int res = 1;

            res = 31 * res + hashCodeArraysAware(aWrapperOfOuterRefToReplaceWithObj);
            res = 31 * res + hashCodeArraysAware(bInnerObj);
            res = 31 * res + hashCodeArraysAware(cRefToOuterObjToReplaceWithInnerRef);
            res = 31 * res + hashCodeArraysAware(dRefToInnerObjToRecalculate);
            res = 31 * res + hashCodeArraysAware(eInnerBytePrimitive);

            return res;
        }
    }
    
    /** */
    private static class TestObject extends TestObjectAllTypes {
        /** */
        public TestObject() {
            setDefaultData();
        }
    }

    /** */
    public static class ComplexWrapper extends TestObject {
        /** */
        private final Object data;

        /** */
        public ComplexWrapper(Object data) {
            this.data = data;
        }

        /** {@inheritDoc} */
        @Override public boolean equals(Object o) {
            if (this == o)
                return true;

            if (!(o instanceof ComplexWrapper))
                return false;

            ComplexWrapper that = (ComplexWrapper)o;

            return super.equals(o) && deepEquals(data, that.data);
        }

        /** {@inheritDoc} */
        @Override public int hashCode() {
            return Objects.hash(super.hashCode(), hashCodeArraysAware(data));
        }
    }
}
