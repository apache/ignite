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
import org.apache.ignite.client.ClientCache;
import org.apache.ignite.client.IgniteClient;
import org.apache.ignite.configuration.BinaryConfiguration;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.ClientConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.binary.mutabletest.GridBinaryTestClasses.TestObjectAllTypes;
import org.apache.ignite.testframework.junits.WithSystemProperty;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import static org.apache.ignite.IgniteSystemProperties.IGNITE_BINARY_SORT_OBJECT_FIELDS;

/** */
@RunWith(Parameterized.class)
@WithSystemProperty(key = IGNITE_BINARY_SORT_OBJECT_FIELDS, value = "true")
public class CrossObjetReferenceSerializationTest extends GridCommonAbstractTest {
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

    /** Test parameters. */
    @Parameterized.Parameters(name = "innerObjectType={0}, outerObjectType={1}, isCompactFooterEnabled={2}")
    public static Iterable<Object[]> parameters() {
        List<Object[]> res = new ArrayList<>();

        for (ObjectType innerObjType : ObjectType.values()) {
            for (ObjectType outerObjType : ObjectType.values()) {
                for (boolean isCompactFooterEnabled : new boolean[] {true, false})
                    res.add(new Object[] {innerObjType, outerObjType, isCompactFooterEnabled});
            }
        }

        return res;
    }

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String igniteInstanceName) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(igniteInstanceName)
            .setCacheConfiguration(new CacheConfiguration<>(DEFAULT_CACHE_NAME))
            .setBinaryConfiguration(new BinaryConfiguration().setCompactFooter(isCompactFooterEnabled));

        return cfg;
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
    }

    /** {@inheritDoc} */
    @Override protected void afterTestsStopped() throws Exception {
        super.afterTestsStopped();

        cli.close();
        srv.close();
    }

    /** */
    @Test
    public void testArray() {
        Object outerObj = createTestObject(outerObjType);

        Object[] arr = new Object[] {new Person(createInnerobject(), outerObj), new Person(createInnerobject(), outerObj)};

        checkPutGetRemove(arr, arr);
    }

    /** */
    @Test
    public void testCollection() {
        Object outerObj = createTestObject(outerObjType) ;

        Collection<Object> col = new ArrayList<>();

        col.add(new Person(createInnerobject(), outerObj));
        col.add(new Person(createInnerobject(), outerObj));

        checkPutGetRemove(col, col);
    }

    /** */
    @Test
    public void testMapReferenceBetweenKeyAndValue() {
        Object outerObj = createTestObject(outerObjType);

        Map<Object, Object> map = new HashMap<>();

        map.put(new Person(createInnerobject(), outerObj), new Person(createInnerobject(), outerObj));
        map.put(0, new Person(createInnerobject(), outerObj));

        checkPutGetRemove(map, map);
    }

    /** */
    @Test
    public void testMapReferenceBetweenEntries() {
        Object outerObj = createTestObject(outerObjType);

        Map<Object, Object> map = new HashMap<>();

        map.put(0, new Person(createInnerobject(), outerObj));
        map.put(new Person(createInnerobject(), outerObj), new Person(createInnerobject(), outerObj));

        checkPutGetRemove(map, map);
    }

    /** */
    @Test
    public void testMapCollectionInValue() {
        Object outerObj = createTestObject(outerObjType) ;

        Collection<Object> col = new ArrayList<>();

        col.add(new Person(createInnerobject(), outerObj));
        col.add(new Person(createInnerobject(), outerObj));

        Map<Object, Object> map = new HashMap<>();

        map.put(0, new TestObjectAllTypes());
        map.put(1, col);

        checkPutGetRemove(map, map);
    }

    /** */
    @Test
    public void testMapArrayInValue() {
        Object outerObj = createTestObject(outerObjType) ;

        Map<Object, Object> map = new HashMap<>();

        map.put(0, new Object[] {new Person(createInnerobject(), outerObj), new Person(createInnerobject(), outerObj)});

        checkPutGetRemove(0, map);
    }

    /** */
    private Object createInnerobject() {
        return createTestObject(innerObjType);
    }

    /** */
    private Object createTestObject(ObjectType type) {
        switch (type) {
            case OBJECT: {
                return new TestObjectAllTypes();
            }

            case ARRAY: {
                TestObjectAllTypes obj = new TestObjectAllTypes();

                return new Object[] {obj, obj};
            }

            case COLLECTION: {
                TestObjectAllTypes obj = new TestObjectAllTypes();

                Collection<Object> col = new ArrayList<>();

                col.add(obj);
                col.add(obj);

                return col;
            }

            case MAP: {
                TestObjectAllTypes obj = new TestObjectAllTypes();

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
    private void assertDeepEquals(Object exp, Object actual) {
        if (exp instanceof Map && actual instanceof Map) {
            Map<Object, Object> lhs = (Map<Object, Object>)exp;
            Map<Object, Object> rhs = (Map<Object, Object>)actual;

            assertEquals(lhs.size(), rhs.size());

            lhs.forEach((k, v) -> assertEqualsArraysAware(v, rhs.get(k)));
        }
        else
            assertEqualsArraysAware(exp, actual);
    }

    /** */
    private static int hashCodeArraysAware(Object obj) {
        return obj.getClass().isArray() ? Arrays.deepHashCode((Object[])obj) : Objects.hash(obj);
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
    private static class Person {
        /** */
        private final Object aWrapperOfOuterRefToReplaceWithObj;

        /** */
        private final Object bInnerObj;

        /** */
        private final Object cRefToOuterObjToReplaceWithInnerRef;

        /** */
        private final Object dRefToInnerObjToRecalculate;

        /** */
        public Person(Object innerObj, Object outerObj) {
            aWrapperOfOuterRefToReplaceWithObj = new ComplexWrapper(outerObj);

            bInnerObj = innerObj;

            cRefToOuterObjToReplaceWithInnerRef = outerObj;

            dRefToInnerObjToRecalculate = innerObj;
        }

        /** {@inheritDoc} */
        @Override public boolean equals(Object o) {
            if (this == o)
                return true;

            if (!(o instanceof Person))
                return false;

            Person person = (Person)o;

            return Objects.deepEquals(aWrapperOfOuterRefToReplaceWithObj, person.aWrapperOfOuterRefToReplaceWithObj)
                && Objects.deepEquals(bInnerObj, person.bInnerObj)
                && Objects.deepEquals(cRefToOuterObjToReplaceWithInnerRef, person.cRefToOuterObjToReplaceWithInnerRef)
                && Objects.deepEquals(dRefToInnerObjToRecalculate, person.dRefToInnerObjToRecalculate);
        }

        /** {@inheritDoc} */
        @Override public int hashCode() {
            int res = 1;

            res = 31 * res + hashCodeArraysAware(aWrapperOfOuterRefToReplaceWithObj);
            res = 31 * res + hashCodeArraysAware(bInnerObj);
            res = 31 * res + hashCodeArraysAware(cRefToOuterObjToReplaceWithInnerRef);
            res = 31 * res + hashCodeArraysAware(dRefToInnerObjToRecalculate);

            return res;
        }
    }

    /** */
    private static class ComplexWrapper extends TestObjectAllTypes {
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

            return super.equals(o) && Objects.deepEquals(data, that.data);
        }

        /** {@inheritDoc} */
        @Override public int hashCode() {
            return Objects.hash(super.hashCode(), hashCodeArraysAware(data));
        }
    }
}
