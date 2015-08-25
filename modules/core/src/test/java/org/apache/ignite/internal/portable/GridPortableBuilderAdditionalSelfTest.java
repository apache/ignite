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

package org.apache.ignite.internal.portable;

import org.apache.ignite.*;
import org.apache.ignite.configuration.*;
import org.apache.ignite.internal.portable.mutabletest.*;
import org.apache.ignite.internal.processors.cache.portable.*;
import org.apache.ignite.internal.util.lang.*;
import org.apache.ignite.marshaller.portable.*;
import org.apache.ignite.portable.*;
import org.apache.ignite.testframework.*;
import org.apache.ignite.testframework.junits.common.*;

import com.google.common.collect.*;
import org.junit.*;

import java.lang.reflect.*;
import java.sql.*;
import java.util.*;
import java.util.Date;

import static org.apache.ignite.cache.CacheMode.*;
import static org.apache.ignite.internal.portable.mutabletest.GridPortableTestClasses.*;

/**
 *
 */
public class GridPortableBuilderAdditionalSelfTest extends GridCommonAbstractTest {
    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String gridName) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(gridName);

        CacheConfiguration cacheCfg = new CacheConfiguration();

        cacheCfg.setCacheMode(REPLICATED);

        cfg.setCacheConfiguration(cacheCfg);

        PortableMarshaller marsh = new PortableMarshaller();

        marsh.setClassNames(Arrays.asList("org.apache.ignite.internal.portable.mutabletest.*"));

        marsh.setConvertStringToBytes(useUtf8());

        cfg.setMarshaller(marsh);

        return cfg;
    }

    /** {@inheritDoc} */
    @Override protected void beforeTestsStarted() throws Exception {
        startGrids(1);
    }

    /** {@inheritDoc} */
    @Override protected void afterTestsStopped() throws Exception {
        stopAllGrids();
    }

    /** {@inheritDoc} */
    @Override protected void afterTest() throws Exception {
        jcache(0).clear();
    }

    /**
     * @return Whether to use UTF8 strings.
     */
    protected boolean useUtf8() {
        return true;
    }

    /**
     * @return Portables API.
     */
    protected IgnitePortables portables() {
        return grid(0).portables();
    }

    /**
     * @throws Exception If failed.
     */
    public void testSimpleTypeFieldRead() throws Exception {
        TestObjectAllTypes exp = new TestObjectAllTypes();

        exp.setDefaultData();

        PortableBuilder mutPo = wrap(exp);

        for (Field field : TestObjectAllTypes.class.getDeclaredFields()) {
            Object expVal = field.get(exp);
            Object actVal = mutPo.getField(field.getName());

            switch (field.getName()) {
                case "anEnum":
                    assertEquals(((PortableBuilderEnum)actVal).getOrdinal(), ((Enum)expVal).ordinal());
                    break;

                case "enumArr": {
                    PortableBuilderEnum[] actArr = (PortableBuilderEnum[])actVal;
                    Enum[] expArr = (Enum[])expVal;

                    assertEquals(expArr.length, actArr.length);

                    for (int i = 0; i < actArr.length; i++)
                        assertEquals(expArr[i].ordinal(), actArr[i].getOrdinal());

                    break;
                }

                case "entry":
                    assertEquals(((Map.Entry)expVal).getKey(), ((Map.Entry)actVal).getKey());
                    assertEquals(((Map.Entry)expVal).getValue(), ((Map.Entry)actVal).getValue());
                    break;

                default:
                    assertTrue(field.getName(), Objects.deepEquals(expVal, actVal));
                    break;
            }
        }
    }

    /**
     *
     */
    public void testSimpleTypeFieldSerialize() {
        TestObjectAllTypes exp = new TestObjectAllTypes();

        exp.setDefaultData();

        PortableBuilderImpl mutPo = wrap(exp);

        TestObjectAllTypes res = mutPo.build().deserialize();

        GridTestUtils.deepEquals(exp, res);
    }

    /**
     * @throws Exception If any error occurs.
     */
    public void testSimpleTypeFieldOverride() throws Exception {
        TestObjectAllTypes exp = new TestObjectAllTypes();

        exp.setDefaultData();

        PortableBuilderImpl mutPo = wrap(new TestObjectAllTypes());

        for (Field field : TestObjectAllTypes.class.getDeclaredFields())
            mutPo.setField(field.getName(), field.get(exp));

        TestObjectAllTypes res = mutPo.build().deserialize();

        GridTestUtils.deepEquals(exp, res);
    }

    /**
     * @throws Exception If any error occurs.
     */
    public void testSimpleTypeFieldSetNull() throws Exception {
        TestObjectAllTypes exp = new TestObjectAllTypes();

        exp.setDefaultData();

        PortableBuilderImpl mutPo = wrap(exp);

        for (Field field : TestObjectAllTypes.class.getDeclaredFields()) {
            if (!field.getType().isPrimitive())
                mutPo.setField(field.getName(), null);
        }

        TestObjectAllTypes res = mutPo.build().deserialize();

        for (Field field : TestObjectAllTypes.class.getDeclaredFields()) {
            if (!field.getType().isPrimitive())
                assertNull(field.getName(), field.get(res));
        }
    }

    /**
     * @throws IgniteCheckedException If any error occurs.
     */
    public void testMakeCyclicDependency() throws IgniteCheckedException {
        TestObjectOuter outer = new TestObjectOuter();
        outer.inner = new TestObjectInner();

        PortableBuilderImpl mutOuter = wrap(outer);

        PortableBuilderImpl mutInner = mutOuter.getField("inner");

        mutInner.setField("outer", mutOuter);
        mutInner.setField("foo", mutInner);

        TestObjectOuter res = mutOuter.build().deserialize();

        assertEquals(res, res.inner.outer);
        assertEquals(res.inner, res.inner.foo);
    }

    /**
     *
     */
    public void testSimpleArrayModification() {
        TestObjectAllTypes obj = new TestObjectAllTypes();

        obj.strArr = new String[]{"a", "a", "a"};

        PortableBuilderImpl mutObj = wrap(obj);

        String[] arr = mutObj.getField("strArr");
        arr[0] = "b";

        TestObjectAllTypes res = mutObj.build().deserialize();

        Assert.assertArrayEquals(obj.strArr, res.strArr);
    }

    /**
     *
     */
    public void testModifyObjectArray() {
        fail("https://issues.apache.org/jira/browse/IGNITE-1273");

        TestObjectContainer obj = new TestObjectContainer();
        obj.foo = new Object[]{"a"};

        PortableBuilderImpl mutObj = wrap(obj);

        Object[] arr = mutObj.getField("foo");

        Assert.assertArrayEquals(new Object[]{"a"}, arr);

        arr[0] = "b";

        TestObjectContainer res = mutObj.build().deserialize();

        Assert.assertArrayEquals(new Object[] {"a"}, (Object[])res.foo);
    }

    /**
     *
     */
    public void testOverrideObjectArrayField() {
        PortableBuilderImpl mutObj = wrap(new TestObjectContainer());

        Object[] createdArr = {mutObj, "a", 1, new String[] {"s", "s"}, new byte[]{1, 2}, new UUID(3, 0)};

        mutObj.setField("foo", createdArr.clone());

        TestObjectContainer res = mutObj.build().deserialize();

        createdArr[0] = res;

        assertTrue(Objects.deepEquals(createdArr, res.foo));
    }

    /**
     *
     */
    public void testDeepArray() {
        TestObjectContainer obj = new TestObjectContainer();
        obj.foo = new Object[]{new Object[]{"a", obj}};

        PortableBuilderImpl mutObj = wrap(obj);

        Object[] arr = (Object[])mutObj.<Object[]>getField("foo")[0];

        assertEquals("a", arr[0]);
        assertSame(mutObj, arr[1]);

        arr[0] = mutObj;

        TestObjectContainer res = mutObj.build().deserialize();

        arr = (Object[])((Object[])res.foo)[0];

        assertSame(arr[0], res);
        assertSame(arr[0], arr[1]);
    }

    /**
     *
     */
    public void testArrayListRead() {
        TestObjectContainer obj = new TestObjectContainer();
        obj.foo = Lists.newArrayList(obj, "a");

        PortableBuilderImpl mutObj = wrap(obj);

        List<Object> list = mutObj.getField("foo");

        assert list.equals(Lists.newArrayList(mutObj, "a"));
    }

    /**
     *
     */
    public void testArrayListOverride() {
        TestObjectContainer obj = new TestObjectContainer();

        PortableBuilderImpl mutObj = wrap(obj);

        ArrayList<Object> list = Lists.newArrayList(mutObj, "a", Lists.newArrayList(1, 2));

        mutObj.setField("foo", list);

        TestObjectContainer res = mutObj.build().deserialize();

        list.set(0, res);

        assertNotSame(list, res.foo);
        assertEquals(list, res.foo);
    }

    /**
     *
     */
    public void testArrayListModification() {
        TestObjectContainer obj = new TestObjectContainer();
        obj.foo = Lists.newArrayList("a", "b", "c");

        PortableBuilderImpl mutObj = wrap(obj);

        List<String> list = mutObj.getField("foo");

        list.add("!"); // "a", "b", "c", "!"
        list.add(0, "_"); // "_", "a", "b", "c", "!"

        String s = list.remove(1); // "_", "b", "c", "!"
        assertEquals("a", s);

        assertEquals(Arrays.asList("c", "!"), list.subList(2, 4));
        assertEquals(1, list.indexOf("b"));
        assertEquals(1, list.lastIndexOf("b"));

        TestObjectContainer res = mutObj.build().deserialize();

        assertTrue(res.foo instanceof ArrayList);
        assertEquals(Arrays.asList("_", "b", "c", "!"), res.foo);
    }

    /**
     *
     */
    public void testArrayListClear() {
        TestObjectContainer obj = new TestObjectContainer();
        obj.foo = Lists.newArrayList("a", "b", "c");

        PortableBuilderImpl mutObj = wrap(obj);

        List<String> list = mutObj.getField("foo");

        list.clear();

        assertEquals(Collections.emptyList(), mutObj.build().<TestObjectContainer>deserialize().foo);
    }

    /**
     *
     */
    public void testArrayListWriteUnmodifiable() {
        TestObjectContainer obj = new TestObjectContainer();

        ArrayList<Object> src = Lists.newArrayList(obj, "a", "b", "c");

        obj.foo = src;

        PortableBuilderImpl mutObj = wrap(obj);

        TestObjectContainer deserialized = mutObj.build().deserialize();

        List<Object> res = (List<Object>)deserialized.foo;

        src.set(0, deserialized);

        assertEquals(src, res);
    }

    /**
     *
     */
    public void testLinkedListRead() {
        TestObjectContainer obj = new TestObjectContainer();
        obj.foo = Lists.newLinkedList(Arrays.asList(obj, "a"));

        PortableBuilderImpl mutObj = wrap(obj);

        List<Object> list = mutObj.getField("foo");

        assert list.equals(Lists.newLinkedList(Arrays.asList(mutObj, "a")));
    }

    /**
     *
     */
    public void testLinkedListOverride() {
        TestObjectContainer obj = new TestObjectContainer();

        PortableBuilderImpl mutObj = wrap(obj);

        List<Object> list = Lists.newLinkedList(Arrays.asList(mutObj, "a", Lists.newLinkedList(Arrays.asList(1, 2))));

        mutObj.setField("foo", list);

        TestObjectContainer res = mutObj.build().deserialize();

        list.set(0, res);

        assertNotSame(list, res.foo);
        assertEquals(list, res.foo);
    }

    /**
     *
     */
    public void testLinkedListModification() {
        TestObjectContainer obj = new TestObjectContainer();

        obj.foo = Lists.newLinkedList(Arrays.asList("a", "b", "c"));

        PortableBuilderImpl mutObj = wrap(obj);

        List<String> list = mutObj.getField("foo");

        list.add("!"); // "a", "b", "c", "!"
        list.add(0, "_"); // "_", "a", "b", "c", "!"

        String s = list.remove(1); // "_", "b", "c", "!"
        assertEquals("a", s);

        assertEquals(Arrays.asList("c", "!"), list.subList(2, 4));
        assertEquals(1, list.indexOf("b"));
        assertEquals(1, list.lastIndexOf("b"));

        TestObjectContainer res = mutObj.build().deserialize();

        assertTrue(res.foo instanceof LinkedList);
        assertEquals(Arrays.asList("_", "b", "c", "!"), res.foo);
    }

    /**
     *
     */
    public void testLinkedListWriteUnmodifiable() {
        TestObjectContainer obj = new TestObjectContainer();

        LinkedList<Object> src = Lists.newLinkedList(Arrays.asList(obj, "a", "b", "c"));

        obj.foo = src;

        PortableBuilderImpl mutObj = wrap(obj);

        TestObjectContainer deserialized = mutObj.build().deserialize();

        List<Object> res = (List<Object>)deserialized.foo;

        src.set(0, deserialized);

        assertEquals(src, res);
    }

    /**
     *
     */
    public void testHashSetRead() {
        TestObjectContainer obj = new TestObjectContainer();
        obj.foo = Sets.newHashSet(obj, "a");

        PortableBuilderImpl mutObj = wrap(obj);

        Set<Object> set = mutObj.getField("foo");

        assert set.equals(Sets.newHashSet(mutObj, "a"));
    }

    /**
     *
     */
    public void testHashSetOverride() {
        TestObjectContainer obj = new TestObjectContainer();

        PortableBuilderImpl mutObj = wrap(obj);

        Set<Object> c = Sets.newHashSet(mutObj, "a", Sets.newHashSet(1, 2));

        mutObj.setField("foo", c);

        TestObjectContainer res = mutObj.build().deserialize();

        c.remove(mutObj);
        c.add(res);

        assertNotSame(c, res.foo);
        assertEquals(c, res.foo);
    }

    /**
     *
     */
    public void testHashSetModification() {
        TestObjectContainer obj = new TestObjectContainer();
        obj.foo = Sets.newHashSet("a", "b", "c");

        PortableBuilderImpl mutObj = wrap(obj);

        Set<String> set = mutObj.getField("foo");

        set.remove("b");
        set.add("!");

        assertEquals(Sets.newHashSet("a", "!", "c"), set);
        assertTrue(set.contains("a"));
        assertTrue(set.contains("!"));

        TestObjectContainer res = mutObj.build().deserialize();

        assertTrue(res.foo instanceof HashSet);
        assertEquals(Sets.newHashSet("a", "!", "c"), res.foo);
    }

    /**
     *
     */
    public void testHashSetWriteUnmodifiable() {
        TestObjectContainer obj = new TestObjectContainer();

        Set<Object> src = Sets.newHashSet(obj, "a", "b", "c");

        obj.foo = src;

        TestObjectContainer deserialized = wrap(obj).build().deserialize();

        Set<Object> res = (Set<Object>)deserialized.foo;

        src.remove(obj);
        src.add(deserialized);

        assertEquals(src, res);
    }

    /**
     *
     */
    public void testMapRead() {
        TestObjectContainer obj = new TestObjectContainer();
        obj.foo = Maps.newHashMap(ImmutableMap.of(obj, "a", "b", obj));

        PortableBuilderImpl mutObj = wrap(obj);

        Map<Object, Object> map = mutObj.getField("foo");

        assert map.equals(ImmutableMap.of(mutObj, "a", "b", mutObj));
    }

    /**
     *
     */
    public void testMapOverride() {
        TestObjectContainer obj = new TestObjectContainer();

        PortableBuilderImpl mutObj = wrap(obj);

        Map<Object, Object> map = Maps.newHashMap(ImmutableMap.of(mutObj, "a", "b", mutObj));

        mutObj.setField("foo", map);

        TestObjectContainer res = mutObj.build().deserialize();

        assertEquals(ImmutableMap.of(res, "a", "b", res), res.foo);
    }

    /**
     *
     */
    public void testMapModification() {
        TestObjectContainer obj = new TestObjectContainer();
        obj.foo = Maps.newHashMap(ImmutableMap.of(1, "a", 2, "b"));

        PortableBuilderImpl mutObj = wrap(obj);

        Map<Object, Object> map = mutObj.getField("foo");

        map.put(3, mutObj);
        Object rmv = map.remove(1);

        assertEquals("a", rmv);

        TestObjectContainer res = mutObj.build().deserialize();

        assertEquals(ImmutableMap.of(2, "b", 3, res), res.foo);
    }

    /**
     *
     */
    public void testEnumArrayModification() {
        TestObjectAllTypes obj = new TestObjectAllTypes();

        obj.enumArr = new TestObjectEnum[]{TestObjectEnum.A, TestObjectEnum.B};

        PortableBuilderImpl mutObj = wrap(obj);

        PortableBuilderEnum[] arr = mutObj.getField("enumArr");
        arr[0] = new PortableBuilderEnum(mutObj.typeId(), TestObjectEnum.B);

        TestObjectAllTypes res = mutObj.build().deserialize();

        Assert.assertArrayEquals(new TestObjectEnum[] {TestObjectEnum.A, TestObjectEnum.B}, res.enumArr);
    }

    /**
     *
     */
    public void testEditObjectWithRawData() {
        GridPortableMarshalerAwareTestClass obj = new GridPortableMarshalerAwareTestClass();

        obj.s = "a";
        obj.sRaw = "aa";

        PortableBuilderImpl mutableObj = wrap(obj);

        mutableObj.setField("s", "z");

        GridPortableMarshalerAwareTestClass res = mutableObj.build().deserialize();
        assertEquals("z", res.s);
        assertEquals("aa", res.sRaw);
    }

    /**
     *
     */
    public void testHashCode() {
        TestObjectContainer obj = new TestObjectContainer();

        PortableBuilderImpl mutableObj = wrap(obj);

        assertEquals(obj.hashCode(), mutableObj.build().hashCode());

        mutableObj.hashCode(25);

        assertEquals(25, mutableObj.build().hashCode());
    }

    /**
     *
     */
    public void testCollectionsInCollection() {
        TestObjectContainer obj = new TestObjectContainer();
        obj.foo = Lists.newArrayList(
            Lists.newArrayList(1, 2),
            Lists.newLinkedList(Arrays.asList(1, 2)),
            Sets.newHashSet("a", "b"),
            Sets.newLinkedHashSet(Arrays.asList("a", "b")),
            Maps.newHashMap(ImmutableMap.of(1, "a", 2, "b")));

        TestObjectContainer deserialized = wrap(obj).build().deserialize();

        assertEquals(obj.foo, deserialized.foo);
    }

    /**
     *
     */
    public void testMapEntryModification() {
        TestObjectContainer obj = new TestObjectContainer();
        obj.foo = ImmutableMap.of(1, "a").entrySet().iterator().next();

        PortableBuilderImpl mutableObj = wrap(obj);

        Map.Entry<Object, Object> entry = mutableObj.getField("foo");

        assertEquals(1, entry.getKey());
        assertEquals("a", entry.getValue());

        entry.setValue("b");

        TestObjectContainer res = mutableObj.build().deserialize();

        assertEquals(new GridMapEntry<>(1, "b"), res.foo);
    }

    /**
     *
     */
    public void testMapEntryOverride() {
        TestObjectContainer obj = new TestObjectContainer();

        PortableBuilderImpl mutableObj = wrap(obj);

        mutableObj.setField("foo", new GridMapEntry<>(1, "a"));

        TestObjectContainer res = mutableObj.build().deserialize();

        assertEquals(new GridMapEntry<>(1, "a"), res.foo);
    }

    /**
     *
     */
    public void testMetadataChangingDoublePut() {
        PortableBuilderImpl mutableObj = wrap(new TestObjectContainer());

        mutableObj.setField("xx567", "a");
        mutableObj.setField("xx567", "b");

        mutableObj.build();

        PortableMetadata metadata = portables().metadata(TestObjectContainer.class);

        assertEquals("String", metadata.fieldTypeName("xx567"));
    }

    /**
     *
     */
    public void testMetadataChangingDoublePut2() {
        PortableBuilderImpl mutableObj = wrap(new TestObjectContainer());

        mutableObj.setField("xx567", "a");
        mutableObj.setField("xx567", "b");

        mutableObj.build();

        PortableMetadata metadata = portables().metadata(TestObjectContainer.class);

        assertEquals("String", metadata.fieldTypeName("xx567"));
    }

    /**
     *
     */
    public void testMetadataChanging() {
        TestObjectContainer c = new TestObjectContainer();

        PortableBuilderImpl mutableObj = wrap(c);

        mutableObj.setField("intField", 1);
        mutableObj.setField("intArrField", new int[] {1});
        mutableObj.setField("arrField", new String[] {"1"});
        mutableObj.setField("strField", "1");
        mutableObj.setField("colField", Lists.newArrayList("1"));
        mutableObj.setField("mapField", Maps.newHashMap(ImmutableMap.of(1, "1")));
        mutableObj.setField("enumField", TestObjectEnum.A);
        mutableObj.setField("enumArrField", new Enum[] {TestObjectEnum.A});

        mutableObj.build();

        PortableMetadata metadata = portables().metadata(c.getClass());

        assertTrue(metadata.fields().containsAll(Arrays.asList("intField", "intArrField", "arrField", "strField",
            "colField", "mapField", "enumField", "enumArrField")));

        assertEquals("int", metadata.fieldTypeName("intField"));
        assertEquals("int[]", metadata.fieldTypeName("intArrField"));
        assertEquals("String[]", metadata.fieldTypeName("arrField"));
        assertEquals("String", metadata.fieldTypeName("strField"));
        assertEquals("Collection", metadata.fieldTypeName("colField"));
        assertEquals("Map", metadata.fieldTypeName("mapField"));
        assertEquals("Enum", metadata.fieldTypeName("enumField"));
        assertEquals("Enum[]", metadata.fieldTypeName("enumArrField"));
    }

    /**
     *
     */
    public void testDateInObjectField() {
        TestObjectContainer obj = new TestObjectContainer();

        obj.foo = new Date();

        PortableBuilderImpl mutableObj = wrap(obj);

        assertEquals(Timestamp.class, mutableObj.getField("foo").getClass());
    }

    /**
     *
     */
    public void testDateInCollection() {
        TestObjectContainer obj = new TestObjectContainer();

        obj.foo = Lists.newArrayList(new Date());

        PortableBuilderImpl mutableObj = wrap(obj);

        assertEquals(Timestamp.class, ((List<?>)mutableObj.getField("foo")).get(0).getClass());
    }

    /**
     *
     */
    @SuppressWarnings("AssertEqualsBetweenInconvertibleTypes")
    public void testDateArrayOverride() {
        TestObjectContainer obj = new TestObjectContainer();

        PortableBuilderImpl mutableObj = wrap(obj);

        Date[] arr = {new Date()};

        mutableObj.setField("foo", arr);

        TestObjectContainer res = mutableObj.build().deserialize();

        assertEquals(Date[].class, res.foo.getClass());
        assertTrue(Objects.deepEquals(arr, res.foo));
    }

    /**
     *
     */
    public void testChangeMap() {
        AddressBook addrBook = new AddressBook();

        addrBook.addCompany(new Company(1, "Google inc", 100, new Address("Saint-Petersburg", "Torzhkovskya", 1, 53), "occupation"));
        addrBook.addCompany(new Company(2, "Apple inc", 100, new Address("Saint-Petersburg", "Torzhkovskya", 1, 54), "occupation"));
        addrBook.addCompany(new Company(3, "Microsoft", 100, new Address("Saint-Petersburg", "Torzhkovskya", 1, 55), "occupation"));
        addrBook.addCompany(new Company(4, "Oracle", 100, new Address("Saint-Petersburg", "Nevskiy", 1, 1), "occupation"));

        PortableBuilderImpl mutableObj = wrap(addrBook);

        Map<String, List<PortableBuilderImpl>> map = mutableObj.getField("companyByStreet");

        List<PortableBuilderImpl> list = map.get("Torzhkovskya");

        PortableBuilderImpl company = list.get(0);

        assert "Google inc".equals(company.<String>getField("name"));

        list.remove(0);

        AddressBook res = mutableObj.build().deserialize();

        assertEquals(Arrays.asList("Nevskiy", "Torzhkovskya"), new ArrayList<>(res.getCompanyByStreet().keySet()));

        List<Company> torzhkovskyaCompanies = res.getCompanyByStreet().get("Torzhkovskya");

        assertEquals(2, torzhkovskyaCompanies.size());
        assertEquals("Apple inc", torzhkovskyaCompanies.get(0).name);
    }

    /**
     *
     */
    public void testSavingObjectWithNotZeroStart() {
        TestObjectOuter out = new TestObjectOuter();
        TestObjectInner inner = new TestObjectInner();

        out.inner = inner;
        inner.outer = out;

        PortableBuilderImpl builder = wrap(out);

        PortableBuilderImpl innerBuilder = builder.getField("inner");

        TestObjectInner res = innerBuilder.build().deserialize();

        assertSame(res, res.outer.inner);
    }

    /**
     *
     */
    public void testPortableObjectField() {
        TestObjectContainer container = new TestObjectContainer(toPortable(new TestObjectArrayList()));

        PortableBuilderImpl wrapper = wrap(container);

        assertTrue(wrapper.getField("foo") instanceof PortableObject);

        TestObjectContainer deserialized = wrapper.build().deserialize();
        assertTrue(deserialized.foo instanceof PortableObject);
    }

    /**
     *
     */
    public void testAssignPortableObject() {
        TestObjectContainer container = new TestObjectContainer();

        PortableBuilderImpl wrapper = wrap(container);

        wrapper.setField("foo", toPortable(new TestObjectArrayList()));

        TestObjectContainer deserialized = wrapper.build().deserialize();
        assertTrue(deserialized.foo instanceof TestObjectArrayList);
    }

    /**
     *
     */
    public void testRemoveFromNewObject() {
        PortableBuilderImpl wrapper = newWrapper(TestObjectAllTypes.class);

        wrapper.setField("str", "a");

        wrapper.removeField("str");

        assertNull(wrapper.build().<TestObjectAllTypes>deserialize().str);
    }

    /**
     *
     */
    public void testRemoveFromExistingObject() {
        TestObjectAllTypes obj = new TestObjectAllTypes();
        obj.setDefaultData();

        PortableBuilderImpl wrapper = wrap(toPortable(obj));

        wrapper.removeField("str");

        assertNull(wrapper.build().<TestObjectAllTypes>deserialize().str);
    }

    /**
     *
     */
    public void testCyclicArrays() {
        fail("https://issues.apache.org/jira/browse/IGNITE-1273");

        TestObjectContainer obj = new TestObjectContainer();

        Object[] arr1 = new Object[1];
        Object[] arr2 = new Object[]{arr1};

        arr1[0] = arr2;

        obj.foo = arr1;

        TestObjectContainer res = toPortable(obj).deserialize();

        Object[] resArr = (Object[])res.foo;

        assertSame(((Object[])resArr[0])[0], resArr);
    }

    /**
     *
     */
    @SuppressWarnings("TypeMayBeWeakened")
    public void testCyclicArrayList() {
        fail("https://issues.apache.org/jira/browse/IGNITE-1273");
        TestObjectContainer obj = new TestObjectContainer();

        List<Object> arr1 = new ArrayList<>();
        List<Object> arr2 = new ArrayList<>();

        arr1.add(arr2);
        arr2.add(arr1);

        obj.foo = arr1;

        TestObjectContainer res = toPortable(obj).deserialize();

        List<?> resArr = (List<?>)res.foo;

        assertSame(((List<Object>)resArr.get(0)).get(0), resArr);
    }

    /**
     * @param obj Object.
     * @return Object in portable format.
     */
    private PortableObject toPortable(Object obj) {
        return portables().toPortable(obj);
    }

    /**
     * @param obj Object.
     * @return GridMutablePortableObject.
     */
    private PortableBuilderImpl wrap(Object obj) {
        return PortableBuilderImpl.wrap(toPortable(obj));
    }

    /**
     * @param aCls Class.
     * @return Wrapper.
     */
    private PortableBuilderImpl newWrapper(Class<?> aCls) {
        CacheObjectPortableProcessorImpl processor = (CacheObjectPortableProcessorImpl)(
            (IgnitePortablesImpl)portables()).processor();

        return new PortableBuilderImpl(processor.portableContext(), processor.typeId(aCls.getName()),
            aCls.getSimpleName());
    }
}
