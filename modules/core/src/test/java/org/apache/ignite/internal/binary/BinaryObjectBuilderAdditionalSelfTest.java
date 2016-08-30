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

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import java.lang.reflect.Field;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.sql.Timestamp;
import java.util.AbstractMap;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.UUID;
import junit.framework.TestCase;
import org.apache.ignite.IgniteBinary;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.binary.BinaryObject;
import org.apache.ignite.binary.BinaryObjectBuilder;
import org.apache.ignite.binary.BinaryObjectException;
import org.apache.ignite.binary.BinaryType;
import org.apache.ignite.configuration.BinaryConfiguration;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.binary.builder.BinaryBuilderEnum;
import org.apache.ignite.internal.binary.builder.BinaryObjectBuilderImpl;
import org.apache.ignite.internal.binary.mutabletest.GridBinaryMarshalerAwareTestClass;
import org.apache.ignite.internal.binary.mutabletest.GridBinaryTestClasses;
import org.apache.ignite.internal.processors.cache.binary.CacheObjectBinaryProcessorImpl;
import org.apache.ignite.internal.processors.cache.binary.IgniteBinaryImpl;
import org.apache.ignite.internal.util.lang.GridMapEntry;
import org.apache.ignite.testframework.GridTestUtils;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.junit.Assert;

import static org.apache.ignite.cache.CacheMode.PARTITIONED;
import static org.apache.ignite.cache.CacheMode.REPLICATED;

/**
 *
 */
public class BinaryObjectBuilderAdditionalSelfTest extends GridCommonAbstractTest {
    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String gridName) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(gridName);

        CacheConfiguration cacheCfg = new CacheConfiguration();
        cacheCfg.setCacheMode(REPLICATED);

        CacheConfiguration cacheCfg2 = new CacheConfiguration("partitioned");
        cacheCfg2.setCacheMode(PARTITIONED);

        cfg.setCacheConfiguration(cacheCfg, cacheCfg2);

        BinaryConfiguration bCfg = new BinaryConfiguration();

        bCfg.setCompactFooter(compactFooter());

        bCfg.setClassNames(Arrays.asList("org.apache.ignite.internal.binary.mutabletest.*"));

        cfg.setMarshaller(new BinaryMarshaller());

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
     * @return Compact footer.
     */
    protected boolean compactFooter() {
        return true;
    }

    /**
     * @return Binaries API.
     */
    protected IgniteBinary binaries() {
        return grid(0).binary();
    }

    /**
     * @throws Exception If failed.
     */
    public void testSimpleTypeFieldRead() throws Exception {
        GridBinaryTestClasses.TestObjectAllTypes exp = new GridBinaryTestClasses.TestObjectAllTypes();

        exp.setDefaultData();

        BinaryObjectBuilder mutPo = wrap(exp);

        for (Field field : GridBinaryTestClasses.TestObjectAllTypes.class.getDeclaredFields()) {
            Object expVal = field.get(exp);
            Object actVal = mutPo.getField(field.getName());

            switch (field.getName()) {
                case "anEnum":
                    assertEquals(((BinaryBuilderEnum)actVal).getOrdinal(), ((Enum)expVal).ordinal());
                    break;

                case "enumArr": {
                    BinaryBuilderEnum[] actArr = (BinaryBuilderEnum[])actVal;
                    Enum[] expArr = (Enum[])expVal;

                    assertEquals(expArr.length, actArr.length);

                    for (int i = 0; i < actArr.length; i++)
                        assertEquals(expArr[i].ordinal(), actArr[i].getOrdinal());

                    break;
                }
            }
        }
    }

    /**
     *
     */
    public void testSimpleTypeFieldSerialize() {
        GridBinaryTestClasses.TestObjectAllTypes exp = new GridBinaryTestClasses.TestObjectAllTypes();

        exp.setDefaultData();

        BinaryObjectBuilderImpl mutPo = wrap(exp);

        GridBinaryTestClasses.TestObjectAllTypes res = mutPo.build().deserialize();

        GridTestUtils.deepEquals(exp, res);
    }

    /**
     * @throws Exception If any error occurs.
     */
    public void testSimpleTypeFieldOverride() throws Exception {
        GridBinaryTestClasses.TestObjectAllTypes exp = new GridBinaryTestClasses.TestObjectAllTypes();

        exp.setDefaultData();

        BinaryObjectBuilderImpl mutPo = wrap(new GridBinaryTestClasses.TestObjectAllTypes());

        for (Field field : GridBinaryTestClasses.TestObjectAllTypes.class.getDeclaredFields())
            mutPo.setField(field.getName(), field.get(exp));

        GridBinaryTestClasses.TestObjectAllTypes res = mutPo.build().deserialize();

        GridTestUtils.deepEquals(exp, res);
    }

    /**
     * @throws Exception If any error occurs.
     */
    public void testSimpleTypeFieldSetNull() throws Exception {
        GridBinaryTestClasses.TestObjectAllTypes exp = new GridBinaryTestClasses.TestObjectAllTypes();

        exp.setDefaultData();

        BinaryObjectBuilderImpl mutPo = wrap(exp);

        for (Field field : GridBinaryTestClasses.TestObjectAllTypes.class.getDeclaredFields()) {
            if (!field.getType().isPrimitive())
                mutPo.setField(field.getName(), null);
        }

        GridBinaryTestClasses.TestObjectAllTypes res = mutPo.build().deserialize();

        for (Field field : GridBinaryTestClasses.TestObjectAllTypes.class.getDeclaredFields()) {
            if (!field.getType().isPrimitive())
                assertNull(field.getName(), field.get(res));
        }
    }

    /**
     * @throws IgniteCheckedException If any error occurs.
     */
    public void testMakeCyclicDependency() throws IgniteCheckedException {
        GridBinaryTestClasses.TestObjectOuter outer = new GridBinaryTestClasses.TestObjectOuter();
        outer.inner = new GridBinaryTestClasses.TestObjectInner();

        BinaryObjectBuilderImpl mutOuter = wrap(outer);

        BinaryObjectBuilderImpl mutInner = mutOuter.getField("inner");

        mutInner.setField("outer", mutOuter);
        mutInner.setField("foo", mutInner);

        GridBinaryTestClasses.TestObjectOuter res = mutOuter.build().deserialize();

        assertEquals(res, res.inner.outer);
        assertEquals(res.inner, res.inner.foo);
    }

    /**
     *
     */
    public void testDateArrayModification() {
        GridBinaryTestClasses.TestObjectAllTypes obj = new GridBinaryTestClasses.TestObjectAllTypes();

        obj.dateArr = new Date[] {new Date(11111), new Date(11111), new Date(11111)};

        BinaryObjectBuilderImpl mutObj = wrap(obj);

        Date[] arr = mutObj.getField("dateArr");
        arr[0] = new Date(22222);

        GridBinaryTestClasses.TestObjectAllTypes res = mutObj.build().deserialize();

        Assert.assertArrayEquals(new Date[] {new Date(22222), new Date(11111), new Date(11111)}, res.dateArr);
    }

    /**
     *
     */
    public void testTimestampArrayModification() {
        GridBinaryTestClasses.TestObjectAllTypes obj = new GridBinaryTestClasses.TestObjectAllTypes();

        obj.tsArr = new Timestamp[] {new Timestamp(111222333), new Timestamp(222333444)};

        BinaryObjectBuilderImpl mutObj = wrap(obj);

        Timestamp[] arr = mutObj.getField("tsArr");
        arr[0] = new Timestamp(333444555);

        GridBinaryTestClasses.TestObjectAllTypes res = mutObj.build().deserialize();

        Assert.assertArrayEquals(new Timestamp[] {new Timestamp(333444555), new Timestamp(222333444)}, res.tsArr);
    }

    /**
     *
     */
    public void testUUIDArrayModification() {
        GridBinaryTestClasses.TestObjectAllTypes obj = new GridBinaryTestClasses.TestObjectAllTypes();

        obj.uuidArr = new UUID[] {new UUID(1, 1), new UUID(1, 1), new UUID(1, 1)};

        BinaryObjectBuilderImpl mutObj = wrap(obj);

        UUID[] arr = mutObj.getField("uuidArr");
        arr[0] = new UUID(2, 2);

        GridBinaryTestClasses.TestObjectAllTypes res = mutObj.build().deserialize();

        Assert.assertArrayEquals(new UUID[] {new UUID(2, 2), new UUID(1, 1), new UUID(1, 1)}, res.uuidArr);
    }

    /**
     *
     */
    public void testDecimalArrayModification() {
        GridBinaryTestClasses.TestObjectAllTypes obj = new GridBinaryTestClasses.TestObjectAllTypes();

        obj.bdArr = new BigDecimal[] {new BigDecimal(1000), new BigDecimal(1000), new BigDecimal(1000)};

        BinaryObjectBuilderImpl mutObj = wrap(obj);

        BigDecimal[] arr = mutObj.getField("bdArr");
        arr[0] = new BigDecimal(2000);

        GridBinaryTestClasses.TestObjectAllTypes res = mutObj.build().deserialize();

        Assert.assertArrayEquals(new BigDecimal[] {new BigDecimal(1000), new BigDecimal(1000), new BigDecimal(1000)},
            res.bdArr);
    }

    /**
     *
     */
    public void testBooleanArrayModification() {
        GridBinaryTestClasses.TestObjectAllTypes obj = new GridBinaryTestClasses.TestObjectAllTypes();

        obj.zArr = new boolean[] {false, false, false};

        BinaryObjectBuilderImpl mutObj = wrap(obj);

        boolean[] arr = mutObj.getField("zArr");
        arr[0] = true;

        GridBinaryTestClasses.TestObjectAllTypes res = mutObj.build().deserialize();

        boolean[] expected = new boolean[] {true, false, false};

        assertEquals(expected.length, res.zArr.length);

        for (int i = 0; i < expected.length; i++)
            assertEquals(expected[i], res.zArr[i]);
    }

    /**
     *
     */
    public void testCharArrayModification() {
        GridBinaryTestClasses.TestObjectAllTypes obj = new GridBinaryTestClasses.TestObjectAllTypes();

        obj.cArr = new char[] {'a', 'a', 'a'};

        BinaryObjectBuilderImpl mutObj = wrap(obj);

        char[] arr = mutObj.getField("cArr");
        arr[0] = 'b';

        GridBinaryTestClasses.TestObjectAllTypes res = mutObj.build().deserialize();

        Assert.assertArrayEquals(new char[] {'b', 'a', 'a'}, res.cArr);
    }

    /**
     *
     */
    public void testDoubleArrayModification() {
        GridBinaryTestClasses.TestObjectAllTypes obj = new GridBinaryTestClasses.TestObjectAllTypes();

        obj.dArr = new double[] {1.0, 1.0, 1.0};

        BinaryObjectBuilderImpl mutObj = wrap(obj);

        double[] arr = mutObj.getField("dArr");
        arr[0] = 2.0;

        GridBinaryTestClasses.TestObjectAllTypes res = mutObj.build().deserialize();

        Assert.assertArrayEquals(new double[] {2.0, 1.0, 1.0}, res.dArr, 0);
    }

    /**
     *
     */
    public void testFloatArrayModification() {
        GridBinaryTestClasses.TestObjectAllTypes obj = new GridBinaryTestClasses.TestObjectAllTypes();

        obj.fArr = new float[] {1.0f, 1.0f, 1.0f};

        BinaryObjectBuilderImpl mutObj = wrap(obj);

        float[] arr = mutObj.getField("fArr");
        arr[0] = 2.0f;

        BinaryObject resBinary = mutObj.build();

        GridBinaryTestClasses.TestObjectAllTypes res = resBinary.deserialize();

        Assert.assertArrayEquals(new float[] {2.0f, 1.0f, 1.0f}, res.fArr, 0);
    }

    /**
     *
     */
    public void testLongArrayModification() {
        GridBinaryTestClasses.TestObjectAllTypes obj = new GridBinaryTestClasses.TestObjectAllTypes();

        obj.lArr = new long[] {1, 1, 1};

        BinaryObjectBuilderImpl mutObj = wrap(obj);

        long[] arr = mutObj.getField("lArr");
        arr[0] = 2;

        GridBinaryTestClasses.TestObjectAllTypes res = mutObj.build().deserialize();

        Assert.assertArrayEquals(new long[] {2, 1, 1}, res.lArr);
    }

    /**
     *
     */
    public void testIntArrayModification() {
        GridBinaryTestClasses.TestObjectAllTypes obj = new GridBinaryTestClasses.TestObjectAllTypes();

        obj.iArr = new int[] {1, 1, 1};

        BinaryObjectBuilderImpl mutObj = wrap(obj);

        int[] arr = mutObj.getField("iArr");
        arr[0] = 2;

        GridBinaryTestClasses.TestObjectAllTypes res = mutObj.build().deserialize();

        Assert.assertArrayEquals(new int[] {2, 1, 1}, res.iArr);
    }

    /**
     *
     */
    public void testShortArrayModification() {
        GridBinaryTestClasses.TestObjectAllTypes obj = new GridBinaryTestClasses.TestObjectAllTypes();

        obj.sArr = new short[] {1, 1, 1};

        BinaryObjectBuilderImpl mutObj = wrap(obj);

        short[] arr = mutObj.getField("sArr");
        arr[0] = 2;

        GridBinaryTestClasses.TestObjectAllTypes res = mutObj.build().deserialize();

        Assert.assertArrayEquals(new short[] {2, 1, 1}, res.sArr);
    }

    /**
     *
     */
    public void testByteArrayModification() {
        GridBinaryTestClasses.TestObjectAllTypes obj = new GridBinaryTestClasses.TestObjectAllTypes();

        obj.bArr = new byte[] {1, 1, 1};

        BinaryObjectBuilderImpl mutObj = wrap(obj);

        byte[] arr = mutObj.getField("bArr");
        arr[0] = 2;

        GridBinaryTestClasses.TestObjectAllTypes res = mutObj.build().deserialize();

        Assert.assertArrayEquals(new byte[] {2, 1, 1}, res.bArr);
    }

    /**
     *
     */
    public void testStringArrayModification() {
        GridBinaryTestClasses.TestObjectAllTypes obj = new GridBinaryTestClasses.TestObjectAllTypes();

        obj.strArr = new String[] {"a", "a", "a"};

        BinaryObjectBuilderImpl mutObj = wrap(obj);

        String[] arr = mutObj.getField("strArr");
        arr[0] = "b";

        GridBinaryTestClasses.TestObjectAllTypes res = mutObj.build().deserialize();

        Assert.assertArrayEquals(new String[] {"b", "a", "a"}, res.strArr);
    }

    /**
     *
     */
    public void testModifyObjectArray() {
        GridBinaryTestClasses.TestObjectContainer obj = new GridBinaryTestClasses.TestObjectContainer();
        obj.foo = new Object[] {"a"};

        BinaryObjectBuilderImpl mutObj = wrap(obj);

        Object[] arr = mutObj.getField("foo");

        Assert.assertArrayEquals(new Object[] {"a"}, arr);

        arr[0] = "b";

        GridBinaryTestClasses.TestObjectContainer res = mutObj.build().deserialize();

        Assert.assertArrayEquals(new Object[] {"b"}, (Object[])res.foo);
    }

    /**
     *
     */
    public void testOverrideObjectArrayField() {
        BinaryObjectBuilderImpl mutObj = wrap(new GridBinaryTestClasses.TestObjectContainer());

        Object[] createdArr = {mutObj, "a", 1, new String[] {"s", "s"}, new byte[] {1, 2}, new UUID(3, 0)};

        mutObj.setField("foo", createdArr.clone(), Object.class);

        GridBinaryTestClasses.TestObjectContainer res = mutObj.build().deserialize();

        createdArr[0] = res;

        assertTrue(Objects.deepEquals(createdArr, res.foo));
    }

    /**
     *
     */
    public void testDeepArray() {
        GridBinaryTestClasses.TestObjectContainer obj = new GridBinaryTestClasses.TestObjectContainer();
        obj.foo = new Object[] {new Object[] {"a", obj}};

        BinaryObjectBuilderImpl mutObj = wrap(obj);

        Object[] arr = (Object[])mutObj.<Object[]>getField("foo")[0];

        assertEquals("a", arr[0]);
        assertSame(mutObj, arr[1]);

        arr[0] = mutObj;

        GridBinaryTestClasses.TestObjectContainer res = mutObj.build().deserialize();

        arr = (Object[])((Object[])res.foo)[0];

        assertSame(arr[0], res);
        assertSame(arr[0], arr[1]);
    }

    /**
     *
     */
    public void testArrayListRead() {
        GridBinaryTestClasses.TestObjectContainer obj = new GridBinaryTestClasses.TestObjectContainer();
        obj.foo = Lists.newArrayList(obj, "a");

        BinaryObjectBuilderImpl mutObj = wrap(obj);

        List<Object> list = mutObj.getField("foo");

        assert list.equals(Lists.newArrayList(mutObj, "a"));
    }

    /**
     *
     */
    public void testArrayListOverride() {
        GridBinaryTestClasses.TestObjectContainer obj = new GridBinaryTestClasses.TestObjectContainer();

        BinaryObjectBuilderImpl mutObj = wrap(obj);

        ArrayList<Object> list = Lists.newArrayList(mutObj, "a", Lists.newArrayList(1, 2));

        mutObj.setField("foo", list, Object.class);

        GridBinaryTestClasses.TestObjectContainer res = mutObj.build().deserialize();

        list.set(0, res);

        assertNotSame(list, res.foo);
        assertEquals(list, res.foo);
    }

    /**
     *
     */
    public void testArrayListModification() {
        GridBinaryTestClasses.TestObjectContainer obj = new GridBinaryTestClasses.TestObjectContainer();
        obj.foo = Lists.newArrayList("a", "b", "c");

        BinaryObjectBuilderImpl mutObj = wrap(obj);

        List<String> list = mutObj.getField("foo");

        list.add("!"); // "a", "b", "c", "!"
        list.add(0, "_"); // "_", "a", "b", "c", "!"

        String s = list.remove(1); // "_", "b", "c", "!"
        assertEquals("a", s);

        assertEquals(Arrays.asList("c", "!"), list.subList(2, 4));
        assertEquals(1, list.indexOf("b"));
        assertEquals(1, list.lastIndexOf("b"));

        GridBinaryTestClasses.TestObjectContainer res = mutObj.build().deserialize();

        assertTrue(res.foo instanceof ArrayList);
        assertEquals(Arrays.asList("_", "b", "c", "!"), res.foo);
    }

    /**
     *
     */
    public void testArrayListClear() {
        GridBinaryTestClasses.TestObjectContainer obj = new GridBinaryTestClasses.TestObjectContainer();
        obj.foo = Lists.newArrayList("a", "b", "c");

        BinaryObjectBuilderImpl mutObj = wrap(obj);

        List<String> list = mutObj.getField("foo");

        list.clear();

        TestCase.assertEquals(Collections.emptyList(), mutObj.build().<GridBinaryTestClasses.TestObjectContainer>deserialize().foo);
    }

    /**
     *
     */
    public void testArrayListWriteUnmodifiable() {
        GridBinaryTestClasses.TestObjectContainer obj = new GridBinaryTestClasses.TestObjectContainer();

        ArrayList<Object> src = Lists.newArrayList(obj, "a", "b", "c");

        obj.foo = src;

        BinaryObjectBuilderImpl mutObj = wrap(obj);

        GridBinaryTestClasses.TestObjectContainer deserialized = mutObj.build().deserialize();

        List<Object> res = (List<Object>)deserialized.foo;

        src.set(0, deserialized);

        assertEquals(src, res);
    }

    /**
     *
     */
    public void testLinkedListRead() {
        GridBinaryTestClasses.TestObjectContainer obj = new GridBinaryTestClasses.TestObjectContainer();
        obj.foo = Lists.newLinkedList(Arrays.asList(obj, "a"));

        BinaryObjectBuilderImpl mutObj = wrap(obj);

        List<Object> list = mutObj.getField("foo");

        assert list.equals(Lists.newLinkedList(Arrays.asList(mutObj, "a")));
    }

    /**
     *
     */
    public void testLinkedListOverride() {
        GridBinaryTestClasses.TestObjectContainer obj = new GridBinaryTestClasses.TestObjectContainer();

        BinaryObjectBuilderImpl mutObj = wrap(obj);

        List<Object> list = Lists.newLinkedList(Arrays.asList(mutObj, "a", Lists.newLinkedList(Arrays.asList(1, 2))));

        mutObj.setField("foo", list, Object.class);

        GridBinaryTestClasses.TestObjectContainer res = mutObj.build().deserialize();

        list.set(0, res);

        assertNotSame(list, res.foo);
        assertEquals(list, res.foo);
    }

    /**
     *
     */
    public void testLinkedListModification() {
        GridBinaryTestClasses.TestObjectContainer obj = new GridBinaryTestClasses.TestObjectContainer();

        obj.foo = Lists.newLinkedList(Arrays.asList("a", "b", "c"));

        BinaryObjectBuilderImpl mutObj = wrap(obj);

        List<String> list = mutObj.getField("foo");

        list.add("!"); // "a", "b", "c", "!"
        list.add(0, "_"); // "_", "a", "b", "c", "!"

        String s = list.remove(1); // "_", "b", "c", "!"
        assertEquals("a", s);

        assertEquals(Arrays.asList("c", "!"), list.subList(2, 4));
        assertEquals(1, list.indexOf("b"));
        assertEquals(1, list.lastIndexOf("b"));

        GridBinaryTestClasses.TestObjectContainer res = mutObj.build().deserialize();

        assertTrue(res.foo instanceof LinkedList);
        assertEquals(Arrays.asList("_", "b", "c", "!"), res.foo);
    }

    /**
     *
     */
    public void testLinkedListWriteUnmodifiable() {
        GridBinaryTestClasses.TestObjectContainer obj = new GridBinaryTestClasses.TestObjectContainer();

        LinkedList<Object> src = Lists.newLinkedList(Arrays.asList(obj, "a", "b", "c"));

        obj.foo = src;

        BinaryObjectBuilderImpl mutObj = wrap(obj);

        GridBinaryTestClasses.TestObjectContainer deserialized = mutObj.build().deserialize();

        List<Object> res = (List<Object>)deserialized.foo;

        src.set(0, deserialized);

        assertEquals(src, res);
    }

    /**
     *
     */
    public void testHashSetRead() {
        GridBinaryTestClasses.TestObjectContainer obj = new GridBinaryTestClasses.TestObjectContainer();
        obj.foo = Sets.newHashSet(obj, "a");

        BinaryObjectBuilderImpl mutObj = wrap(obj);

        Set<Object> set = mutObj.getField("foo");

        assert set.equals(Sets.newHashSet(mutObj, "a"));
    }

    /**
     *
     */
    public void testHashSetOverride() {
        GridBinaryTestClasses.TestObjectContainer obj = new GridBinaryTestClasses.TestObjectContainer();

        BinaryObjectBuilderImpl mutObj = wrap(obj);

        Set<Object> c = Sets.newHashSet(mutObj, "a", Sets.newHashSet(1, 2));

        mutObj.setField("foo", c, Object.class);

        GridBinaryTestClasses.TestObjectContainer res = mutObj.build().deserialize();

        c.remove(mutObj);
        c.add(res);

        assertNotSame(c, res.foo);
        assertEquals(c, res.foo);
    }

    /**
     *
     */
    public void testHashSetModification() {
        GridBinaryTestClasses.TestObjectContainer obj = new GridBinaryTestClasses.TestObjectContainer();
        obj.foo = Sets.newHashSet("a", "b", "c");

        BinaryObjectBuilderImpl mutObj = wrap(obj);

        Set<String> set = mutObj.getField("foo");

        set.remove("b");
        set.add("!");

        assertEquals(Sets.newHashSet("a", "!", "c"), set);
        assertTrue(set.contains("a"));
        assertTrue(set.contains("!"));

        GridBinaryTestClasses.TestObjectContainer res = mutObj.build().deserialize();

        assertTrue(res.foo instanceof HashSet);
        assertEquals(Sets.newHashSet("a", "!", "c"), res.foo);
    }

    /**
     *
     */
    public void testHashSetWriteUnmodifiable() {
        GridBinaryTestClasses.TestObjectContainer obj = new GridBinaryTestClasses.TestObjectContainer();

        Set<Object> src = Sets.newHashSet(obj, "a", "b", "c");

        obj.foo = src;

        GridBinaryTestClasses.TestObjectContainer deserialized = wrap(obj).build().deserialize();

        Set<Object> res = (Set<Object>)deserialized.foo;

        src.remove(obj);
        src.add(deserialized);

        assertEquals(src, res);
    }

    /**
     *
     */
    public void testMapRead() {
        GridBinaryTestClasses.TestObjectContainer obj = new GridBinaryTestClasses.TestObjectContainer();
        obj.foo = Maps.newHashMap(ImmutableMap.of(obj, "a", "b", obj));

        BinaryObjectBuilderImpl mutObj = wrap(obj);

        Map<Object, Object> map = mutObj.getField("foo");

        assert map.equals(ImmutableMap.of(mutObj, "a", "b", mutObj));
    }

    /**
     *
     */
    public void testMapOverride() {
        GridBinaryTestClasses.TestObjectContainer obj = new GridBinaryTestClasses.TestObjectContainer();

        BinaryObjectBuilderImpl mutObj = wrap(obj);

        Map<Object, Object> map = Maps.newHashMap(ImmutableMap.of(mutObj, "a", "b", mutObj));

        mutObj.setField("foo", map, Object.class);

        GridBinaryTestClasses.TestObjectContainer res = mutObj.build().deserialize();

        assertEquals(ImmutableMap.of(res, "a", "b", res), res.foo);
    }

    /**
     *
     */
    public void testMapModification() {
        GridBinaryTestClasses.TestObjectContainer obj = new GridBinaryTestClasses.TestObjectContainer();
        obj.foo = Maps.newHashMap(ImmutableMap.of(1, "a", 2, "b"));

        BinaryObjectBuilderImpl mutObj = wrap(obj);

        Map<Object, Object> map = mutObj.getField("foo");

        map.put(3, mutObj);
        Object rmv = map.remove(1);

        assertEquals("a", rmv);

        GridBinaryTestClasses.TestObjectContainer res = mutObj.build().deserialize();

        assertEquals(ImmutableMap.of(2, "b", 3, res), res.foo);
    }

    /**
     *
     */
    public void testEnumArrayModification() {
        GridBinaryTestClasses.TestObjectAllTypes obj = new GridBinaryTestClasses.TestObjectAllTypes();

        obj.enumArr = new GridBinaryTestClasses.TestObjectEnum[] {GridBinaryTestClasses.TestObjectEnum.A, GridBinaryTestClasses.TestObjectEnum.B};

        BinaryObjectBuilderImpl mutObj = wrap(obj);

        BinaryBuilderEnum[] arr = mutObj.getField("enumArr");
        arr[0] = new BinaryBuilderEnum(mutObj.typeId(), GridBinaryTestClasses.TestObjectEnum.B);

        GridBinaryTestClasses.TestObjectAllTypes res = mutObj.build().deserialize();

        Assert.assertArrayEquals(new GridBinaryTestClasses.TestObjectEnum[] {GridBinaryTestClasses.TestObjectEnum.A, GridBinaryTestClasses.TestObjectEnum.B}, res.enumArr);
    }

    /**
     *
     */
    public void testEditObjectWithRawData() {
        GridBinaryMarshalerAwareTestClass obj = new GridBinaryMarshalerAwareTestClass();

        obj.s = "a";
        obj.sRaw = "aa";

        BinaryObjectBuilderImpl mutableObj = wrap(obj);

        mutableObj.setField("s", "z");

        GridBinaryMarshalerAwareTestClass res = mutableObj.build().deserialize();
        assertEquals("z", res.s);
        assertEquals("aa", res.sRaw);
    }

    /**
     *
     */
    public void testHashCode() {
        GridBinaryTestClasses.TestObjectContainer obj = new GridBinaryTestClasses.TestObjectContainer();

        BinaryObjectBuilderImpl mutableObj = wrap(obj);

        assertEquals(obj.hashCode(), mutableObj.build().hashCode());

        mutableObj.hashCode(25);

        assertEquals(25, mutableObj.build().hashCode());
    }

    /**
     *
     */
    public void testCollectionsInCollection() {
        GridBinaryTestClasses.TestObjectContainer obj = new GridBinaryTestClasses.TestObjectContainer();
        obj.foo = Lists.newArrayList(
            Lists.newArrayList(1, 2),
            Lists.newLinkedList(Arrays.asList(1, 2)),
            Sets.newHashSet("a", "b"),
            Sets.newLinkedHashSet(Arrays.asList("a", "b")),
            Maps.newHashMap(ImmutableMap.of(1, "a", 2, "b")));

        GridBinaryTestClasses.TestObjectContainer deserialized = wrap(obj).build().deserialize();

        assertEquals(obj.foo, deserialized.foo);
    }

    /**
     *
     */
    public void testMapEntryOverride() {
        GridBinaryTestClasses.TestObjectContainer obj = new GridBinaryTestClasses.TestObjectContainer();

        BinaryObjectBuilderImpl mutableObj = wrap(obj);

        mutableObj.setField("foo", new GridMapEntry<>(1, "a"));

        GridBinaryTestClasses.TestObjectContainer res = mutableObj.build().deserialize();

        assertEquals(new GridMapEntry<>(1, "a"), res.foo);
    }

    /**
     *
     */
    public void testMetadataChangingDoublePut() {
        BinaryObjectBuilderImpl mutableObj = wrap(new GridBinaryTestClasses.TestObjectContainer());

        mutableObj.setField("xx567", "a");
        mutableObj.setField("xx567", "b");

        mutableObj.build();

        BinaryType metadata = binaries().type(GridBinaryTestClasses.TestObjectContainer.class);

        assertEquals("String", metadata.fieldTypeName("xx567"));
    }

    /**
     *
     */
    public void testMetadataChangingDoublePut2() {
        BinaryObjectBuilderImpl mutableObj = wrap(new GridBinaryTestClasses.TestObjectContainer());

        mutableObj.setField("xx567", "a");
        mutableObj.setField("xx567", "b");

        mutableObj.build();

        BinaryType metadata = binaries().type(GridBinaryTestClasses.TestObjectContainer.class);

        assertEquals("String", metadata.fieldTypeName("xx567"));
    }

    /**
     *
     */
    public void testMetadataChanging() {
        GridBinaryTestClasses.TestObjectContainer c = new GridBinaryTestClasses.TestObjectContainer();

        BinaryObjectBuilderImpl mutableObj = wrap(c);

        mutableObj.setField("intField", 1);
        mutableObj.setField("intArrField", new int[] {1});
        mutableObj.setField("arrField", new String[] {"1"});
        mutableObj.setField("strField", "1");
        mutableObj.setField("colField", Lists.newArrayList("1"));
        mutableObj.setField("mapField", Maps.newHashMap(ImmutableMap.of(1, "1")));
        mutableObj.setField("enumField", GridBinaryTestClasses.TestObjectEnum.A);
        mutableObj.setField("enumArrField", new Enum[] {GridBinaryTestClasses.TestObjectEnum.A});

        mutableObj.build();

        BinaryType metadata = binaries().type(c.getClass());

        assertTrue(metadata.fieldNames().containsAll(Arrays.asList("intField", "intArrField", "arrField", "strField",
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
    public void testWrongMetadataNullField() {
        BinaryObjectBuilder builder = binaries().builder("SomeType");

        builder.setField("dateField", null);

        builder.setField("objectField", null, Integer.class);

        builder.build();

        try {
            builder = binaries().builder("SomeType");

            builder.setField("dateField", new Date());

            builder.build();
        }
        catch (BinaryObjectException ex) {
            assertTrue(ex.getMessage().startsWith("Wrong value has been set"));
        }

        builder = binaries().builder("SomeType");

        try {
            builder.setField("objectField", new GridBinaryTestClasses.Company());

            builder.build();

            fail("BinaryObjectBuilder accepted wrong metadata");
        }
        catch (BinaryObjectException ex) {
            assertTrue(ex.getMessage().startsWith("Wrong value has been set"));
        }
    }

    /**
     *
     */
    public void testWrongMetadataNullField2() {
        BinaryObjectBuilder builder = binaries().builder("SomeType1");

        builder.setField("dateField", null);

        builder.setField("objectField", null, Integer.class);

        BinaryObject obj = builder.build();

        try {
            builder = binaries().builder(obj);

            builder.setField("dateField", new Date());

            builder.build();
        }
        catch (BinaryObjectException ex) {
            assertTrue(ex.getMessage().startsWith("Wrong value has been set"));
        }

        builder = binaries().builder(obj);

        try {
            builder.setField("objectField", new GridBinaryTestClasses.Company());

            builder.build();

            fail("BinaryObjectBuilder accepted wrong metadata");
        }
        catch (BinaryObjectException ex) {
            assertTrue(ex.getMessage().startsWith("Wrong value has been set"));
        }
    }

    /**
     *
     */
    public void testCorrectMetadataNullField() {
        BinaryObjectBuilder builder = binaries().builder("SomeType2");

        builder.setField("dateField", null, Date.class);

        builder.setField("objectField", null, GridBinaryTestClasses.Company.class);

        builder.build();

        builder = binaries().builder("SomeType2");

        builder.setField("dateField", new Date());

        builder.setField("objectField", new GridBinaryTestClasses.Company());

        builder.build();

    }

    /**
     *
     */
    public void testCorrectMetadataNullField2() {
        BinaryObjectBuilder builder = binaries().builder("SomeType3");

        builder.setField("dateField", null, Date.class);

        builder.setField("objectField", null, GridBinaryTestClasses.Company.class);

        BinaryObject obj = builder.build();

        builder = binaries().builder(obj);

        builder.setField("dateField", new Date());

        builder.setField("objectField", new GridBinaryTestClasses.Company());

        builder.build();
    }

    /**
     *
     */
    public void testDateInObjectField() {
        GridBinaryTestClasses.TestObjectContainer obj = new GridBinaryTestClasses.TestObjectContainer();

        obj.foo = new Date();

        BinaryObjectBuilderImpl mutableObj = wrap(obj);

        assertEquals(Date.class, mutableObj.getField("foo").getClass());
    }

    /**
     *
     */
    public void testTimestampInObjectField() {
        GridBinaryTestClasses.TestObjectContainer obj = new GridBinaryTestClasses.TestObjectContainer();

        obj.foo = new Timestamp(100020003);

        BinaryObjectBuilderImpl mutableObj = wrap(obj);

        assertEquals(Timestamp.class, mutableObj.getField("foo").getClass());
    }

    /**
     *
     */
    public void testDateInCollection() {
        GridBinaryTestClasses.TestObjectContainer obj = new GridBinaryTestClasses.TestObjectContainer();

        obj.foo = Lists.newArrayList(new Date());

        BinaryObjectBuilderImpl mutableObj = wrap(obj);

        assertEquals(Date.class, ((List<?>)mutableObj.getField("foo")).get(0).getClass());
    }

    /**
     *
     */
    public void testTimestampInCollection() {
        GridBinaryTestClasses.TestObjectContainer obj = new GridBinaryTestClasses.TestObjectContainer();

        obj.foo = Lists.newArrayList(new Timestamp(100020003));

        BinaryObjectBuilderImpl mutableObj = wrap(obj);

        assertEquals(Timestamp.class, ((List<?>)mutableObj.getField("foo")).get(0).getClass());
    }

    /**
     *
     */
    @SuppressWarnings("AssertEqualsBetweenInconvertibleTypes")
    public void testDateArrayOverride() {
        GridBinaryTestClasses.TestObjectContainer obj = new GridBinaryTestClasses.TestObjectContainer();

        BinaryObjectBuilderImpl mutableObj = wrap(obj);

        Date[] arr = {new Date()};

        mutableObj.setField("foo", arr, Object.class);

        GridBinaryTestClasses.TestObjectContainer res = mutableObj.build().deserialize();

        assertEquals(Date[].class, res.foo.getClass());
        assertTrue(Objects.deepEquals(arr, res.foo));
    }

    /**
     *
     */
    @SuppressWarnings("AssertEqualsBetweenInconvertibleTypes")
    public void testTimestampArrayOverride() {
        GridBinaryTestClasses.TestObjectContainer obj = new GridBinaryTestClasses.TestObjectContainer();

        BinaryObjectBuilderImpl mutableObj = wrap(obj);

        Timestamp[] arr = {new Timestamp(100020003)};

        mutableObj.setField("foo", arr, Object.class);

        GridBinaryTestClasses.TestObjectContainer res = mutableObj.build().deserialize();

        assertEquals(Timestamp[].class, res.foo.getClass());
        assertTrue(Objects.deepEquals(arr, res.foo));
    }

    /**
     *
     */
    public void testChangeMap() {
        GridBinaryTestClasses.Addresses addrs = new GridBinaryTestClasses.Addresses();

        addrs.addCompany(new GridBinaryTestClasses.Company(1, "Google inc", 100,
            new GridBinaryTestClasses.Address("Saint-Petersburg", "Torzhkovskya", 1, 53), "occupation"));

        addrs.addCompany(new GridBinaryTestClasses.Company(2, "Apple inc", 100,
            new GridBinaryTestClasses.Address("Saint-Petersburg", "Torzhkovskya", 1, 54), "occupation"));

        addrs.addCompany(new GridBinaryTestClasses.Company(3, "Microsoft", 100,
            new GridBinaryTestClasses.Address("Saint-Petersburg", "Torzhkovskya", 1, 55), "occupation"));

        addrs.addCompany(new GridBinaryTestClasses.Company(4, "Oracle", 100,
            new GridBinaryTestClasses.Address("Saint-Petersburg", "Nevskiy", 1, 1), "occupation"));

        BinaryObjectBuilderImpl binaryAddres = wrap(addrs);

        Map<String, BinaryObjectBuilderImpl> map = binaryAddres.getField("companyByStreet");

        BinaryObjectBuilderImpl binaryCompanies = map.get("Torzhkovskya");

        List<BinaryObjectBuilderImpl> binaryCompaniesList = binaryCompanies.getField("companies");

        BinaryObjectBuilderImpl company = binaryCompaniesList.get(0);

        assert "Google inc".equals(company.<String>getField("name"));

        binaryCompaniesList.remove(0);

        GridBinaryTestClasses.Addresses res = binaryAddres.build().deserialize();

        assertEquals(Arrays.asList("Nevskiy", "Torzhkovskya"), new ArrayList<>(res.getCompanyByStreet().keySet()));

        GridBinaryTestClasses.Companies torzhkovskyaCompanies = res.getCompanyByStreet().get("Torzhkovskya");

        assertEquals(2, torzhkovskyaCompanies.size());
        assertEquals("Apple inc", torzhkovskyaCompanies.get(0).name);
    }

    /**
     *
     */
    public void testSavingObjectWithNotZeroStart() {
        GridBinaryTestClasses.TestObjectOuter out = new GridBinaryTestClasses.TestObjectOuter();
        GridBinaryTestClasses.TestObjectInner inner = new GridBinaryTestClasses.TestObjectInner();

        out.inner = inner;
        inner.outer = out;

        BinaryObjectBuilderImpl builder = wrap(out);

        BinaryObjectBuilderImpl innerBuilder = builder.getField("inner");

        GridBinaryTestClasses.TestObjectInner res = innerBuilder.build().deserialize();

        assertSame(res, res.outer.inner);
    }

    /**
     *
     */
    public void testBinaryObjectField() {
        GridBinaryTestClasses.TestObjectContainer container = new GridBinaryTestClasses.TestObjectContainer(toBinary(new GridBinaryTestClasses.TestObjectArrayList()));

        BinaryObjectBuilderImpl wrapper = wrap(container);

        assertTrue(wrapper.getField("foo") instanceof BinaryObject);

        GridBinaryTestClasses.TestObjectContainer deserialized = wrapper.build().deserialize();
        assertTrue(deserialized.foo instanceof BinaryObject);
    }

    /**
     *
     */
    public void testAssignBinaryObject() {
        GridBinaryTestClasses.TestObjectContainer container = new GridBinaryTestClasses.TestObjectContainer();

        BinaryObjectBuilderImpl wrapper = wrap(container);

        wrapper.setField("foo", toBinary(new GridBinaryTestClasses.TestObjectArrayList()));

        GridBinaryTestClasses.TestObjectContainer deserialized = wrapper.build().deserialize();
        assertTrue(deserialized.foo instanceof GridBinaryTestClasses.TestObjectArrayList);
    }

    /**
     *
     */
    public void testRemoveFromNewObject() {
        BinaryObjectBuilderImpl wrapper = newWrapper(GridBinaryTestClasses.TestObjectAllTypes.class);

        wrapper.setField("str", "a");

        wrapper.removeField("str");

        TestCase.assertNull(wrapper.build().<GridBinaryTestClasses.TestObjectAllTypes>deserialize().str);
    }

    /**
     *
     */
    public void testRemoveFromExistingObject() {
        GridBinaryTestClasses.TestObjectAllTypes obj = new GridBinaryTestClasses.TestObjectAllTypes();
        obj.setDefaultData();

        BinaryObjectBuilderImpl wrapper = wrap(toBinary(obj));

        wrapper.removeField("str");

        TestCase.assertNull(wrapper.build().<GridBinaryTestClasses.TestObjectAllTypes>deserialize().str);
    }

    /**
     *
     */
    public void testCyclicArrays() {
        GridBinaryTestClasses.TestObjectContainer obj = new GridBinaryTestClasses.TestObjectContainer();

        Object[] arr1 = new Object[1];
        Object[] arr2 = new Object[] {arr1};

        arr1[0] = arr2;

        obj.foo = arr1;

        GridBinaryTestClasses.TestObjectContainer res = toBinary(obj).deserialize();

        Object[] resArr = (Object[])res.foo;

        assertSame(((Object[])resArr[0])[0], resArr);
    }

    /**
     *
     */
    @SuppressWarnings("TypeMayBeWeakened")
    public void testCyclicArrayList() {
        GridBinaryTestClasses.TestObjectContainer obj = new GridBinaryTestClasses.TestObjectContainer();

        List<Object> arr1 = new ArrayList<>();
        List<Object> arr2 = new ArrayList<>();

        arr1.add(arr2);
        arr2.add(arr1);

        obj.foo = arr1;

        GridBinaryTestClasses.TestObjectContainer res = toBinary(obj).deserialize();

        List<?> resArr = (List<?>)res.foo;

        assertSame(((List<Object>)resArr.get(0)).get(0), resArr);
    }

    /**
     * @throws Exception If failed.
     */
    public void testSameBinaryKey() throws Exception {
        IgniteCache<BinaryObject, BinaryObject> replicatedCache =
            jcache(0).withKeepBinary();

        IgniteCache<BinaryObject, BinaryObject> partitionedCache =
            jcache(0, "partitioned").withKeepBinary();

        BinaryObjectBuilder keyBuilder = ignite(0).binary().builder("keyType")
            .setField("F1", "V1").hashCode("V1".hashCode());

        BinaryObjectBuilder valBuilder = ignite(0).binary().builder("valueType")
            .setField("F2", "V2")
            .setField("F3", "V3");

        BinaryObject key = keyBuilder.build();
        BinaryObject val = valBuilder.build();

        replicatedCache.put(key, val);
        partitionedCache.put(key, val);

        assertNotNull(replicatedCache.get(key));
        assertNotNull(partitionedCache.get(key));
    }

    /**
     * @param obj Object.
     * @return Object in binary format.
     */
    private BinaryObject toBinary(Object obj) {
        return binaries().toBinary(obj);
    }

    /**
     * @param obj Object.
     * @return GridMutableBinaryObject.
     */
    private BinaryObjectBuilderImpl wrap(Object obj) {
        return BinaryObjectBuilderImpl.wrap(toBinary(obj));
    }

    /**
     * @param aCls Class.
     * @return Wrapper.
     */
    private BinaryObjectBuilderImpl newWrapper(Class<?> aCls) {
        CacheObjectBinaryProcessorImpl processor = (CacheObjectBinaryProcessorImpl)(
            (IgniteBinaryImpl)binaries()).processor();

        return new BinaryObjectBuilderImpl(processor.binaryContext(), processor.typeId(aCls.getName()),
            processor.binaryContext().userTypeName(aCls.getName()));
    }

    /**
     * Check that correct type is stored in binary object.
     */
    public void testCollectionsSerialization() {
        final BinaryObjectBuilder root = newWrapper(BigInteger.class);

        final List<Integer> arrList = new ArrayList<>();

        arrList.add(Integer.MAX_VALUE);

        final List<Integer> linkedList = new LinkedList<>();

        linkedList.add(Integer.MAX_VALUE);

        final Set<Integer> hashSet = new HashSet<>();

        hashSet.add(Integer.MAX_VALUE);

        final Set<Integer> linkedHashSet = new LinkedHashSet<>();

        linkedHashSet.add(Integer.MAX_VALUE);

        final Map<String, String> hashMap = new HashMap<>();

        hashMap.put("key", "val");

        final Map<String, String> linkedHashMap = new LinkedHashMap<>();

        linkedHashMap.put("key", "val");

        // collections
        root.setField("arrayList", arrList);
        root.setField("linkedList", linkedList);
        root.setField("hashSet", hashSet);
        root.setField("linkedHashSet", linkedHashSet);

        root.setField("singletonList", Collections.singletonList(Integer.MAX_VALUE), Collection.class);
        root.setField("singletonSet", Collections.singleton(Integer.MAX_VALUE), Collection.class);

        // maps
        root.setField("hashMap", hashMap);
        root.setField("linkedHashMap", linkedHashMap);

        root.setField("singletonMap", Collections.singletonMap("key", "val"), Map.class);

        // objects
        root.setField("asList", Collections.singletonList(Integer.MAX_VALUE));
        root.setField("asSet", Collections.singleton(Integer.MAX_VALUE));
        root.setField("asMap", Collections.singletonMap("key", "val"));
        root.setField("asListHint", Collections.singletonList(Integer.MAX_VALUE), List.class);
        root.setField("asSetHint", Collections.singleton(Integer.MAX_VALUE), Set.class);
        root.setField("asMapHint", (AbstractMap)Collections.singletonMap("key", "val"), AbstractMap.class);

        BinaryObject binaryObj = root.build();

        final String COL = "Collection";
        final String MAP = "Map";
        final String OBJ = "Object";

        assert COL.equals(binaryObj.type().fieldTypeName("arrayList"));
        assert COL.equals(binaryObj.type().fieldTypeName("linkedList"));
        assert COL.equals(binaryObj.type().fieldTypeName("hashSet"));
        assert COL.equals(binaryObj.type().fieldTypeName("linkedHashSet"));
        assert COL.equals(binaryObj.type().fieldTypeName("linkedHashSet"));
        assert COL.equals(binaryObj.type().fieldTypeName("linkedHashSet"));

        assert COL.equals(binaryObj.type().fieldTypeName("singletonList"));
        assert COL.equals(binaryObj.type().fieldTypeName("singletonSet"));

        assert MAP.equals(binaryObj.type().fieldTypeName("singletonMap"));

        assert OBJ.equals(binaryObj.type().fieldTypeName("asList"));
        assert OBJ.equals(binaryObj.type().fieldTypeName("asSet"));
        assert OBJ.equals(binaryObj.type().fieldTypeName("asMap"));
        assert OBJ.equals(binaryObj.type().fieldTypeName("asListHint"));
        assert OBJ.equals(binaryObj.type().fieldTypeName("asSetHint"));
        assert OBJ.equals(binaryObj.type().fieldTypeName("asMapHint"));
    }
}
