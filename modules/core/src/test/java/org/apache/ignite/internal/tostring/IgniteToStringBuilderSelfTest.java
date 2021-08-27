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

package org.apache.ignite.internal.tostring;

import java.lang.reflect.Array;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.TreeMap;
import java.util.UUID;
import java.util.concurrent.Callable;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.Executor;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.locks.ReadWriteLock;
import org.apache.ignite.internal.testframework.IgniteAbstractTest;
import org.apache.ignite.internal.util.IgniteUtils;
import org.apache.ignite.lang.IgniteInternalException;
import org.apache.ignite.lang.IgniteSystemProperties;
import org.junit.jupiter.api.Test;

import static org.apache.ignite.internal.tostring.IgniteToStringBuilder.identity;
import static org.apache.ignite.lang.IgniteSystemProperties.IGNITE_TO_STRING_COLLECTION_LIMIT;
import static org.apache.ignite.lang.IgniteSystemProperties.IGNITE_TO_STRING_MAX_LENGTH;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Tests for {@link IgniteToStringBuilder}.
 */
public class IgniteToStringBuilderSelfTest extends IgniteAbstractTest {
    /**
     *
     */
    @Test
    public void testToString() {
        TestClass1 obj = new TestClass1();

        assertEquals(obj.toStringManual(), obj.toStringAutomatic());
    }

    /**
     *
     */
    @Test
    public void testToStringWithAdditions() {
        TestClass1 obj = new TestClass1();

        String manual = obj.toStringWithAdditionalManual();

        String automatic = obj.toStringWithAdditionalAutomatic();

        assertEquals(manual, automatic);
    }

    /**
     *
     */
    @Test
    public void testToStringCheckSimpleListRecursionPrevention() {
        ArrayList<Object> list1 = new ArrayList<>();
        ArrayList<Object> list2 = new ArrayList<>();

        list2.add(list1);
        list1.add(list2);

        assertEquals("ArrayList [size=1]", IgniteToStringBuilder.toString(ArrayList.class, list1));
        assertEquals("ArrayList [size=1]", IgniteToStringBuilder.toString(ArrayList.class, list2));
    }

    /**
     *
     */
    @Test
    public void testToStringCheckSimpleMapRecursionPrevention() {
        HashMap<Object, Object> map1 = new HashMap<>();
        HashMap<Object, Object> map2 = new HashMap<>();

        map1.put("2", map2);
        map2.put("1", map1);

        assertEquals("HashMap []", IgniteToStringBuilder.toString(HashMap.class, map1));
        assertEquals("HashMap []", IgniteToStringBuilder.toString(HashMap.class, map2));
    }

    /**
     *
     */
    @Test
    public void testToStringCheckListAdvancedRecursionPrevention() {
        ArrayList<Object> list1 = new ArrayList<>();
        ArrayList<Object> list2 = new ArrayList<>();

        list2.add(list1);
        list1.add(list2);

        final String hash1 = identity(list1);
        final String hash2 = identity(list2);

        assertEquals("ArrayList" + hash1 + " [size=1, name=ArrayList [ArrayList" + hash1 + "]]",
            IgniteToStringBuilder.toString(ArrayList.class, list1, "name", list2));
        assertEquals("ArrayList" + hash2 + " [size=1, name=ArrayList [ArrayList" + hash2 + "]]",
            IgniteToStringBuilder.toString(ArrayList.class, list2, "name", list1));
    }

    /**
     *
     */
    @Test
    public void testToStringCheckMapAdvancedRecursionPrevention() {
        HashMap<Object, Object> map1 = new HashMap<>();
        HashMap<Object, Object> map2 = new HashMap<>();

        map1.put("2", map2);
        map2.put("1", map1);

        final String hash1 = identity(map1);
        final String hash2 = identity(map2);

        assertEquals("HashMap" + hash1 + " [name=HashMap {1=HashMap" + hash1 + "}]",
            IgniteToStringBuilder.toString(HashMap.class, map1, "name", map2));
        assertEquals("HashMap" + hash2 + " [name=HashMap {2=HashMap" + hash2 + "}]",
            IgniteToStringBuilder.toString(HashMap.class, map2, "name", map1));
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testToStringCheckObjectRecursionPrevention() throws Exception {
        Node n1 = new Node();
        Node n2 = new Node();
        Node n3 = new Node();
        Node n4 = new Node();

        n1.name = "n1";
        n2.name = "n2";
        n3.name = "n3";
        n4.name = "n4";

        n1.next = n2;
        n2.next = n3;
        n3.next = n4;
        n4.next = n3;

        n1.nodes = new Node[4];
        n2.nodes = n1.nodes;
        n3.nodes = n1.nodes;
        n4.nodes = n1.nodes;

        n1.nodes[0] = n1;
        n1.nodes[1] = n2;
        n1.nodes[2] = n3;
        n1.nodes[3] = n4;

        String expN1 = n1.toString();
        String expN2 = n2.toString();
        String expN3 = n3.toString();
        String expN4 = n4.toString();

        CyclicBarrier bar = new CyclicBarrier(4);
        ExecutorService pool = Executors.newFixedThreadPool(4);

        CompletableFuture<String> fut1 = runAsync(new BarrierCallable(bar, n1, expN1), pool);
        CompletableFuture<String> fut2 = runAsync(new BarrierCallable(bar, n2, expN2), pool);
        CompletableFuture<String> fut3 = runAsync(new BarrierCallable(bar, n3, expN3), pool);
        CompletableFuture<String> fut4 = runAsync(new BarrierCallable(bar, n4, expN4), pool);

        fut1.get(3_000, TimeUnit.MILLISECONDS);
        fut2.get(3_000, TimeUnit.MILLISECONDS);
        fut3.get(3_000, TimeUnit.MILLISECONDS);
        fut4.get(3_000, TimeUnit.MILLISECONDS);

        IgniteUtils.shutdownAndAwaitTermination(pool, 3_000, TimeUnit.MILLISECONDS);
    }

    /**
     * @param callable Callable.
     * @return Completable future.
     */
    private static <U> CompletableFuture<U> runAsync(Callable<U> callable, Executor pool) {
        return CompletableFuture.supplyAsync(() -> {
            try {
                return callable.call();
            }
            catch (Throwable th) {
                throw new IgniteInternalException(th);
            }
        }, pool);
    }

    /**
     * JUnit.
     */
    @Test
    public void testToStringPerformance() {
        TestClass1 obj = new TestClass1();

        // Warm up.
        obj.toStringAutomatic();

        long start = System.currentTimeMillis();

        for (int i = 0; i < 100000; i++)
            obj.toStringManual();

        logger().info("Manual toString() took: {}ms", System.currentTimeMillis() - start);

        start = System.currentTimeMillis();

        for (int i = 0; i < 100000; i++)
            obj.toStringAutomatic();

        logger().info("Automatic toString() took: {}ms", System.currentTimeMillis() - start);
    }

    /**
     * Test array print.
     *
     * @param v value to get array class and fill array.
     * @param limit value of IGNITE_TO_STRING_COLLECTION_LIMIT.
     */
    private <T, V> void testArr(V v, int limit) {
        T[] arrOf = (T[])Array.newInstance(v.getClass(), limit + 1);
        Arrays.fill(arrOf, v);
        T[] arr = Arrays.copyOf(arrOf, limit);

        checkArrayOverflow(arrOf, arr, limit);
    }

    /**
     * Test array print.
     */
    @Test
    @SuppressWarnings({"rawtypes", "unchecked"})
    public void testArrLimitWithRecursion() {
        int limit = IgniteSystemProperties.getInteger(IGNITE_TO_STRING_COLLECTION_LIMIT, 100);

        ArrayList[] arrOf = new ArrayList[limit + 1];
        Arrays.fill(arrOf, new ArrayList());
        ArrayList[] arr = Arrays.copyOf(arrOf, limit);

        arrOf[0].add(arrOf);
        arr[0].add(arr);

        checkArrayOverflow(arrOf, arr, limit);
    }

    /**
     * @param arrOf Array.
     * @param arr Array copy.
     * @param limit Array limit.
     */
    private void checkArrayOverflow(Object[] arrOf, Object[] arr, int limit) {
        String arrStr = IgniteToStringBuilder.arrayToString(arr);
        String arrOfStr = IgniteToStringBuilder.arrayToString(arrOf);

        // Simulate overflow
        StringBuilder resultSB = new StringBuilder(arrStr);
        resultSB.deleteCharAt(resultSB.length() - 1);
        resultSB.append("... and ").append(arrOf.length - limit).append(" more]");

        arrStr = resultSB.toString();

        assertEquals(arrStr, arrOfStr, "Collection limit error in array of type " +
            arrOf.getClass().getName() + " error, normal arr: <" + arrStr + ">, overflowed arr: <" + arrOfStr + ">");
    }

    /**
     *
     */
    @Test
    public void testToStringCollectionLimits() {
        int limit = IgniteSystemProperties.getInteger(IGNITE_TO_STRING_COLLECTION_LIMIT, 100);

        Object[] vals = new Object[] {
            Byte.MIN_VALUE, Boolean.TRUE, Short.MIN_VALUE, Integer.MIN_VALUE, Long.MIN_VALUE,
            Float.MIN_VALUE, Double.MIN_VALUE, Character.MIN_VALUE, new TestClass1()};
        for (Object val : vals)
            testArr(val, limit);

        //noinspection ZeroLengthArrayAllocation
        int[] intArr1 = new int[0];

        assertEquals("[]", IgniteToStringBuilder.arrayToString(intArr1));
        assertEquals("null", IgniteToStringBuilder.arrayToString(null));

        int[] intArr2 = {1, 2, 3};

        assertEquals("[1, 2, 3]", IgniteToStringBuilder.arrayToString(intArr2));

        Object[] intArr3 = {2, 3, 4};

        assertEquals("[2, 3, 4]", IgniteToStringBuilder.arrayToString(intArr3));

        byte[] byteArr = new byte[1];

        byteArr[0] = 1;
        assertEquals(Arrays.toString(byteArr), IgniteToStringBuilder.arrayToString(byteArr));
        byteArr = Arrays.copyOf(byteArr, 101);
        assertTrue(IgniteToStringBuilder.arrayToString(byteArr).contains("... and 1 more"),
            "Can't find \"... and 1 more\" in overflowed array string!");

        boolean[] boolArr = new boolean[1];

        boolArr[0] = true;
        assertEquals(Arrays.toString(boolArr), IgniteToStringBuilder.arrayToString(boolArr));
        boolArr = Arrays.copyOf(boolArr, 101);
        assertTrue(IgniteToStringBuilder.arrayToString(boolArr).contains("... and 1 more"),
            "Can't find \"... and 1 more\" in overflowed array string!");

        short[] shortArr = new short[1];

        shortArr[0] = 100;
        assertEquals(Arrays.toString(shortArr), IgniteToStringBuilder.arrayToString(shortArr));
        shortArr = Arrays.copyOf(shortArr, 101);
        assertTrue(IgniteToStringBuilder.arrayToString(shortArr).contains("... and 1 more"),
            "Can't find \"... and 1 more\" in overflowed array string!");

        int[] intArr = new int[1];

        intArr[0] = 10000;
        assertEquals(Arrays.toString(intArr), IgniteToStringBuilder.arrayToString(intArr));
        intArr = Arrays.copyOf(intArr, 101);
        assertTrue(IgniteToStringBuilder.arrayToString(intArr).contains("... and 1 more"),
            "Can't find \"... and 1 more\" in overflowed array string!");

        long[] longArr = new long[1];

        longArr[0] = 10000000;
        assertEquals(Arrays.toString(longArr), IgniteToStringBuilder.arrayToString(longArr));
        longArr = Arrays.copyOf(longArr, 101);
        assertTrue(
            IgniteToStringBuilder.arrayToString(longArr).contains("... and 1 more"),
            "Can't find \"... and 1 more\" in overflowed array string!");

        float[] floatArr = new float[1];

        floatArr[0] = 1.f;
        assertEquals(Arrays.toString(floatArr), IgniteToStringBuilder.arrayToString(floatArr));
        floatArr = Arrays.copyOf(floatArr, 101);
        assertTrue(IgniteToStringBuilder.arrayToString(floatArr).contains("... and 1 more"),
            "Can't find \"... and 1 more\" in overflowed array string!");

        double[] doubleArr = new double[1];

        doubleArr[0] = 1.;
        assertEquals(Arrays.toString(doubleArr), IgniteToStringBuilder.arrayToString(doubleArr));
        doubleArr = Arrays.copyOf(doubleArr, 101);
        assertTrue(IgniteToStringBuilder.arrayToString(doubleArr).contains("... and 1 more"),
            "Can't find \"... and 1 more\" in overflowed array string!");

        char[] cArr = new char[1];

        cArr[0] = 'a';
        assertEquals(Arrays.toString(cArr), IgniteToStringBuilder.arrayToString(cArr));
        cArr = Arrays.copyOf(cArr, 101);
        assertTrue(IgniteToStringBuilder.arrayToString(cArr).contains("... and 1 more"),
            "Can't find \"... and 1 more\" in overflowed array string!");

        Map<String, String> strMap = new TreeMap<>();
        List<String> strList = new ArrayList<>(limit + 1);

        TestClass1 testCls = new TestClass1();

        testCls.strMap = strMap;
        testCls.strListIncl = strList;

        for (int i = 0; i < limit; i++) {
            strMap.put("k" + i, "v");
            strList.add("e");
        }

        checkColAndMap(testCls);
    }

    /**
     *
     */
    @Test
    @SuppressWarnings({"rawtypes", "unchecked"})
    public void testToStringColAndMapLimitWithRecursion() {
        int limit = IgniteSystemProperties.getInteger(IGNITE_TO_STRING_COLLECTION_LIMIT, 100);
        Map strMap = new TreeMap<>();
        List strList = new ArrayList<>(limit + 1);

        TestClass1 testClass = new TestClass1();
        testClass.strMap = strMap;
        testClass.strListIncl = strList;

        Map m = new TreeMap();
        m.put("m", strMap);

        List l = new ArrayList();
        l.add(strList);

        strMap.put("k0", m);
        strList.add(l);

        for (int i = 1; i < limit; i++) {
            strMap.put("k" + i, "v");
            strList.add("e");
        }

        checkColAndMap(testClass);
    }

    /**
     * @param testCls Class with collection and map included in toString().
     */
    private void checkColAndMap(TestClass1 testCls) {
        String testClsStr = IgniteToStringBuilder.toString(TestClass1.class, testCls);

        testCls.strMap.put("kz", "v"); // important to add last element in TreeMap here
        testCls.strListIncl.add("e");

        String testClsStrOf = IgniteToStringBuilder.toString(TestClass1.class, testCls);

        String testClsStrOfR = testClsStrOf.replaceAll("... and 1 more", "");

        assertEquals(testClsStr.length(), testClsStrOfR.length(),
            "Collection limit error in Map or List, normal: <" + testClsStr + ">, overflowed: <" + testClsStrOf + ">");
    }

    /**
     *
     */
    @Test
    public void testToStringSizeLimits() {
        int limit = IgniteSystemProperties.getInteger(IGNITE_TO_STRING_MAX_LENGTH, 10_000);
        int tailLen = limit / 10 * 2;

        StringBuilder sb = new StringBuilder(limit + 10);

        sb.append("a".repeat(Math.max(0, limit - 100)));

        String actual = IgniteToStringBuilder.toString(TestClass2.class, new TestClass2(sb.toString()));
        String exp = "TestClass2 [str=" + sb + ", nullArr=null]";

        assertEquals(exp, actual);

        sb.append("b".repeat(110));

        actual = IgniteToStringBuilder.toString(TestClass2.class, new TestClass2(sb.toString()));
        exp = "TestClass2 [str=" + sb + ", nullArr=null]";

        assertEquals(exp.substring(0, limit - tailLen), actual.substring(0, limit - tailLen));
        assertEquals(exp.substring(exp.length() - tailLen), actual.substring(actual.length() - tailLen));

        assertTrue(actual.contains("... and"));
        assertTrue(actual.contains("skipped ..."));
    }

    /**
     *
     */
    @Test
    public void testHierarchy() {
        Wrapper w = new Wrapper();
        Parent p = w.p;
        String hash = identity(p);

        assertEquals("Wrapper [p=Child [b=0, pb=Parent[] [null], super=Parent [a=0, pa=Parent[] [null]]]]",
            w.toString());

        p.pa[0] = p;

        assertEquals("Wrapper [p=Child" + hash + " [b=0, pb=Parent[] [null]," +
            " super=Parent [a=0, pa=Parent[] [Child" + hash + "]]]]", w.toString());

        ((Child)p).pb[0] = p;

        assertEquals("Wrapper [p=Child" + hash + " [b=0, pb=Parent[] [Child" + hash
            + "], super=Parent [a=0, pa=Parent[] [Child" + hash + "]]]]", w.toString());
    }

    /**
     * Verifies that {@link IgniteToStringBuilder} doesn't fail while iterating over concurrently modified collection.
     *
     * @throws Exception If failed.
     */
    @Test
    public void testToStringCheckConcurrentModificationExceptionFromList() throws Exception {
        ClassWithList classWithList = new ClassWithList();

        CountDownLatch modificationStartedLatch = new CountDownLatch(1);
        AtomicBoolean finished = new AtomicBoolean(false);

        final CompletableFuture<Void> finishFut = CompletableFuture.runAsync(() -> {
            List<SlowToStringObject> list = classWithList.list;
            for (int i = 0; i < 100; i++)
                list.add(new SlowToStringObject());

            Random rnd = new Random();

            while (!finished.get()) {
                if (rnd.nextBoolean() && list.size() > 1)
                    list.remove(list.size() / 2);
                else
                    list.add(list.size() / 2, new SlowToStringObject());

                if (modificationStartedLatch.getCount() > 0)
                    modificationStartedLatch.countDown();
            }
        });

        modificationStartedLatch.await();

        String s = null;

        try {
            s = classWithList.toString();
        }
        finally {
            finished.set(true);

            finishFut.get();

            assertNotNull(s);
            assertTrue(s.contains("concurrent modification"));
        }
    }

    /**
     * Verifies that {@link IgniteToStringBuilder} doesn't fail while iterating over concurrently modified map.
     *
     * @throws Exception If failed.
     */
    @Test
    public void testToStringCheckConcurrentModificationExceptionFromMap() throws Exception {
        ClassWithMap classWithMap = new ClassWithMap();

        CountDownLatch modificationStartedLatch = new CountDownLatch(1);
        AtomicBoolean finished = new AtomicBoolean(false);

        CompletableFuture<Void> finishFut = CompletableFuture.runAsync(() -> {
            Map<Integer, SlowToStringObject> map = classWithMap.map;
            for (int i = 0; i < 100; i++)
                map.put(i, new SlowToStringObject());

            Random rnd = new Random();

            while (!finished.get()) {
                if (rnd.nextBoolean() && map.size() > 1)
                    map.remove(map.size() / 2);
                else
                    map.put(map.size() / 2, new SlowToStringObject());

                if (modificationStartedLatch.getCount() > 0)
                    modificationStartedLatch.countDown();
            }
        });

        modificationStartedLatch.await();

        String s = null;

        try {
            s = classWithMap.toString();
        }
        finally {
            finished.set(true);

            finishFut.get();

            assertNotNull(s);
            assertTrue(s.contains("concurrent modification"));
        }
    }

    /**
     * Test verifies that when RuntimeException is thrown from toString method of some class
     * IgniteToString builder doesn't fail but finishes building toString representation.
     */
    @Test
    public void testRuntimeExceptionCaught() {
        WrapperForFaultyToStringClass wr = new WrapperForFaultyToStringClass(
            new ClassWithFaultyToString[] {new ClassWithFaultyToString()});

        String strRep = wr.toString();

        //field before faulty field was written successfully to string representation
        assertTrue(strRep.contains("id=12345"));

        //message from RuntimeException was written to string representation
        assertTrue(strRep.contains("toString failed"));

        //field after faulty field was written successfully to string representation
        assertTrue(strRep.contains("str=str"));
    }

    /**
     * Test class.
     */
    @SuppressWarnings("InstanceVariableMayNotBeInitialized")
    private static class Node {
        /**
         *
         */
        @IgniteToStringInclude
        String name;

        /**
         *
         */
        @IgniteToStringInclude
        Node next;

        /**
         *
         */
        @IgniteToStringInclude
        Node[] nodes;

        /** {@inheritDoc} */
        @Override public String toString() {
            return IgniteToStringBuilder.toString(Node.class, this);
        }
    }

    /**
     * Test class.
     */
    private static class BarrierCallable implements Callable<String> {
        /**
         *
         */
        CyclicBarrier bar;

        /**
         *
         */
        Object obj;

        /** Expected value of {@code toString()} method. */
        String exp;

        /**
         *
         */
        private BarrierCallable(CyclicBarrier bar, Object obj, String exp) {
            this.bar = bar;
            this.obj = obj;
            this.exp = exp;
        }

        /** {@inheritDoc} */
        @Override public String call() throws Exception {
            for (int i = 0; i < 10; i++) {
                bar.await();

                assertEquals(exp, obj.toString());
            }

            return null;
        }
    }

    /**
     * Class containing another class with faulty toString implementation
     * to force IgniteToStringBuilder to call faulty toString.
     */
    @SuppressWarnings({"FieldMayBeFinal", "unused"})
    private static class WrapperForFaultyToStringClass {
        /**
         *
         */
        @IgniteToStringInclude
        private int id = 12345;

        /**
         *
         */
        @SuppressWarnings("unused")
        @IgniteToStringInclude
        private ClassWithFaultyToString[] arr;

        /**
         *
         */
        @SuppressWarnings("unused")
        @IgniteToStringInclude
        private String str = "str";

        /**
         *
         */
        WrapperForFaultyToStringClass(ClassWithFaultyToString[] arr) {
            this.arr = arr;
        }

        /** {@inheritDoc} */
        @Override public String toString() {
            return S.toString(WrapperForFaultyToStringClass.class, this);
        }
    }

    /**
     * Class throwing a RuntimeException from a {@link ClassWithFaultyToString#toString()} method.
     */
    private static class ClassWithFaultyToString {
        /** {@inheritDoc} */
        @Override public String toString() {
            throw new RuntimeException("toString failed");
        }
    }

    /**
     * Test class.
     */
    @SuppressWarnings({"InstanceVariableMayNotBeInitialized", "FieldMayBeFinal", "unused", "FieldMayBeStatic"})
    private static class TestClass1 {
        /**
         *
         */
        @IgniteToStringOrder(0)
        private String id = "1234567890";

        /**
         *
         */
        private int intVar;

        /**
         *
         */
        @IgniteToStringInclude(sensitive = true)
        private long longVar;

        /**
         *
         */
        @IgniteToStringOrder(1)
        private UUID uuidVar = UUID.randomUUID();

        /**
         *
         */
        private boolean boolVar;

        /**
         *
         */
        private byte byteVar;

        /**
         *
         */
        private String name = "qwertyuiopasdfghjklzxcvbnm";

        /**
         *
         */
        private final Integer finalInt = 2;

        /**
         *
         */
        private List<String> strList;

        /**
         *
         */
        @IgniteToStringInclude
        private Map<String, String> strMap;

        /**
         *
         */
        @IgniteToStringInclude
        private List<String> strListIncl;

        /**
         *
         */
        private Object obj = new Object();

        /**
         *
         */
        private ReadWriteLock lock;

        /**
         * @return Manual string.
         */
        String toStringManual() {
            StringBuilder buf = new StringBuilder();

            buf.append(getClass().getSimpleName()).append(" [");

            buf.append("id=").append(id).append(", ");
            buf.append("uuidVar=").append(uuidVar).append(", ");
            buf.append("intVar=").append(intVar).append(", ");
            if (S.includeSensitive())
                buf.append("longVar=").append(longVar).append(", ");
            buf.append("boolVar=").append(boolVar).append(", ");
            buf.append("byteVar=").append(byteVar).append(", ");
            buf.append("name=").append(name).append(", ");
            buf.append("finalInt=").append(finalInt).append(", ");
            buf.append("strMap=").append(strMap).append(", ");
            buf.append("strListIncl=").append(strListIncl);

            buf.append("]");

            return buf.toString();
        }

        /**
         * @return Automatic string.
         */
        String toStringAutomatic() {
            return S.toString(TestClass1.class, this);
        }

        /**
         * @return Automatic string with additional parameters.
         */
        String toStringWithAdditionalAutomatic() {
            return S.toString(TestClass1.class, this, "newParam1", 1, false, "newParam2", 2, true);
        }

        /**
         * @return Manual string with additional parameters.
         */
        String toStringWithAdditionalManual() {
            StringBuilder s = new StringBuilder(toStringManual());
            s.setLength(s.length() - 1);
            s.append(", newParam1=").append(1);
            if (S.includeSensitive())
                s.append(", newParam2=").append(2);
            s.append(']');
            return s.toString();
        }
    }

    /**
     *
     */
    @SuppressWarnings({"InstanceVariableMayNotBeInitialized", "FieldMayBeFinal", "unused"})
    private static class TestClass2 {
        /**
         *
         */
        @SuppressWarnings("unused")
        @IgniteToStringInclude
        private String str;

        /**
         *
         */
        @IgniteToStringInclude
        private Object[] nullArr;

        /**
         * @param str String.
         */
        TestClass2(String str) {
            this.str = str;
        }
    }

    /**
     *
     */
    @SuppressWarnings("FieldMayBeFinal")
    private static class ClassWithList {
        /**
         *
         */
        @IgniteToStringInclude
        private List<SlowToStringObject> list = new LinkedList<>();

        /** {@inheritDoc} */
        @Override public String toString() {
            return S.toString(ClassWithList.class, this);
        }
    }

    /**
     *
     */
    @SuppressWarnings("FieldMayBeFinal")
    private static class ClassWithMap {
        /**
         *
         */
        @IgniteToStringInclude
        private Map<Integer, SlowToStringObject> map = new HashMap<>();

        /** {@inheritDoc} */
        @Override public String toString() {
            return S.toString(ClassWithMap.class, this);
        }
    }

    /**
     * Class sleeps a short quanta of time to increase chances of data race
     * between {@link IgniteToStringBuilder} iterating over collection  user thread concurrently modifying it.
     */
    private static class SlowToStringObject {
        /** {@inheritDoc} */
        @Override public String toString() {
            try {
                Thread.sleep(1);
            }
            catch (InterruptedException e) {
                log.error(e.getMessage(), e);
            }

            return super.toString();
        }
    }

    /**
     *
     */
    @SuppressWarnings({"InstanceVariableMayNotBeInitialized", "MismatchedReadAndWriteOfArray", "unused", "FieldMayBeFinal"})
    private static class Parent {
        /**
         *
         */
        private int a;

        /**
         *
         */
        @IgniteToStringInclude
        private Parent[] pa = new Parent[1];

        /** {@inheritDoc} */
        @Override public String toString() {
            return S.toString(Parent.class, this);
        }
    }

    /**
     *
     */
    @SuppressWarnings({"InstanceVariableMayNotBeInitialized", "MismatchedReadAndWriteOfArray", "unused", "FieldMayBeFinal"})
    private static class Child extends Parent {
        /**
         *
         */
        private int b;

        /**
         *
         */
        @IgniteToStringInclude
        private Parent[] pb = new Parent[1];

        /** {@inheritDoc} */
        @Override public String toString() {
            return S.toString(Child.class, this, super.toString());
        }
    }

    /**
     *
     */
    private static class Wrapper {
        /**
         *
         */
        @IgniteToStringInclude
        Parent p = new Child();

        /** {@inheritDoc} */
        @Override public String toString() {
            return S.toString(Wrapper.class, this);
        }
    }
}
