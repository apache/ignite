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

package org.apache.ignite.internal.util.tostring;

import java.lang.reflect.Array;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import java.util.UUID;
import java.util.concurrent.Callable;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.locks.ReadWriteLock;
import org.apache.ignite.IgniteLogger;
import org.apache.ignite.IgniteSystemProperties;
import org.apache.ignite.internal.IgniteInternalFuture;
import org.apache.ignite.internal.util.typedef.internal.S;
import org.apache.ignite.testframework.GridTestUtils;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.apache.ignite.testframework.junits.common.GridCommonTest;

import static org.apache.ignite.IgniteSystemProperties.IGNITE_TO_STRING_COLLECTION_LIMIT;
import static org.apache.ignite.IgniteSystemProperties.IGNITE_TO_STRING_MAX_LENGTH;

/**
 * Tests for {@link GridToStringBuilder}.
 */
@GridCommonTest(group = "Utils")
public class GridToStringBuilderSelfTest extends GridCommonAbstractTest {
    /**
     * @throws Exception If failed.
     */
    public void testToString() throws Exception {
        TestClass1 obj = new TestClass1();

        IgniteLogger log = log();

        log.info(obj.toStringManual());
        log.info(obj.toStringAutomatic());

        assertEquals (obj.toStringManual(), obj.toStringAutomatic());
    }

    /**
     * @throws Exception If failed.
     */
    public void testToStringWithAdditions() throws Exception {
        TestClass1 obj = new TestClass1();

        IgniteLogger log = log();

        String manual = obj.toStringWithAdditionalManual();
        log.info(manual);

        String automatic = obj.toStringWithAdditionalAutomatic();
        log.info(automatic);

        assertEquals(manual, automatic);
    }

    /**
     * @throws Exception If failed.
     */
    public void testToStringCheckSimpleListRecursionPrevention() throws Exception {
        ArrayList<Object> list1 = new ArrayList<>();
        ArrayList<Object> list2 = new ArrayList<>();

        list2.add(list1);
        list1.add(list2);

        info(GridToStringBuilder.toString(ArrayList.class, list1));
        info(GridToStringBuilder.toString(ArrayList.class, list2));
    }

    /**
     * @throws Exception If failed.
     */
    public void testToStringCheckSimpleMapRecursionPrevention() throws Exception {
        HashMap<Object, Object> map1 = new HashMap<>();
        HashMap<Object, Object> map2 = new HashMap<>();

        map1.put("2", map2);
        map2.put("1", map1);

        info(GridToStringBuilder.toString(HashMap.class, map1));
        info(GridToStringBuilder.toString(HashMap.class, map2));
    }

    /**
     * @throws Exception If failed.
     */
    public void testToStringCheckListAdvancedRecursionPrevention() throws Exception {
        ArrayList<Object> list1 = new ArrayList<>();
        ArrayList<Object> list2 = new ArrayList<>();

        list2.add(list1);
        list1.add(list2);

        info(GridToStringBuilder.toString(ArrayList.class, list1, "name", list2));
        info(GridToStringBuilder.toString(ArrayList.class, list2, "name", list1));
    }

    /**
     * @throws Exception If failed.
     */
    public void testToStringCheckMapAdvancedRecursionPrevention() throws Exception {
        HashMap<Object, Object> map1 = new HashMap<>();
        HashMap<Object, Object> map2 = new HashMap<>();

        map1.put("2", map2);
        map2.put("1", map1);

        info(GridToStringBuilder.toString(HashMap.class, map1, "name", map2));
        info(GridToStringBuilder.toString(HashMap.class, map2, "name", map1));
    }

    /**
     * @throws Exception If failed.
     */
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

        info(expN1);
        info(expN2);
        info(expN3);
        info(expN4);
        info(GridToStringBuilder.toString("Test", "Appended vals", n1));

        CyclicBarrier bar = new CyclicBarrier(4);

        IgniteInternalFuture<String> fut1 = GridTestUtils.runAsync(new BarrierCallable(bar, n1, expN1));
        IgniteInternalFuture<String> fut2 = GridTestUtils.runAsync(new BarrierCallable(bar, n2, expN2));
        IgniteInternalFuture<String> fut3 = GridTestUtils.runAsync(new BarrierCallable(bar, n3, expN3));
        IgniteInternalFuture<String> fut4 = GridTestUtils.runAsync(new BarrierCallable(bar, n4, expN4));

        fut1.get(3_000);
        fut2.get(3_000);
        fut3.get(3_000);
        fut4.get(3_000);
    }

    /**
     * Test class.
     */
    private static class Node {
        /** */
        @GridToStringInclude
        String name;

        /** */
        @GridToStringInclude
        Node next;

        /** */
        @GridToStringInclude
        Node[] nodes;

        /** {@inheritDoc} */
        @Override public String toString() {
            return GridToStringBuilder.toString(Node.class, this);
        }
    }

    /**
     * Test class.
     */
    private static class BarrierCallable implements Callable<String> {
        /** */
        CyclicBarrier bar;

        /** */
        Object obj;

        /** Expected value of {@code toString()} method. */
        String exp;

        /** */
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
     * JUnit.
     */
    public void testToStringPerformance() {
        TestClass1 obj = new TestClass1();

        IgniteLogger log = log();

        // Warm up.
        obj.toStringAutomatic();

        long start = System.currentTimeMillis();

        for (int i = 0; i < 100000; i++)
            obj.toStringManual();

        log.info("Manual toString() took: " + (System.currentTimeMillis() - start) + "ms");

        start = System.currentTimeMillis();

        for (int i = 0; i < 100000; i++)
            obj.toStringAutomatic();

        log.info("Automatic toString() took: " + (System.currentTimeMillis() - start) + "ms");
    }

    /**
     * Test array print.
     * @param v value to get array class and fill array.
     * @param limit value of IGNITE_TO_STRING_COLLECTION_LIMIT.
     * @throws Exception if failed.
     */
    private <T, V> void testArr(V v, int limit) throws Exception {
        T[] arrOf = (T[]) Array.newInstance(v.getClass(), limit + 1);
        Arrays.fill(arrOf, v);
        T[] arr = Arrays.copyOf(arrOf, limit);

        checkArrayOverflow(arrOf, arr, limit);
    }

    /**
     * Test array print.
     *
     * @throws Exception if failed.
     */
    public void testArrLimitWithRecursion() throws Exception {
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
        String arrStr = GridToStringBuilder.arrayToString(arr.getClass(), arr);
        String arrOfStr = GridToStringBuilder.arrayToString(arrOf.getClass(), arrOf);

        // Simulate overflow
        StringBuilder resultSB = new StringBuilder(arrStr);
            resultSB.deleteCharAt(resultSB.length()-1);
            resultSB.append("... and ").append(arrOf.length - limit).append(" more]");

        arrStr = resultSB.toString();

        info(arrOfStr);
        info(arrStr);

        assertTrue("Collection limit error in array of type " + arrOf.getClass().getName()
            + " error, normal arr: <" + arrStr + ">, overflowed arr: <" + arrOfStr + ">", arrStr.equals(arrOfStr));
    }

    /**
     * @throws Exception If failed.
     */
    public void testToStringCollectionLimits() throws Exception {
        int limit = IgniteSystemProperties.getInteger(IGNITE_TO_STRING_COLLECTION_LIMIT, 100);

        Object vals[] = new Object[] {Byte.MIN_VALUE, Boolean.TRUE, Short.MIN_VALUE, Integer.MIN_VALUE, Long.MIN_VALUE,
            Float.MIN_VALUE, Double.MIN_VALUE, Character.MIN_VALUE, new TestClass1()};
        for (Object val : vals)
            testArr(val, limit);

        byte[] byteArr = new byte[1];
        byteArr[0] = 1;
        assertEquals(Arrays.toString(byteArr), GridToStringBuilder.arrayToString(byteArr.getClass(), byteArr));
        byteArr = Arrays.copyOf(byteArr, 101);
        assertTrue("Can't find \"... and 1 more\" in overflowed array string!",
            GridToStringBuilder.arrayToString(byteArr.getClass(), byteArr).contains("... and 1 more"));

        boolean[] boolArr = new boolean[1];
        boolArr[0] = true;
        assertEquals(Arrays.toString(boolArr), GridToStringBuilder.arrayToString(boolArr.getClass(), boolArr));
        boolArr = Arrays.copyOf(boolArr, 101);
        assertTrue("Can't find \"... and 1 more\" in overflowed array string!",
            GridToStringBuilder.arrayToString(boolArr.getClass(), boolArr).contains("... and 1 more"));

        short[] shortArr = new short[1];
        shortArr[0] = 100;
        assertEquals(Arrays.toString(shortArr), GridToStringBuilder.arrayToString(shortArr.getClass(), shortArr));
        shortArr = Arrays.copyOf(shortArr, 101);
        assertTrue("Can't find \"... and 1 more\" in overflowed array string!",
            GridToStringBuilder.arrayToString(shortArr.getClass(), shortArr).contains("... and 1 more"));

        int[] intArr = new int[1];
        intArr[0] = 10000;
        assertEquals(Arrays.toString(intArr), GridToStringBuilder.arrayToString(intArr.getClass(), intArr));
        intArr = Arrays.copyOf(intArr, 101);
        assertTrue("Can't find \"... and 1 more\" in overflowed array string!",
            GridToStringBuilder.arrayToString(intArr.getClass(), intArr).contains("... and 1 more"));

        long[] longArr = new long[1];
        longArr[0] = 10000000;
        assertEquals(Arrays.toString(longArr), GridToStringBuilder.arrayToString(longArr.getClass(), longArr));
        longArr = Arrays.copyOf(longArr, 101);
        assertTrue("Can't find \"... and 1 more\" in overflowed array string!",
            GridToStringBuilder.arrayToString(longArr.getClass(), longArr).contains("... and 1 more"));

        float[] floatArr = new float[1];
        floatArr[0] = 1.f;
        assertEquals(Arrays.toString(floatArr), GridToStringBuilder.arrayToString(floatArr.getClass(), floatArr));
        floatArr = Arrays.copyOf(floatArr, 101);
        assertTrue("Can't find \"... and 1 more\" in overflowed array string!",
            GridToStringBuilder.arrayToString(floatArr.getClass(), floatArr).contains("... and 1 more"));

        double[] doubleArr = new double[1];
        doubleArr[0] = 1.;
        assertEquals(Arrays.toString(doubleArr), GridToStringBuilder.arrayToString(doubleArr.getClass(), doubleArr));
        doubleArr = Arrays.copyOf(doubleArr, 101);
        assertTrue("Can't find \"... and 1 more\" in overflowed array string!",
            GridToStringBuilder.arrayToString(doubleArr.getClass(), doubleArr).contains("... and 1 more"));

        char[] charArr = new char[1];
        charArr[0] = 'a';
        assertEquals(Arrays.toString(charArr), GridToStringBuilder.arrayToString(charArr.getClass(), charArr));
        charArr = Arrays.copyOf(charArr, 101);
        assertTrue("Can't find \"... and 1 more\" in overflowed array string!",
            GridToStringBuilder.arrayToString(charArr.getClass(), charArr).contains("... and 1 more"));

        Map<String, String> strMap = new TreeMap<>();
        List<String> strList = new ArrayList<>(limit+1);

        TestClass1 testClass = new TestClass1();
        testClass.strMap = strMap;
        testClass.strListIncl = strList;

        for (int i = 0; i < limit; i++) {
            strMap.put("k" + i, "v");
            strList.add("e");
        }

        checkColAndMap(testClass);
    }

    /**
     * @throws Exception If failed.
     */
    public void testToStringColAndMapLimitWithRecursion() throws Exception {
        int limit = IgniteSystemProperties.getInteger(IGNITE_TO_STRING_COLLECTION_LIMIT, 100);
        Map strMap = new TreeMap<>();
        List strList = new ArrayList<>(limit+1);

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
        String testClsStr = GridToStringBuilder.toString(TestClass1.class, testCls);

        testCls.strMap.put("kz", "v"); // important to add last element in TreeMap here
        testCls.strListIncl.add("e");

        String testClsStrOf = GridToStringBuilder.toString(TestClass1.class, testCls);

        String testClsStrOfR = testClsStrOf.replaceAll("... and 1 more","");

        info(testClsStr);
        info(testClsStrOf);
        info(testClsStrOfR);

        assertTrue("Collection limit error in Map or List, normal: <" + testClsStr + ">, overflowed: <"
            + testClsStrOf + ">", testClsStr.length() == testClsStrOfR.length());
    }

    /**
     * @throws Exception If failed.
     */
    public void testToStringSizeLimits() throws Exception {
        int limit = IgniteSystemProperties.getInteger(IGNITE_TO_STRING_MAX_LENGTH, 10_000);
        int tailLen = limit / 10 * 2;
        StringBuilder sb = new StringBuilder(limit + 10);
        for (int i = 0; i < limit - 100; i++) {
            sb.append('a');
        }
        String actual = GridToStringBuilder.toString(TestClass2.class, new TestClass2(sb.toString()));
        String expected = "TestClass2 [str=" + sb.toString() + ", nullArr=null]";
        assertEquals(expected, actual);

        for (int i = 0; i < 110; i++) {
            sb.append('b');
        }
        actual = GridToStringBuilder.toString(TestClass2.class, new TestClass2(sb.toString()));
        expected = "TestClass2 [str=" + sb.toString() + ", nullArr=null]";
        assertEquals(expected.substring(0, limit - tailLen), actual.substring(0, limit - tailLen));
        assertEquals(expected.substring(expected.length() - tailLen), actual.substring(actual.length() - tailLen));
        assertTrue(actual.contains("... and"));
        assertTrue(actual.contains("skipped ..."));
    }

    /**
     * Test class.
     */
    private static class TestClass1 {
        /** */
        @SuppressWarnings("unused")
        @GridToStringOrder(0)
        private String id = "1234567890";

        /** */
        @SuppressWarnings("unused")
        private int intVar;

        /** */
        @SuppressWarnings("unused")
        @GridToStringInclude(sensitive = true)
        private long longVar;

        /** */
        @SuppressWarnings("unused")
        @GridToStringOrder(1)
        private final UUID uuidVar = UUID.randomUUID();

        /** */
        @SuppressWarnings("unused")
        private boolean boolVar;

        /** */
        @SuppressWarnings("unused")
        private byte byteVar;

        /** */
        @SuppressWarnings("unused")
        private String name = "qwertyuiopasdfghjklzxcvbnm";

        /** */
        @SuppressWarnings("unused")
        private final Integer finalInt = 2;

        /** */
        @SuppressWarnings("unused")
        private List<String> strList;

        /** */
        @SuppressWarnings("unused")
        @GridToStringInclude
        private Map<String, String> strMap;

        /** */
        @SuppressWarnings("unused")
        @GridToStringInclude
        private List<String> strListIncl;


        /** */
        @SuppressWarnings("unused")
        private final Object obj = new Object();

        /** */
        @SuppressWarnings("unused")
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
            if (S.INCLUDE_SENSITIVE)
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
            if (S.INCLUDE_SENSITIVE)
                s.append(", newParam2=").append(2);
            s.append(']');
            return s.toString();
        }
    }

    /**
     *
     */
    private static class TestClass2{
        /** */
        @SuppressWarnings("unused")
        @GridToStringInclude
        private String str;

        /** */
        @GridToStringInclude
        private Object[] nullArr;

        /**
         * @param str String.
         */
        public TestClass2(String str) {
            this.str = str;
        }
    }
}
