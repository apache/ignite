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
import java.util.concurrent.locks.ReadWriteLock;
import org.apache.ignite.IgniteLogger;
import org.apache.ignite.IgniteSystemProperties;
import static org.apache.ignite.IgniteSystemProperties.IGNITE_TO_STRING_COLLECTION_LIMIT;
import org.apache.ignite.internal.util.typedef.internal.S;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.apache.ignite.testframework.junits.common.GridCommonTest;

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

        assert obj.toStringManual().equals(obj.toStringAutomatic());
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

        assert manual.equals(automatic);
    }

    /**
     * @throws Exception If failed.
     */
    public void testToStringCheckSimpleRecursionPrevention() throws Exception {
        ArrayList<Object> list1 = new ArrayList<>();
        ArrayList<Object> list2 = new ArrayList<>();

        list2.add(list1);
        list1.add(list2);


        GridToStringBuilder.toString(ArrayList.class, list1);
        GridToStringBuilder.toString(ArrayList.class, list2);
    }

    /**
     * @throws Exception If failed.
     */
    public void testToStringCheckAdvancedRecursionPrevention() throws Exception {
        fail("https://issues.apache.org/jira/browse/IGNITE-602");

        ArrayList<Object> list1 = new ArrayList<>();
        ArrayList<Object> list2 = new ArrayList<>();

        list2.add(list1);
        list1.add(list2);

        GridToStringBuilder.toString(ArrayList.class, list1, "name", list2);
        GridToStringBuilder.toString(ArrayList.class, list2, "name", list1);
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

        String arrStr = GridToStringBuilder.arrayToString(arr.getClass(), arr);
        String arrOfStr = GridToStringBuilder.arrayToString(arrOf.getClass(), arrOf);

        // Simulate overflow
        StringBuilder resultSB = new StringBuilder(arrStr);
            resultSB.deleteCharAt(resultSB.length()-1);
            resultSB.append("... and ").append(arrOf.length - limit).append(" more]");
            arrStr = resultSB.toString();

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

        Map<String, String> strMap = new TreeMap<>();
        List<String> strList = new ArrayList<>(limit+1);

        TestClass1 testClass = new TestClass1();
        testClass.strMap = strMap;
        testClass.strListIncl = strList;

        for (int i = 0; i < limit; i++) {
            strMap.put("k" + i, "v");
            strList.add("e");
        }
        String testClassStr = GridToStringBuilder.toString(TestClass1.class, testClass);

        strMap.put("kz", "v"); // important to add last element in TreeMap here
        strList.add("e");

        String testClassStrOf = GridToStringBuilder.toString(TestClass1.class, testClass);

        String testClassStrOfR = testClassStrOf.replaceAll("... and 1 more","");

        assertTrue("Collection limit error in Map or List, normal: <" + testClassStr + ">, overflowed: <"
            +"testClassStrOf", testClassStr.length() == testClassStrOfR.length());

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
}