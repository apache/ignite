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

import java.math.BigDecimal;
import java.sql.Timestamp;
import java.util.Date;
import java.util.UUID;
import org.apache.ignite.configuration.IgniteConfiguration;

/**
 * Unit tests for serialized field comparer.
 */
public class BinarySerialiedFieldComparatorSelfTest extends BinarySerialiedFieldComparatorAbstractSelfTest {
    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String igniteInstanceName) throws Exception {
        IgniteConfiguration cfg =  super.getConfiguration(igniteInstanceName);

        cfg.setMarshaller(new BinaryMarshaller());

        return cfg;
    }

    /**
     * Test byte fields.
     *
     * @throws Exception If failed.
     */
    public void testByte() throws Exception {
        checkTwoValues((byte)1, (byte)2);
    }

    /**
     * Test boolean fields.
     *
     * @throws Exception If failed.
     */
    public void testBoolean() throws Exception {
        checkTwoValues(true, false);
    }

    /**
     * Test short fields.
     *
     * @throws Exception If failed.
     */
    public void testShort() throws Exception {
        checkTwoValues((short)1, (short)2);
    }

    /**
     * Test char fields.
     *
     * @throws Exception If failed.
     */
    public void testChar() throws Exception {
        checkTwoValues('a', 'b');
    }

    /**
     * Test int fields.
     *
     * @throws Exception If failed.
     */
    public void testInt() throws Exception {
        checkTwoValues(1, 2);
    }

    /**
     * Test long fields.
     *
     * @throws Exception If failed.
     */
    public void testLong() throws Exception {
        checkTwoValues(1L, 2L);
    }

    /**
     * Test float fields.
     *
     * @throws Exception If failed.
     */
    public void testFloat() throws Exception {
        checkTwoValues(1.0f, 2.0f);
    }

    /**
     * Test double fields.
     *
     * @throws Exception If failed.
     */
    public void testDouble() throws Exception {
        checkTwoValues(1.0d, 2.0d);
    }

    /**
     * Test string fields.
     *
     * @throws Exception If failed.
     */
    public void testString() throws Exception {
        checkTwoValues("str1", "str2");
    }

    /**
     * Test date fields.
     *
     * @throws Exception If failed.
     */
    public void testDate() throws Exception {
        long time = System.currentTimeMillis();

        checkTwoValues(new Date(time), new Date(time + 100));
    }

    /**
     * Test date fields.
     *
     * @throws Exception If failed.
     */
    public void testTimestamp() throws Exception {
        long time = System.currentTimeMillis();

        checkTwoValues(new Timestamp(time), new Timestamp(time + 100));
    }

    /**
     * Test UUID fields.
     *
     * @throws Exception If failed.
     */
    public void testUuid() throws Exception {
        checkTwoValues(UUID.randomUUID(), UUID.randomUUID());
    }

    /**
     * Test decimal fields.
     *
     * @throws Exception If failed.
     */
    public void testDecimal() throws Exception {
        checkTwoValues(new BigDecimal("12.3E+7"), new BigDecimal("12.4E+7"));
        checkTwoValues(new BigDecimal("12.3E+7"), new BigDecimal("12.3E+8"));
    }

    /**
     * Test object fields.
     *
     * @throws Exception If failed.
     */
    public void testInnerObject() throws Exception {
        checkTwoValues(new InnerClass(1), new InnerClass(2));
    }

    /**
     * Test byte array fields.
     *
     * @throws Exception If failed.
     */
    public void testByteArray() throws Exception {
        checkTwoValues(new byte[] { 1, 2 }, new byte[] { 1, 3 });
        checkTwoValues(new byte[] { 1, 2 }, new byte[] { 1 });
        checkTwoValues(new byte[] { 1, 2 }, new byte[] { 3 });
        checkTwoValues(new byte[] { 1, 2 }, new byte[] { 1, 2, 3 });
    }

    /**
     * Test boolean array fields.
     *
     * @throws Exception If failed.
     */
    public void testBooleanArray() throws Exception {
        checkTwoValues(new boolean[] { true, false }, new boolean[] { false, true });
        checkTwoValues(new boolean[] { true, false }, new boolean[] { true });
        checkTwoValues(new boolean[] { true, false }, new boolean[] { false });
        checkTwoValues(new boolean[] { true, false }, new boolean[] { true, false, true });
    }

    /**
     * Test short array fields.
     *
     * @throws Exception If failed.
     */
    public void testShortArray() throws Exception {
        checkTwoValues(new short[] { 1, 2 }, new short[] { 1, 3 });
        checkTwoValues(new short[] { 1, 2 }, new short[] { 1 });
        checkTwoValues(new short[] { 1, 2 }, new short[] { 3 });
        checkTwoValues(new short[] { 1, 2 }, new short[] { 1, 2, 3 });
    }

    /**
     * Test char array fields.
     *
     * @throws Exception If failed.
     */
    public void testCharArray() throws Exception {
        checkTwoValues(new char[] { 1, 2 }, new char[] { 1, 3 });
        checkTwoValues(new char[] { 1, 2 }, new char[] { 1 });
        checkTwoValues(new char[] { 1, 2 }, new char[] { 3 });
        checkTwoValues(new char[] { 1, 2 }, new char[] { 1, 2, 3 });
    }

    /**
     * Test int array fields.
     *
     * @throws Exception If failed.
     */
    public void testIntArray() throws Exception {
        checkTwoValues(new int[] { 1, 2 }, new int[] { 1, 3 });
        checkTwoValues(new int[] { 1, 2 }, new int[] { 1 });
        checkTwoValues(new int[] { 1, 2 }, new int[] { 3 });
        checkTwoValues(new int[] { 1, 2 }, new int[] { 1, 2, 3 });
    }

    /**
     * Test long array fields.
     *
     * @throws Exception If failed.
     */
    public void testLongArray() throws Exception {
        checkTwoValues(new long[] { 1, 2 }, new long[] { 1, 3 });
        checkTwoValues(new long[] { 1, 2 }, new long[] { 1 });
        checkTwoValues(new long[] { 1, 2 }, new long[] { 3 });
        checkTwoValues(new long[] { 1, 2 }, new long[] { 1, 2, 3 });
    }

    /**
     * Test float array fields.
     *
     * @throws Exception If failed.
     */
    public void testFloatArray() throws Exception {
        checkTwoValues(new float[] { 1.0f, 2.0f }, new float[] { 1.0f, 3.0f });
        checkTwoValues(new float[] { 1.0f, 2.0f }, new float[] { 1.0f });
        checkTwoValues(new float[] { 1.0f, 2.0f }, new float[] { 3.0f });
        checkTwoValues(new float[] { 1.0f, 2.0f }, new float[] { 1.0f, 2.0f, 3.0f });
    }

    /**
     * Test double array fields.
     *
     * @throws Exception If failed.
     */
    public void testDoubleArray() throws Exception {
        checkTwoValues(new double[] { 1.0d, 2.0d }, new double[] { 1.0d, 3.0d });
        checkTwoValues(new double[] { 1.0d, 2.0d }, new double[] { 1.0d });
        checkTwoValues(new double[] { 1.0d, 2.0d }, new double[] { 3.0d });
        checkTwoValues(new double[] { 1.0d, 2.0d }, new double[] { 1.0d, 2.0d, 3.0d });
    }

    /**
     * Test string array fields.
     *
     * @throws Exception If failed.
     */
    public void testStringArray() throws Exception {
        checkTwoValues(new String[] { "a", "b" }, new String[] { "a", "c" });
        checkTwoValues(new String[] { "a", "b" }, new String[] { "a" });
        checkTwoValues(new String[] { "a", "b" }, new String[] { "c" });
        checkTwoValues(new String[] { "a", "b" }, new String[] { "a", "b", "c" });
    }

    /**
     * Test date array fields.
     *
     * @throws Exception If failed.
     */
    public void testDateArray() throws Exception {
        long curTime = System.currentTimeMillis();

        Date v1 = new Date(curTime);
        Date v2 = new Date(curTime + 1000);
        Date v3 = new Date(curTime + 2000);

        checkTwoValues(new Date[] { v1, v2 }, new Date[] { v1, v3 });
        checkTwoValues(new Date[] { v1, v2 }, new Date[] { v1 });
        checkTwoValues(new Date[] { v1, v2 }, new Date[] { v3 });
        checkTwoValues(new Date[] { v1, v2 }, new Date[] { v1, v2, v3 });
    }

    /**
     * Test timestamp array fields.
     *
     * @throws Exception If failed.
     */
    public void testTimestampArray() throws Exception {
        long curTime = System.currentTimeMillis();

        Timestamp v1 = new Timestamp(curTime);
        Timestamp v2 = new Timestamp(curTime + 1000);
        Timestamp v3 = new Timestamp(curTime + 2000);

        checkTwoValues(new Timestamp[] { v1, v2 }, new Timestamp[] { v1, v3 });
        checkTwoValues(new Timestamp[] { v1, v2 }, new Timestamp[] { v1 });
        checkTwoValues(new Timestamp[] { v1, v2 }, new Timestamp[] { v3 });
        checkTwoValues(new Timestamp[] { v1, v2 }, new Timestamp[] { v1, v2, v3 });
    }

    /**
     * Test UUID array fields.
     *
     * @throws Exception If failed.
     */
    public void testUuidArray() throws Exception {
        UUID v1 = UUID.randomUUID();
        UUID v2 = UUID.randomUUID();
        UUID v3 = UUID.randomUUID();

        checkTwoValues(new UUID[] { v1, v2 }, new UUID[] { v1, v3 });
        checkTwoValues(new UUID[] { v1, v2 }, new UUID[] { v1 });
        checkTwoValues(new UUID[] { v1, v2 }, new UUID[] { v3 });
        checkTwoValues(new UUID[] { v1, v2 }, new UUID[] { v1, v2, v3 });
    }

    /**
     * Test decimal array fields.
     *
     * @throws Exception If failed.
     */
    public void testDecimalArray() throws Exception {
        BigDecimal v1 = new BigDecimal("12.3E+7");
        BigDecimal v2 = new BigDecimal("12.4E+7");
        BigDecimal v3 = new BigDecimal("12.5E+7");

        checkTwoValues(new BigDecimal[] { v1, v2 }, new BigDecimal[] { v1, v3 });
        checkTwoValues(new BigDecimal[] { v1, v2 }, new BigDecimal[] { v1 });
        checkTwoValues(new BigDecimal[] { v1, v2 }, new BigDecimal[] { v3 });
        checkTwoValues(new BigDecimal[] { v1, v2 }, new BigDecimal[] { v1, v2, v3 });

        v2 = new BigDecimal("12.3E+8");
        v3 = new BigDecimal("12.3E+9");

        checkTwoValues(new BigDecimal[] { v1, v2 }, new BigDecimal[] { v1, v3 });
        checkTwoValues(new BigDecimal[] { v1, v2 }, new BigDecimal[] { v1 });
        checkTwoValues(new BigDecimal[] { v1, v2 }, new BigDecimal[] { v3 });
        checkTwoValues(new BigDecimal[] { v1, v2 }, new BigDecimal[] { v1, v2, v3 });
    }

    /**
     * Test object array fields.
     *
     * @throws Exception If failed.
     */
    public void testInnerObjectArray() throws Exception {
        InnerClass v1 = new InnerClass(1);
        InnerClass v2 = new InnerClass(2);
        InnerClass v3 = new InnerClass(3);

        checkTwoValues(new InnerClass[] { v1, v2 }, new InnerClass[] { v1, v3 });
        checkTwoValues(new InnerClass[] { v1, v2 }, new InnerClass[] { v1 });
        checkTwoValues(new InnerClass[] { v1, v2 }, new InnerClass[] { v3 });
        checkTwoValues(new InnerClass[] { v1, v2 }, new InnerClass[] { v1, v2, v3 });
    }

    /**
     * Inner class.
     */
    @SuppressWarnings("unused")
    private static class InnerClass {
        /** Value. */
        private int val;

        /**
         * Constructor.
         *
         * @param val Value.
         */
        public InnerClass(int val) {
            this.val = val;
        }
    }
}