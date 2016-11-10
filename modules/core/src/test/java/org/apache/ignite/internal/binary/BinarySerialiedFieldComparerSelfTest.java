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

import org.apache.ignite.binary.BinaryObjectBuilder;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.util.GridUnsafe;
import org.apache.ignite.internal.util.typedef.F;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.eclipse.jetty.util.ConcurrentHashSet;

import java.math.BigDecimal;
import java.sql.Timestamp;
import java.util.Date;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Unit tests for serialized field comparer.
 */
public class BinarySerialiedFieldComparerSelfTest extends GridCommonAbstractTest {
    /** Type counter. */
    private static final AtomicInteger TYPE_CTR = new AtomicInteger();

    /** Single field name. */
    private static final String FIELD_SINGLE = "single";

    /** Pointers to release. */
    private final Set<Long> ptrs = new ConcurrentHashSet<>();

    /** {@inheritDoc} */
    @Override protected void beforeTestsStarted() throws Exception {
        super.beforeTestsStarted();

        startGrid();
    }

    /** {@inheritDoc} */
    @Override protected void afterTestsStopped() throws Exception {
        stopAllGrids();

        super.afterTestsStopped();
    }

    /** {@inheritDoc} */
    @Override protected void beforeTest() throws Exception {
        super.beforeTest();

        TYPE_CTR.incrementAndGet();
    }

    /** {@inheritDoc} */
    @Override protected void afterTest() throws Exception {
        for (Long ptr : ptrs)
            GridUnsafe.freeMemory(ptr);

        super.afterTest();
    }

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String gridName) throws Exception {
        IgniteConfiguration cfg =  super.getConfiguration(gridName);

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
     * Test byte fields.
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
     * Check two different not-null values.
     *
     * @throws Exception If failed.
     */
    public void checkTwoValues(Object val1, Object val2) throws Exception {
        checkTwoValues(val1, val2, false, false);
        checkTwoValues(val1, val2, false, true);
        checkTwoValues(val1, val2, true, false);
        checkTwoValues(val1, val2, true, true);
    }

    /**
     * Check two different not-null values.
     *
     * @param val1 Value 1.
     * @param val2 Value 2.
     * @param offheap1 Offheap flag 1.
     * @param offheap2 Offheap flag 2.
     * @throws Exception If failed.
     */
    public void checkTwoValues(Object val1, Object val2, boolean offheap1, boolean offheap2) throws Exception {
        assertNotNull(val1);
        assertNotNull(val2);

        compareSingle(convert(buildSingle(val1), offheap1), convert(buildSingle(val1), offheap2), true);
        compareSingle(convert(buildSingle(val1), offheap1), convert(buildSingle(val2), offheap2), false);
        compareSingle(convert(buildSingle(val1), offheap1), convert(buildSingle(null), offheap2), false);
        compareSingle(convert(buildSingle(val1), offheap1), convert(buildEmpty(), offheap2), false);

        compareSingle(convert(buildSingle(val2), offheap1), convert(buildSingle(val1), offheap2), false);
        compareSingle(convert(buildSingle(val2), offheap1), convert(buildSingle(val2), offheap2), true);
        compareSingle(convert(buildSingle(val2), offheap1), convert(buildSingle(null), offheap2), false);
        compareSingle(convert(buildSingle(val2), offheap1), convert(buildEmpty(), offheap2), false);

        compareSingle(convert(buildSingle(null), offheap1), convert(buildSingle(val1), offheap2), false);
        compareSingle(convert(buildSingle(null), offheap1), convert(buildSingle(val2), offheap2), false);
        compareSingle(convert(buildSingle(null), offheap1), convert(buildSingle(null), offheap2), true);
        compareSingle(convert(buildSingle(null), offheap1), convert(buildEmpty(), offheap2), true);

        compareSingle(convert(buildEmpty(), offheap1), convert(buildSingle(val1), offheap2), false);
        compareSingle(convert(buildEmpty(), offheap1), convert(buildSingle(val2), offheap2), false);
        compareSingle(convert(buildEmpty(), offheap1), convert(buildSingle(null), offheap2), true);
        compareSingle(convert(buildEmpty(), offheap1), convert(buildEmpty(), offheap2), true);
    }

    /**
     * Compare single field.
     *
     * @param first First object.
     * @param second Second object.
     * @param expRes Expected result.
     */
    private void compareSingle(BinaryObjectExImpl first, BinaryObjectExImpl second, boolean expRes) {
        BinarySerializedFieldComparer firstComp = first.createFieldComparer();
        BinarySerializedFieldComparer secondComp = second.createFieldComparer();

        // Compare expected result.
        firstComp.findField(singleFieldOrder(first));
        secondComp.findField(singleFieldOrder(second));

        assertEquals(expRes, BinarySerializedFieldComparer.equals(firstComp, secondComp));
    }

    /**
     * Get single field order.
     *
     * @param obj Object.
     * @return Order.
     */
    private int singleFieldOrder(BinaryObjectExImpl obj) {
        return obj.hasField(FIELD_SINGLE) ? 0 : BinarySchema.ORDER_NOT_FOUND;
    }

    /**
     * Convert binary object to it's final state.
     *
     * @param obj Object.
     * @param offheap Offheap flag.
     * @return Result.
     */
    private BinaryObjectExImpl convert(BinaryObjectExImpl obj, boolean offheap) {
        if (offheap) {
            byte[] arr = obj.array();

            long ptr = GridUnsafe.allocateMemory(arr.length);

            ptrs.add(ptr);

            GridUnsafe.copyMemory(arr, GridUnsafe.BYTE_ARR_OFF, null, ptr, arr.length);

            obj = new BinaryObjectOffheapImpl(obj.context(), ptr, 0, obj.array().length);
        }

        return obj;
    }

    /**
     * Build object with a single field.
     *
     * @param val Value.
     * @return Result.
     */
    private BinaryObjectImpl buildSingle(Object val) {
        return build(FIELD_SINGLE, val);
    }

    /**
     * Build empty object.
     *
     * @return Empty object.
     */
    private BinaryObjectImpl buildEmpty() {
        return build();
    }

    /**
     * Build object.
     *
     * @param parts Parts.
     * @return Result.
     */
    private BinaryObjectImpl build(Object... parts) {
        String typeName = "Type" + TYPE_CTR.get();

        BinaryObjectBuilder builder = grid().binary().builder(typeName);

        if (!F.isEmpty(parts)) {
            for (int i = 0; i < parts.length; )
                builder.setField((String)parts[i++], parts[i++]);
        }

        return (BinaryObjectImpl) builder.build();
    }

    /**
     * Inner class.
     */
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