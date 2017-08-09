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

import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.ignite.binary.BinaryObjectBuilder;
import org.apache.ignite.internal.util.GridUnsafe;
import org.apache.ignite.internal.util.typedef.F;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.eclipse.jetty.util.ConcurrentHashSet;

/**
 * Unit tests for serialized field comparer.
 */
public class BinarySerializedFieldComparatorAbstractSelfTest extends GridCommonAbstractTest {
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
        BinarySerializedFieldComparator firstComp = first.createFieldComparator();
        BinarySerializedFieldComparator secondComp = second.createFieldComparator();

        // Compare expected result.
        firstComp.findField(singleFieldOrder(first));
        secondComp.findField(singleFieldOrder(second));

        assertEquals(expRes, BinarySerializedFieldComparator.equals(firstComp, secondComp));
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
}