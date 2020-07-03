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
import java.util.Arrays;
import java.util.Date;
import java.util.UUID;
import org.apache.ignite.binary.BinaryField;
import org.apache.ignite.binary.BinaryObject;
import org.apache.ignite.binary.BinaryTypeConfiguration;
import org.apache.ignite.configuration.BinaryConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.util.IgniteUtils;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.logger.NullLogger;
import org.apache.ignite.marshaller.MarshallerContextTestImpl;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.junit.Test;

/**
 * Contains tests for binary object fields.
 */
public abstract class BinaryFieldsAbstractSelfTest extends GridCommonAbstractTest {
    /** Marshaller. */
    protected BinaryMarshaller dfltMarsh;

    /**
     * Create marshaller.
     *
     * @return Binary marshaller.
     * @throws Exception If failed.
     */
    protected BinaryMarshaller createMarshaller() throws Exception {
        BinaryContext ctx = new BinaryContext(BinaryCachingMetadataHandler.create(), new IgniteConfiguration(),
            new NullLogger());

        BinaryMarshaller marsh = new BinaryMarshaller();

        BinaryConfiguration bCfg = new BinaryConfiguration();

        bCfg.setCompactFooter(compactFooter());

        bCfg.setTypeConfigurations(Arrays.asList(
            new BinaryTypeConfiguration(TestObject.class.getName()),
            new BinaryTypeConfiguration(TestOuterObject.class.getName()),
            new BinaryTypeConfiguration(TestInnerObject.class.getName())
        ));

        IgniteConfiguration iCfg = new IgniteConfiguration();

        iCfg.setBinaryConfiguration(bCfg);

        marsh.setContext(new MarshallerContextTestImpl(null));

        IgniteUtils.invoke(BinaryMarshaller.class, marsh, "setBinaryContext", ctx, iCfg);

        return marsh;
    }

    /**
     * @return Whether to use compact footer.
     */
    protected boolean compactFooter() {
        return true;
    }

    /**
     * Get binary context for the current marshaller.
     *
     * @param marsh Marshaller.
     * @return Binary context.
     */
    protected static BinaryContext binaryContext(BinaryMarshaller marsh) {
        GridBinaryMarshaller impl = U.field(marsh, "impl");

        return impl.context();
    }

    /** {@inheritDoc} */
    @Override protected void beforeTest() throws Exception {
        super.beforeTest();

        dfltMarsh = createMarshaller();
    }

    /**
     * Test byte field.
     *
     * @throws Exception If failed.
     */
    @Test
    public void testByte() throws Exception {
        check("fByte");
    }

    /**
     * Test byte array field.
     *
     * @throws Exception If failed.
     */
    @Test
    public void testByteArray() throws Exception {
        check("fByteArr");
    }

    /**
     * Test boolean field.
     *
     * @throws Exception If failed.
     */
    @Test
    public void testBoolean() throws Exception {
        check("fBool");
    }

    /**
     * Test boolean array field.
     *
     * @throws Exception If failed.
     */
    @Test
    public void testBooleanArray() throws Exception {
        check("fBoolArr");
    }

    /**
     * Test short field.
     *
     * @throws Exception If failed.
     */
    @Test
    public void testShort() throws Exception {
        check("fShort");
    }

    /**
     * Test short array field.
     *
     * @throws Exception If failed.
     */
    @Test
    public void testShortArray() throws Exception {
        check("fShortArr");
    }

    /**
     * Test char field.
     *
     * @throws Exception If failed.
     */
    @Test
    public void testChar() throws Exception {
        check("fChar");
    }

    /**
     * Test char array field.
     *
     * @throws Exception If failed.
     */
    @Test
    public void testCharArray() throws Exception {
        check("fCharArr");
    }

    /**
     * Test int field.
     *
     * @throws Exception If failed.
     */
    @Test
    public void testInt() throws Exception {
        check("fInt");
    }

    /**
     * Test int array field.
     *
     * @throws Exception If failed.
     */
    @Test
    public void testIntArray() throws Exception {
        check("fIntArr");
    }

    /**
     * Test long field.
     *
     * @throws Exception If failed.
     */
    @Test
    public void testLong() throws Exception {
        check("fLong");
    }

    /**
     * Test long array field.
     *
     * @throws Exception If failed.
     */
    @Test
    public void testLongArray() throws Exception {
        check("fLongArr");
    }

    /**
     * Test float field.
     *
     * @throws Exception If failed.
     */
    @Test
    public void testFloat() throws Exception {
        check("fFloat");
    }

    /**
     * Test float array field.
     *
     * @throws Exception If failed.
     */
    @Test
    public void testFloatArray() throws Exception {
        check("fFloatArr");
    }

    /**
     * Test double field.
     *
     * @throws Exception If failed.
     */
    @Test
    public void testDouble() throws Exception {
        check("fDouble");
    }

    /**
     * Test double array field.
     *
     * @throws Exception If failed.
     */
    @Test
    public void testDoubleArray() throws Exception {
        check("fDoubleArr");
    }

    /**
     * Test string field.
     *
     * @throws Exception If failed.
     */
    @Test
    public void testString() throws Exception {
        check("fString");
    }

    /**
     * Test string array field.
     *
     * @throws Exception If failed.
     */
    @Test
    public void testStringArray() throws Exception {
        check("fStringArr");
    }

    /**
     * Test date field.
     *
     * @throws Exception If failed.
     */
    @Test
    public void testDate() throws Exception {
        check("fDate");
    }

    /**
     * Test date array field.
     *
     * @throws Exception If failed.
     */
    @Test
    public void testDateArray() throws Exception {
        check("fDateArr");
    }

    /**
     * Test timestamp field.
     *
     * @throws Exception If failed.
     */
    @Test
    public void testTimestamp() throws Exception {
        check("fTimestamp");
    }

    /**
     * Test timestamp array field.
     *
     * @throws Exception If failed.
     */
    @Test
    public void testTimestampArray() throws Exception {
        check("fTimestampArr");
    }

    /**
     * Test UUID field.
     *
     * @throws Exception If failed.
     */
    @Test
    public void testUuid() throws Exception {
        check("fUuid");
    }

    /**
     * Test UUID array field.
     *
     * @throws Exception If failed.
     */
    @Test
    public void testUuidArray() throws Exception {
        check("fUuidArr");
    }

    /**
     * Test decimal field.
     *
     * @throws Exception If failed.
     */
    @Test
    public void testDecimal() throws Exception {
        check("fDecimal");
    }

    /**
     * Test decimal array field.
     *
     * @throws Exception If failed.
     */
    @Test
    public void testDecimalArray() throws Exception {
        check("fDecimalArr");
    }

    /**
     * Test object field.
     *
     * @throws Exception If failed.
     */
    @Test
    public void testObject() throws Exception {
        check("fObj");
    }

    /**
     * Test object array field.
     *
     * @throws Exception If failed.
     */
    @Test
    public void testObjectArray() throws Exception {
        check("fObjArr");
    }

    /**
     * Test null field.
     *
     * @throws Exception If failed.
     */
    @Test
    public void testNull() throws Exception {
        check("fNull");
    }

    /**
     * Test missing field.
     *
     * @throws Exception If failed.
     */
    @Test
    public void testMissing() throws Exception {
        String fieldName = "fMissing";

        checkNormal(dfltMarsh, fieldName, false);
        checkNested(dfltMarsh, fieldName, false);
    }

    /**
     * Check field resolution in both normal and nested modes.
     *
     * @param fieldName Field name.
     * @throws Exception If failed.
     */
    public void check(String fieldName) throws Exception {
        checkNormal(dfltMarsh, fieldName, true);
        checkNested(dfltMarsh, fieldName, true);
    }

    /**
     * Check field.
     *
     * @param marsh Marshaller.
     * @param fieldName Field name.
     * @param exists Whether field should exist.
     * @throws Exception If failed.
     */
    private void checkNormal(BinaryMarshaller marsh, String fieldName, boolean exists) throws Exception {
        TestContext testCtx = context(marsh, fieldName);

        check0(fieldName, testCtx, exists);
    }

    /**
     * Check nested field.
     *
     * @param marsh Marshaller.
     * @param fieldName Field name.
     * @param exists Whether field should exist.
     * @throws Exception If failed.
     */
    private void checkNested(BinaryMarshaller marsh, String fieldName, boolean exists) throws Exception {
        TestContext testCtx = nestedContext(marsh, fieldName);

        check0(fieldName, testCtx, exists);
    }

    /**
     * Internal check routine.
     *
     * @param fieldName Field name.
     * @param ctx Context.
     * @param exists Whether field should exist.
     * @throws Exception If failed.
     */
    private void check0(String fieldName, TestContext ctx, boolean exists) throws Exception {
        Object val = ctx.field.value(ctx.portObj);

        if (exists) {
            assertTrue(ctx.field.exists(ctx.portObj));

            Object expVal = U.field(ctx.obj, fieldName);

            if (val instanceof BinaryObject)
                val = ((BinaryObject) val).deserialize();

            if (val != null && val.getClass().isArray()) {
                assertNotNull(expVal);

                if (val instanceof byte[])
                    assertTrue(Arrays.equals((byte[]) expVal, (byte[]) val));
                else if (val instanceof boolean[])
                    assertTrue(Arrays.equals((boolean[]) expVal, (boolean[]) val));
                else if (val instanceof short[])
                    assertTrue(Arrays.equals((short[]) expVal, (short[]) val));
                else if (val instanceof char[])
                    assertTrue(Arrays.equals((char[]) expVal, (char[]) val));
                else if (val instanceof int[])
                    assertTrue(Arrays.equals((int[]) expVal, (int[]) val));
                else if (val instanceof long[])
                    assertTrue(Arrays.equals((long[]) expVal, (long[]) val));
                else if (val instanceof float[])
                    assertTrue(Arrays.equals((float[]) expVal, (float[]) val));
                else if (val instanceof double[])
                    assertTrue(Arrays.equals((double[]) expVal, (double[]) val));
                else {
                    Object[] expVal0 = (Object[])expVal;
                    Object[] val0 = (Object[])val;

                    assertEquals(expVal0.length, val0.length);

                    for (int i = 0; i < expVal0.length; i++) {
                        Object expItem = expVal0[i];
                        Object item = val0[i];

                        if (item instanceof BinaryObject)
                            item = ((BinaryObject)item).deserialize();

                        assertEquals(expItem, item);
                    }
                }
            }
            else
                assertEquals(expVal, val);
        }
        else {
            assertFalse(ctx.field.exists(ctx.portObj));

            assert val == null;
        }
    }

    /**
     * Get test context.
     *
     * @param marsh Binary marshaller.
     * @param fieldName Field name.
     * @return Test context.
     * @throws Exception If failed.
     */
    private TestContext context(BinaryMarshaller marsh, String fieldName) throws Exception {
        TestObject obj = createObject();

        BinaryObjectExImpl portObj = toBinary(marsh, obj);

        BinaryField field = portObj.type().field(fieldName);

        return new TestContext(obj, portObj, field);
    }

    /**
     * Get test context with nested test object.
     *
     * @param marsh Binary marshaller.
     * @param fieldName Field name.
     * @return Test context.
     * @throws Exception If failed.
     */
    private TestContext nestedContext(BinaryMarshaller marsh, String fieldName)
        throws Exception {
        TestObject obj = createObject();
        TestOuterObject outObj = new TestOuterObject(obj);

        BinaryObjectExImpl portOutObj = toBinary(marsh, outObj);
        BinaryObjectExImpl portObj = portOutObj.field("fInner");

        assert portObj != null;

        BinaryField field = portObj.type().field(fieldName);

        return new TestContext(obj, portObj, field);
    }

    /**
     * Create test object.
     *
     * @return Test object.
     */
    private TestObject createObject() {
        return new TestObject(0);
    }

    /**
     * Convert object to binary object.
     *
     * @param marsh Marshaller.
     * @param obj Object.
     * @return Binary object.
     * @throws Exception If failed.
     */
    protected abstract BinaryObjectExImpl toBinary(BinaryMarshaller marsh, Object obj) throws Exception;

    /**
     * Outer test object.
     */
    public static class TestOuterObject {
        /** Inner object. */
        public TestObject fInner;

        /**
         * Default constructor.
         */
        public TestOuterObject() {
            // No-op.
        }

        /**
         * Constructor.
         *
         * @param fInner Inner object.
         */
        public TestOuterObject(TestObject fInner) {
            this.fInner = fInner;
        }
    }

    /**
     * Test object class, c
     */
    public static class TestObject {
        /** Primitive fields. */
        public byte fByte;

        public boolean fBool;

        public short fShort;

        public char fChar;

        public int fInt;

        public long fLong;

        public float fFloat;

        public double fDouble;

        public byte[] fByteArr;

        public boolean[] fBoolArr;

        public short[] fShortArr;

        public char[] fCharArr;

        public int[] fIntArr;

        public long[] fLongArr;

        public float[] fFloatArr;

        public double[] fDoubleArr;

        /** Special fields. */
        public String fString;

        public Date fDate;

        public Timestamp fTimestamp;

        public UUID fUuid;

        public BigDecimal fDecimal;

        public String[] fStringArr;

        public Date[] fDateArr;

        public Timestamp[] fTimestampArr;

        public UUID[] fUuidArr;

        public BigDecimal[] fDecimalArr;

        /** Nested object. */
        public TestInnerObject fObj;

        public TestInnerObject[] fObjArr;

        /** Field which is always set to null. */
        public Object fNull;

        /**
         * Default constructor.
         */
        public TestObject() {
            // No-op.
        }

        /**
         * Non-default constructor.
         *
         * @param ignore Ignored.
         */
        public TestObject(int ignore) {
            fByte = 1;
            fBool = true;
            fShort = 2;
            fChar = 3;
            fInt = 4;
            fLong = 5;
            fFloat = 6.6f;
            fDouble = 7.7;

            fByteArr = new byte[] { 1, 2 };
            fBoolArr = new boolean[] { true, false };
            fShortArr = new short[] { 2, 3 };
            fCharArr = new char[] { 3, 4 };
            fIntArr = new int[] { 4, 5 };
            fLongArr = new long[] { 5, 6 };
            fFloatArr = new float[] { 6.6f, 7.7f };
            fDoubleArr = new double[] { 7.7, 8.8 };

            fString = "8";
            fDate = new Date();
            fTimestamp = new Timestamp(new Date().getTime() + 1);
            fUuid = UUID.randomUUID();
            fDecimal = new BigDecimal(9);

            fStringArr = new String[] { "8", "9" };
            fDateArr = new Date[] { new Date(), new Date(new Date().getTime() + 1) };
            fTimestampArr =
                new Timestamp[] { new Timestamp(new Date().getTime() + 1), new Timestamp(new Date().getTime() + 2) };
            fUuidArr = new UUID[] { UUID.randomUUID(), UUID.randomUUID() };
            fDecimalArr = new BigDecimal[] { new BigDecimal(9), new BigDecimal(10) };

            fObj = new TestInnerObject(10);
            fObjArr = new TestInnerObject[] { new TestInnerObject(10), new TestInnerObject(11) };
        }
    }

    /**
     * Inner test object.
     */
    public static class TestInnerObject {
        /** Value. */
        private int val;

        /**
         * Default constructor.
         */
        public TestInnerObject() {
            // No-op.
        }

        /**
         * Constructor.
         *
         * @param val Value.
         */
        public TestInnerObject(int val) {
            this.val = val;
        }

        /** {@inheritDoc} */
        @Override public int hashCode() {
            return val;
        }

        /** {@inheritDoc} */
        @Override public boolean equals(Object other) {
            return other != null && other instanceof TestInnerObject && val == ((TestInnerObject)(other)).val;
        }
    }

    /**
     * Test context.
     */
    public static class TestContext {
        /** Object. */
        public final TestObject obj;

        /** Binary object. */
        public final BinaryObjectExImpl portObj;

        /** Field. */
        public final BinaryField field;

        /**
         * Constructor.
         *
         * @param obj Object.
         * @param portObj Binary object.
         * @param field Field.
         */
        public TestContext(TestObject obj, BinaryObjectExImpl portObj, BinaryField field) {
            this.obj = obj;
            this.portObj = portObj;
            this.field = field;
        }
    }
}
