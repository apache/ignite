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

import java.util.Collections;
import org.apache.ignite.binary.BinaryField;
import org.apache.ignite.binary.BinaryTypeConfiguration;
import org.apache.ignite.configuration.BinaryConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.util.IgniteUtils;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.logger.NullLogger;
import org.apache.ignite.marshaller.MarshallerContextTestImpl;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;

import static org.apache.ignite.binary.BinaryStringEncoding.ENC_NAME_WINDOWS_1251;

/**
 * Contains tests for binary object encoded string field.
 */
public abstract class BinaryEncodedStringFieldAbstractSelfTest extends GridCommonAbstractTest {
    /** Marshaller. */
    protected BinaryMarshaller dfltMarsh;

    /**
     * Create marshaller.
     *
     * @return Binary marshaller.
     * @throws Exception If failed.
     */
    protected BinaryMarshaller createMarshaller() throws Exception {
        BinaryConfiguration bCfg = new BinaryConfiguration();

        bCfg.setCompactFooter(true);

        bCfg.setTypeConfigurations(Collections.singletonList(new BinaryTypeConfiguration(TestObject.class.getName())));

        IgniteConfiguration iCfg = new IgniteConfiguration();

        iCfg.setEncoding(ENC_NAME_WINDOWS_1251);

        iCfg.setBinaryConfiguration(bCfg);

        BinaryContext ctx = new BinaryContext(BinaryCachingMetadataHandler.create(), iCfg, new NullLogger());

        BinaryMarshaller marsh = new BinaryMarshaller();

        marsh.setContext(new MarshallerContextTestImpl(null));

        IgniteUtils.invoke(BinaryMarshaller.class, marsh, "setBinaryContext", ctx, iCfg);

        return marsh;
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
     * Test string field.
     *
     * @throws Exception If failed.
     */
    public void testString() throws Exception {
        check("fString");
    }

    /**
     * Test string array field.
     *
     * @throws Exception If failed.
     */
    public void testStringArray() throws Exception {
        check("fStringArr");
    }

    /**
     * Check field.
     *
     * @param fieldName Field name.
     * @throws Exception If failed.
     */
    public void check(String fieldName) throws Exception {
        TestContext testCtx = context(dfltMarsh, fieldName);

        check0(fieldName, testCtx);
    }

    /**
     * Internal check routine.
     *
     * @param fieldName Field name.
     * @param ctx Context.
     * @throws Exception If failed.
     */
    private void check0(String fieldName, TestContext ctx) throws Exception {
        Object val = ctx.field.value(ctx.portObj);

        assertTrue(ctx.field.exists(ctx.portObj));

        Object expVal = U.field(ctx.obj, fieldName);

        if (val != null && val.getClass().isArray()) {
            assertNotNull(expVal);

            Object[] expVal0 = (Object[])expVal;
            Object[] val0 = (Object[])val;

            assertEquals(expVal0.length, val0.length);

            for (int i = 0; i < expVal0.length; i++)
                assertEquals(expVal0[i], val0[i]);
        }
        else
            assertEquals(expVal, val);
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
     * Test object class, c
     */
    @SuppressWarnings("UnusedDeclaration")
    public static class TestObject {
        public String fString;

        public String[] fStringArr;

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
            fString = "Ы";
            fStringArr = new String[] {"Ю", "Я"};
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
