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

import java.util.Arrays;
import org.apache.ignite.binary.BinaryField;
import org.apache.ignite.binary.BinaryTypeConfiguration;
import org.apache.ignite.configuration.BinaryConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.util.IgniteUtils;
import org.apache.ignite.logger.NullLogger;
import org.apache.ignite.marshaller.MarshallerContextTestImpl;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.junit.Test;

/**
 * Contains tests for compact offsets.
 */
public abstract class BinaryFooterOffsetsAbstractSelfTest extends GridCommonAbstractTest {
    /** 2 pow 8. */
    private static int POW_8 = 1 << 8;

    /** 2 pow 16. */
    private static int POW_16 = 1 << 16;

    /** Marshaller. */
    protected BinaryMarshaller marsh;

    /** Binary context. */
    protected BinaryContext ctx;

    /** {@inheritDoc} */
    @Override protected void beforeTest() throws Exception {
        super.beforeTest();

        ctx = new BinaryContext(BinaryCachingMetadataHandler.create(), new IgniteConfiguration(), new NullLogger());

        marsh = new BinaryMarshaller();

        IgniteConfiguration iCfg = new IgniteConfiguration();

        BinaryConfiguration bCfg = new BinaryConfiguration();

        bCfg.setTypeConfigurations(Arrays.asList(new BinaryTypeConfiguration(TestObject.class.getName())));

        bCfg.setCompactFooter(compactFooter());

        iCfg.setBinaryConfiguration(bCfg);

        marsh.setContext(new MarshallerContextTestImpl(null));

        IgniteUtils.invoke(BinaryMarshaller.class, marsh, "setBinaryContext", ctx, iCfg);
    }

    /**
     * @return Whether to use compact footers.
     */
    protected boolean compactFooter() {
        return true;
    }

    /**
     * Test 1 byte.
     *
     * @throws Exception If failed.
     */
    @Test
    public void test1Byte() throws Exception {
        check(POW_8 >> 2);
    }

    /**
     * Test 1 byte with sign altering.
     *
     * @throws Exception If failed.
     */
    @Test
    public void test1ByteSign() throws Exception {
        check(POW_8 >> 1);
    }

    /**
     * Test 2 bytes.
     *
     * @throws Exception If failed.
     */
    @Test
    public void test2Bytes() throws Exception {
        check(POW_16 >> 2);
    }

    /**
     * Test 2 bytes with sign altering.
     *
     * @throws Exception If failed.
     */
    @Test
    public void test2BytesSign() throws Exception {
        check(POW_16 >> 1);
    }

    /**
     * Test 4 bytes.
     *
     * @throws Exception If failed.
     */
    @Test
    public void test4Bytes() throws Exception {
        check(POW_16 << 2);
    }

    /**
     * Main check routine.
     *
     * @param len Length of the first field.
     *
     * @throws Exception If failed.
     */
    private void check(int len) throws Exception {
        TestObject obj = new TestObject(len);

        BinaryObjectExImpl portObj = toBinary(marsh, obj);

        // 1. Test binary object content.
        assert portObj.hasField("field1");
        assert portObj.hasField("field2");

        byte[] field1 = portObj.field("field1");
        Integer field2 = portObj.field("field2");

        assert field1 != null;
        assert field2 != null;

        assert Arrays.equals(obj.field1, field1);
        assert obj.field2 == field2;

        // 2. Test fields API.
        BinaryField field1Desc = portObj.type().field("field1");
        BinaryField field2Desc = portObj.type().field("field2");

        assert field1Desc.exists(portObj);
        assert field2Desc.exists(portObj);

        assert Arrays.equals(obj.field1, (byte[])field1Desc.value(portObj));
        assert obj.field2 == (Integer)field2Desc.value(portObj);

        // 3. Test deserialize.
        TestObject objRestored = portObj.deserialize();

        assert objRestored != null;

        assert Arrays.equals(obj.field1, objRestored.field1);
        assert obj.field2 == objRestored.field2;
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
     * Test object.
     */
    public static class TestObject {
        /** First field with variable length. */
        public byte[] field1;

        /** Second field. */
        public int field2;

        /**
         * Default constructor.
         */
        public TestObject() {
            // No-op.
        }

        /**
         * Constructor.
         *
         * @param len Array length.
         */
        public TestObject(int len) {
            field1 = new byte[len];

            field1[0] = 1;
            field1[len - 1] = 2;

            field2 = len;
        }
    }
}
