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

import java.io.Serializable;
import java.util.Arrays;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.binary.BinaryObject;
import org.apache.ignite.binary.BinaryTypeConfiguration;
import org.apache.ignite.cache.CacheMode;
import org.apache.ignite.configuration.BinaryConfiguration;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.internal.IgniteKernal;
import org.apache.ignite.internal.processors.cache.binary.CacheObjectBinaryProcessorImpl;
import org.apache.ignite.marshaller.Marshaller;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;

/**
 * Contains tests for binary enums.
 */
@SuppressWarnings("unchecked")
public class BinaryEnumsSelfTest extends GridCommonAbstractTest {
    /** Cache name. */
    private static String CACHE_NAME = "cache";

    /** Whether to register types or not. */
    private boolean register;

    /** Node 1. */
    private Ignite node1;

    /** Node 2. */
    private Ignite node2;

    /** Cache 1. */
    private IgniteCache cache1;

    /** Cache 2. */
    private IgniteCache cache2;

    /** Binary cache 1. */
    private IgniteCache cacheBinary1;

    /** Binary cache 2. */
    private IgniteCache cacheBinary2;

    /** {@inheritDoc} */
    @Override protected void beforeTest() throws Exception {
        register = false;
    }

    /** {@inheritDoc} */
    @Override protected void afterTest() throws Exception {
        stopAllGrids();
    }

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String gridName) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(gridName);

        if (register) {
            BinaryConfiguration bCfg = new BinaryConfiguration();

            BinaryTypeConfiguration enumCfg = new BinaryTypeConfiguration(EnumType.class.getName());
            enumCfg.setEnum(true);

            bCfg.setTypeConfigurations(Arrays.asList(enumCfg, new BinaryTypeConfiguration(EnumHolder.class.getName())));

            cfg.setBinaryConfiguration(bCfg);
        }

        cfg.setMarshaller(new BinaryMarshaller());

        CacheConfiguration ccfg = new CacheConfiguration();
        ccfg.setName(CACHE_NAME);
        ccfg.setCacheMode(CacheMode.PARTITIONED);

        cfg.setCacheConfiguration(ccfg);

        return cfg;
    }

    /**
     * Start up routine.
     *
     * @throws Exception If failed.
     */
    private void startUp(boolean register) throws Exception {
        this.register = register;

        node1 = startGrid(0);
        cache1 = node1.cache(CACHE_NAME);
        cacheBinary1 = cache1.withKeepBinary();

        node2 = startGrid(1);
        cache2 = node2.cache(CACHE_NAME);
        cacheBinary2 = cache2.withKeepBinary();
    }

    /**
     * Test operations on simple type which is registered in advance.
     *
     * @throws Exception If failed.
     */
    public void testSimpleRegistered() throws Exception {
        checkSimple(true);
    }

    /**
     * Test operations on simple type which is not registered in advance.
     *
     * @throws Exception If failed.
     */
    public void testSimpleNotRegistered() throws Exception {
        checkSimple(false);
    }

    /**
     * Test operations when enum is nested into an object (registered).
     *
     * @throws Exception If failed.
     */
    public void testNestedRegistered() throws Exception {
        checkNested(true);
    }

    /**
     * Test operations when enum is nested into an object (not registered).
     *
     * @throws Exception If failed.
     */
    public void testNestedNotRegistered() throws Exception {
        checkNested(false);
    }

    /**
     * Test builder operations on simple type which is registered in advance.
     *
     * @throws Exception If failed.
     */
    public void testSimpleBuilderRegistered() throws Exception {
        checkSimpleBuilder(true);
    }

    /**
     * Test builder operations on simple type which is not registered in advance.
     *
     * @throws Exception If failed.
     */
    public void testSimpleBuilderNotRegistered() throws Exception {
        checkSimpleBuilder(false);
    }

    /**
     * Test builder operations when enum is nested into an object (registered).
     *
     * @throws Exception If failed.
     */
    public void testNestedBuilderRegistered() throws Exception {
        checkNestedBuilder(true);
    }

    /**
     * Test builder operations when enum is nested into an object (not registered).
     *
     * @throws Exception If failed.
     */
    public void testNestedBuilderNotRegistered() throws Exception {
        checkNestedBuilder(false);
    }

    /**
     * @throws Exception If failed.
     */
    public void testInstanceFromBytes() throws Exception {
        startUp(true);

        BinaryContext binCtx =
            ((CacheObjectBinaryProcessorImpl)((IgniteKernal)node1).context().cacheObjects()).binaryContext();

        int ord = EnumType.ONE.ordinal();

        String clsName = EnumType.class.getName();

        checkInstanceFromBytes(binCtx, ord, GridBinaryMarshaller.UNREGISTERED_TYPE_ID, clsName);

        checkInstanceFromBytes(binCtx, ord, 42, null);
    }

    /**
     * @param binCtx Binary context.
     * @param ord Enum ordinal.
     * @param typeId Type Id.
     * @param clsName Class name.
     */
    private void checkInstanceFromBytes(BinaryContext binCtx, int ord, int typeId, String clsName)
        throws IgniteCheckedException {

        BinaryEnumObjectImpl srcBinEnum =new BinaryEnumObjectImpl(binCtx, typeId, clsName, ord);

        Marshaller marsh = node1.configuration().getMarshaller();

        byte[] bytes = marsh.marshal(srcBinEnum);

        BinaryEnumObjectImpl binEnum = new BinaryEnumObjectImpl(binCtx, bytes);

        assertEquals(clsName, binEnum.className());
        assertEquals(typeId, binEnum.typeId());
        assertEquals(ord, binEnum.enumOrdinal());
    }

    /**
     * Check simple serialization - deserialization.
     *
     * @param registered If type should be registered in advance.
     * @throws Exception If failed.
     */
    public void checkSimple(boolean registered) throws Exception {
        startUp(registered);

        cache1.put(1, EnumType.ONE);

        validateSimple(1, EnumType.ONE, registered);
    }

    /**
     * Check nested serialization - deserialization.
     *
     * @param registered If type should be registered in advance.
     * @throws Exception If failed.
     */
    private void checkNested(boolean registered) throws Exception {
        startUp(registered);

        cache1.put(1, new EnumHolder(EnumType.ONE));

        validateNested(1, EnumType.ONE, registered);
    }

    /**
     * Check nested builder serialization - deserialization.
     *
     * @param registered If type should be registered in advance.
     * @throws Exception If failed.
     */
    private void checkNestedBuilder(boolean registered) throws Exception {
        startUp(registered);

        BinaryObject obj = node1.binary().builder(EnumHolder.class.getName()).setField("val", EnumType.ONE).build();

        assert node1.binary().type(EnumHolder.class.getName()) != null;
        assert node1.binary().type(EnumType.class.getName()) != null;

        cacheBinary1.put(1, obj);

        validateNested(1, EnumType.ONE, registered);

        obj = (BinaryObject)cacheBinary1.get(1);
        obj = node1.binary().builder(obj).setField("val", EnumType.TWO).build();

        cacheBinary1.put(1, obj);

        validateNested(1, EnumType.TWO, registered);
    }

    /**
     * Validate nested object.
     *
     * @param key Key.
     * @param val Value.
     * @param registered Registered flag.
     * @throws Exception If failed.
     */
    private void validateNested(int key, EnumType val, boolean registered) throws Exception {
        if (registered) {
            EnumHolder res1 = (EnumHolder) cache1.get(key);
            EnumHolder res2 = (EnumHolder) cache2.get(key);

            assertEquals(val, res1.val);
            assertEquals(val, res2.val);
        }

        BinaryObject resBinary1 = (BinaryObject)cacheBinary1.get(key);
        BinaryObject resBinary2 = (BinaryObject)cacheBinary2.get(key);

        validate((BinaryObject)resBinary1.field("val"), val);
        validate((BinaryObject)resBinary2.field("val"), val);
    }

    /**
     * Check simple serialization - deserialization using builder.
     *
     * @param registered If type should be registered in advance.
     * @throws Exception If failed.
     */
    public void checkSimpleBuilder(boolean registered) throws Exception {
        startUp(registered);

        BinaryObject binary = node1.binary().buildEnum(EnumType.class.getName(), EnumType.ONE.ordinal());

        cacheBinary1.put(1, binary);

        validateSimple(1, EnumType.ONE, registered);
    }

    /**
     * Test enum array (registered).
     *
     * @throws Exception If failed.
     */
    public void testSimpleArrayRegistered() throws Exception {
        checkSimpleArray(true);
    }

    /**
     * Test enum array (not registered).
     *
     * @throws Exception If failed.
     */
    public void testSimpleArrayNotRegistered() throws Exception {
        checkSimpleArray(false);
    }

    /**
     * Test enum array created using builder (registered).
     *
     * @throws Exception If failed.
     */
    public void testSimpleBuilderArrayRegistered() throws Exception {
        checkSimpleBuilderArray(true);
    }

    /**
     * Test enum array created using builder (not registered).
     *
     * @throws Exception If failed.
     */
    public void testSimpleBuilderArrayNotRegistered() throws Exception {
        checkSimpleBuilderArray(false);
    }

    /**
     * Check arrays with builder.
     *
     * @param registered Registered flag.
     * @throws Exception If failed.
     */
    public void checkSimpleArray(boolean registered) throws Exception {
        startUp(registered);

        cache1.put(1, new EnumType[] { EnumType.ONE, EnumType.TWO });

        validateSimpleArray(registered);
    }

    /**
     * Check arrays with builder.
     *
     * @param registered Registered flag.
     * @throws Exception If failed.
     */
    public void checkSimpleBuilderArray(boolean registered) throws Exception {
        startUp(registered);

        BinaryObject binaryOne = node1.binary().buildEnum(EnumType.class.getName(), EnumType.ONE.ordinal());
        BinaryObject binaryTwo = node1.binary().buildEnum(EnumType.class.getName(), EnumType.TWO.ordinal());

        cacheBinary1.put(1, new BinaryObject[] { binaryOne, binaryTwo });

        validateSimpleArray(registered);
    }

    /**
     * Check ability to resolve typeId from class name.
     *
     * @throws Exception If failed.
     */
    public void testZeroTypeId() throws Exception {
        startUp(true);

        final BinaryContext ctx =
            ((CacheObjectBinaryProcessorImpl)((IgniteEx)node1).context().cacheObjects()).binaryContext();

        final BinaryObject enumObj =
            new BinaryEnumObjectImpl(ctx, 0, EnumType.class.getName(), EnumType.ONE.ordinal());

        assert enumObj.type().isEnum();
    }

    /**
     * Validate simple array.
     *
     * @param registered Registered flag.
     */
    private void validateSimpleArray(boolean registered) {
        if (registered) {
            Object[] arr1 = (Object[])cache1.get(1);
            Object[] arr2 = (Object[])cache2.get(1);

            assertEquals(2, arr1.length);
            assertEquals(2, arr2.length);

            assertEquals(EnumType.ONE, arr1[0]);
            assertEquals(EnumType.TWO, arr1[1]);

            assertEquals(EnumType.ONE, arr2[0]);
            assertEquals(EnumType.TWO, arr2[1]);
        }

        Object[] arrBinary1 = (Object[])cacheBinary1.get(1);
        Object[] arrBinary2 = (Object[])cacheBinary2.get(1);

        assertEquals(2, arrBinary1.length);
        assertEquals(2, arrBinary2.length);

        validate((BinaryObject) arrBinary1[0], EnumType.ONE);
        validate((BinaryObject) arrBinary1[1], EnumType.TWO);

        validate((BinaryObject) arrBinary2[0], EnumType.ONE);
        validate((BinaryObject) arrBinary2[1], EnumType.TWO);
    }

    /**
     * Internal check routine for simple scenario.
     *
     * @param key Key.
     * @param val Value.
     * @param registered Registered flag.
     * @throws Exception If failed.
     */
    private void validateSimple(int key, EnumType val, boolean registered) throws Exception {
        if (registered) {
            assertEquals(val, cache1.get(key));
            assertEquals(val, cache2.get(key));
        }

        validate((BinaryObject) cacheBinary1.get(key), val);
        validate((BinaryObject) cacheBinary2.get(key), val);
    }

    /**
     * Validate single value.
     *
     * @param obj Binary value.
     * @param val Expected value.
     */
    private void validate(BinaryObject obj, EnumType val) {
        assertTrue(obj.type().isEnum());

        assertEquals(node1.binary().typeId(EnumType.class.getName()), obj.type().typeId());
        assertEquals(node2.binary().typeId(EnumType.class.getName()), obj.type().typeId());

        assertEquals(val.ordinal(), obj.enumOrdinal());
    }

    /**
     * Enumeration holder.
     */
    public static class EnumHolder implements Serializable {
        /** Value. */
        public EnumType val;

        /**
         * Default constructor.
         */
        @SuppressWarnings("UnusedDeclaration")
        public EnumHolder() {
            // No-op.
        }

        /**
         * Constructor.
         *
         * @param val Value.
         */
        public EnumHolder(EnumType val) {
            this.val = val;
        }
    }

    /**
     * Enumeration for tests.
     */
    public enum EnumType {
        ONE,
        TWO
    }
}
