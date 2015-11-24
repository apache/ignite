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

package org.apache.ignite.internal.portable;

import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.binary.BinaryObject;
import org.apache.ignite.binary.BinaryTypeConfiguration;
import org.apache.ignite.cache.CacheMode;
import org.apache.ignite.configuration.BinaryConfiguration;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.marshaller.portable.BinaryMarshaller;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;

import java.util.Arrays;

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
    private IgniteCache<Integer, BinaryObject> cacheBinary1;

    /** Binary cache 2. */
    private IgniteCache<Integer, BinaryObject> cacheBinary2;

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
     * @throws Exception
     */
    public void testSimpleRegistered() throws Exception{
        startUp(true);

        cache1.put(0, new EnumHolder(EnumType.ONE));

        Object res = cache1.get(0);

        cache1.put(1, EnumType.ONE);

        assertEquals(EnumType.ONE, cache1.get(1));
        assertEquals(EnumType.ONE, cache2.get(1));

        BinaryObject obj = cacheBinary1.get(1);

        assertTrue(obj.type().isEnum());
        assertEquals(node1.binary().typeId(EnumType.class.getName()), obj.typeId());
        assertEquals(EnumType.ONE.ordinal(), obj.enumOrdinal());
    }

    /**
     * Enumeration holder.
     */
    public static class EnumHolder {
        /** Value. */
        public EnumType val;

        /**
         * Default constructor.
         */
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
    public static enum EnumType {
        ONE,
        TWO
    }
}
