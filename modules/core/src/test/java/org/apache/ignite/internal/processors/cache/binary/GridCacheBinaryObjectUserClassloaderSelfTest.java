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

package org.apache.ignite.internal.processors.cache.binary;

import java.io.Serializable;
import java.util.HashSet;
import java.util.Set;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.binary.BinaryObjectException;
import org.apache.ignite.binary.BinaryReader;
import org.apache.ignite.binary.BinarySerializer;
import org.apache.ignite.binary.BinaryTypeConfiguration;
import org.apache.ignite.binary.BinaryWriter;
import org.apache.ignite.configuration.BinaryConfiguration;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.binary.BinaryMarshaller;
import org.apache.ignite.internal.util.typedef.internal.S;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.junit.Test;

import static org.apache.ignite.cache.CacheMode.REPLICATED;
import static org.apache.ignite.cache.CacheWriteSynchronizationMode.FULL_SYNC;

/**
 *
 */
public class GridCacheBinaryObjectUserClassloaderSelfTest extends GridCommonAbstractTest {
    /** */
    private static volatile boolean customBinaryConf = false;

    /** */
    private static volatile boolean deserialized = false;

    /** */
    private static volatile boolean useWrappingLoader = false;

    /** {@inheritDoc} */
    @Override protected void afterTest() throws Exception {
        super.afterTest();

        stopAllGrids();
    }

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String igniteInstanceName) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(igniteInstanceName);

        cfg.setCacheConfiguration(cacheConfiguration(igniteInstanceName));

        cfg.setMarshaller(new BinaryMarshaller());

        cfg.setClassLoader(useWrappingLoader ? new WrappingClassLoader(getExternalClassLoader()) :
            getExternalClassLoader());

        if (customBinaryConf) {
            BinarySerializer bs = new BinarySerializer() {
                /** {@inheritDoc} */
                @Override public void writeBinary(Object obj, BinaryWriter writer) throws BinaryObjectException {
                    //No-op.
                }

                /** {@inheritDoc} */
                @Override public void readBinary(Object obj, BinaryReader reader) throws BinaryObjectException {
                    deserialized = true;
                }
            };

            BinaryTypeConfiguration btcfg1 = new BinaryTypeConfiguration();

            btcfg1.setTypeName("org.apache.ignite.tests.p2p.CacheDeploymentTestValue");

            btcfg1.setSerializer(bs);

            BinaryTypeConfiguration btcfg2 = new BinaryTypeConfiguration();

            btcfg2.setTypeName("org.apache.ignite.internal.processors.cache.binary." +
                "GridCacheBinaryObjectUserClassloaderSelfTest$TestValue1");

            btcfg2.setSerializer(bs);

            BinaryConfiguration bcfg = new BinaryConfiguration();

            Set<BinaryTypeConfiguration> set = new HashSet<>();

            set.add(btcfg1);
            set.add(btcfg2);

            bcfg.setTypeConfigurations(set);

            cfg.setBinaryConfiguration(bcfg);
        }

        return cfg;
    }

    /**
     * Gets cache configuration for grid with specified name.
     *
     * @param igniteInstanceName Ignite instance name.
     * @return Cache configuration.
     */
    CacheConfiguration cacheConfiguration(String igniteInstanceName) {
        CacheConfiguration cacheCfg = defaultCacheConfiguration();

        cacheCfg.setCacheMode(REPLICATED);
        cacheCfg.setWriteSynchronizationMode(FULL_SYNC);

        return cacheCfg;
    }


    /**
     * @throws Exception If test failed.
     */
    @Test
    public void testConfigurationRegistration() throws Exception {
        useWrappingLoader = false;

        doTestConfigurationRegistration();
    }

    /**
     * @throws Exception If test failed.
     */
    @Test
    public void testConfigurationRegistrationWithWrappingLoader() throws Exception {
        useWrappingLoader = true;

        doTestConfigurationRegistration();
    }

    /**
     * @throws Exception If test failed.
     */
    private void doTestConfigurationRegistration() throws Exception {
        try {
            customBinaryConf = true;

            Ignite i1 = startGrid(1);
            Ignite i2 = startGrid(2);

            IgniteCache<Integer, Object> cache1 = i1.cache(DEFAULT_CACHE_NAME);
            IgniteCache<Integer, Object> cache2 = i2.cache(DEFAULT_CACHE_NAME);

            ClassLoader ldr = useWrappingLoader ?
                ((WrappingClassLoader)i1.configuration().getClassLoader()).getParent() :
                i1.configuration().getClassLoader();

            Object v1 = ldr.loadClass("org.apache.ignite.tests.p2p.CacheDeploymentTestValue").newInstance();
            Object v2 = ldr.loadClass("org.apache.ignite.tests.p2p.CacheDeploymentTestValue2").newInstance();

            cache1.put(1, v1);
            cache1.put(2, v2);
            cache1.put(3, new TestValue1(123));
            cache1.put(4, new TestValue2(123));

            deserialized = false;

            cache2.get(1);

            assertTrue(deserialized);

            deserialized = false;

            cache2.get(2);

            assertFalse(deserialized);

            deserialized = false;

            cache2.get(3);

            assertTrue(deserialized);

            deserialized = false;

            cache2.get(4);

            assertFalse(deserialized);
        }
        finally {
            customBinaryConf = false;
        }
    }

    /**
     *
     */
    private static class TestValue1 implements Serializable {
        /** */
        private int val;

        /**
         * @param val Value.
         */
        public TestValue1(int val) {
            this.val = val;
        }

        /**
         * @return Value.
         */
        public int value() {
            return val;
        }

        /** {@inheritDoc} */
        @Override public String toString() {
            return S.toString(TestValue1.class, this);
        }
    }

    /**
     *
     */
    private static class TestValue2 implements Serializable {
        /** */
        private int val;

        /**
         * @param val Value.
         */
        public TestValue2(int val) {
            this.val = val;
        }

        /**
         * @return Value.
         */
        public int value() {
            return val;
        }

        /** {@inheritDoc} */
        @Override public String toString() {
            return S.toString(TestValue2.class, this);
        }
    }

    /**
     *
     */
    private static class WrappingClassLoader extends ClassLoader {
        public WrappingClassLoader(ClassLoader parent) {
            super(parent);
        }
    }
}
