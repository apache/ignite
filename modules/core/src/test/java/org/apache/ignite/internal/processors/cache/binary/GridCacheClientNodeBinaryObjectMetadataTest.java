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

import java.util.Arrays;
import java.util.Collection;
import org.apache.ignite.Ignite;
import org.apache.ignite.binary.BinaryType;
import org.apache.ignite.binary.BinaryTypeConfiguration;
import org.apache.ignite.cache.CacheAtomicityMode;
import org.apache.ignite.cache.CacheKeyConfiguration;
import org.apache.ignite.cache.CacheMode;
import org.apache.ignite.cache.affinity.Affinity;
import org.apache.ignite.configuration.BinaryConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.configuration.NearCacheConfiguration;
import org.apache.ignite.internal.binary.BinaryMarshaller;
import org.apache.ignite.internal.processors.cache.GridCacheAbstractSelfTest;
import org.apache.ignite.spi.discovery.tcp.TcpDiscoverySpi;
import org.junit.Test;

import static org.apache.ignite.cache.CacheAtomicityMode.ATOMIC;

/**
 *
 */
public class GridCacheClientNodeBinaryObjectMetadataTest extends GridCacheAbstractSelfTest {
    /** {@inheritDoc} */
    @Override protected int gridCount() {
        return 4;
    }

    /** {@inheritDoc} */
    @Override protected CacheMode cacheMode() {
        return CacheMode.PARTITIONED;
    }

    /** {@inheritDoc} */
    @Override protected CacheAtomicityMode atomicityMode() {
        return ATOMIC;
    }

    /** {@inheritDoc} */
    @Override protected NearCacheConfiguration nearConfiguration() {
        return null;
    }

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String igniteInstanceName) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(igniteInstanceName);

        BinaryMarshaller marsh = new BinaryMarshaller();

        BinaryConfiguration bCfg = new BinaryConfiguration();

        bCfg.setClassNames(Arrays.asList(TestObject1.class.getName(), TestObject2.class.getName()));

        BinaryTypeConfiguration typeCfg = new BinaryTypeConfiguration();

        typeCfg.setTypeName(TestObject1.class.getName());

        CacheKeyConfiguration keyCfg = new CacheKeyConfiguration(TestObject1.class.getName(), "val2");

        cfg.setCacheKeyConfiguration(keyCfg);

        bCfg.setTypeConfigurations(Arrays.asList(typeCfg));

        cfg.setBinaryConfiguration(bCfg);

        if (igniteInstanceName.equals(getTestIgniteInstanceName(gridCount() - 1)))
            cfg.setClientMode(true);

        cfg.setMarshaller(marsh);

        ((TcpDiscoverySpi)cfg.getDiscoverySpi()).setForceServerMode(true);

        return cfg;
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testBinaryMetadataOnClient() throws Exception {
        Ignite ignite0 = ignite(gridCount() - 1);

        assertTrue(ignite0.configuration().isClientMode());

        Ignite ignite1 = ignite(0);

        assertFalse(ignite1.configuration().isClientMode());

        Affinity<Object> aff0 = ignite0.affinity(DEFAULT_CACHE_NAME);
        Affinity<Object> aff1 = ignite1.affinity(DEFAULT_CACHE_NAME);

        for (int i = 0; i < 100; i++) {
            TestObject1 obj1 = new TestObject1(i, i + 1);

            assertEquals(aff1.mapKeyToPrimaryAndBackups(obj1),
                aff0.mapKeyToPrimaryAndBackups(obj1));

            TestObject2 obj2 = new TestObject2(i, i + 1);

            assertEquals(aff1.mapKeyToPrimaryAndBackups(obj2),
                aff0.mapKeyToPrimaryAndBackups(obj2));
        }

        Collection<BinaryType> meta1 = ignite1.binary().types();
        Collection<BinaryType> meta2 = ignite1.binary().types();

        assertEquals(meta1.size(), meta2.size());

        for (BinaryType m1 : meta1) {
            boolean found = false;

            for (BinaryType m2 : meta1) {
                if (m1.typeName().equals(m2.typeName())) {
                    assertEquals(m1.affinityKeyFieldName(), m2.affinityKeyFieldName());
                    assertEquals(m1.fieldNames(), m2.fieldNames());

                    found = true;

                    break;
                }
            }

            assertTrue(found);
        }
    }

    /**
     *
     */
    static class TestObject1 {
        /** */
        private int val1;

        /** */
        private int val2;

        /**
         * @param val1 Value 1.
         * @param val2 Value 2.
         */
        public TestObject1(int val1, int val2) {
            this.val1 = val1;
            this.val2 = val2;
        }

        /** {@inheritDoc} */
        @Override public boolean equals(Object o) {
            if (this == o)
                return true;

            if (o == null || getClass() != o.getClass())
                return false;

            TestObject1 that = (TestObject1)o;

            return val1 == that.val1;

        }

        /** {@inheritDoc} */
        @Override public int hashCode() {
            return val1;
        }
    }

    /**
     *
     */
    static class TestObject2 {
        /** */
        private int val1;

        /** */
        private int val2;

        /**
         * @param val1 Value 1.
         * @param val2 Value 2.
         */
        public TestObject2(int val1, int val2) {
            this.val1 = val1;
            this.val2 = val2;
        }

        /** {@inheritDoc} */
        @Override public boolean equals(Object o) {
            if (this == o)
                return true;

            if (o == null || getClass() != o.getClass())
                return false;

            TestObject2 that = (TestObject2)o;

            return val2 == that.val2;

        }

        /** {@inheritDoc} */
        @Override public int hashCode() {
            return val2;
        }
    }
}
