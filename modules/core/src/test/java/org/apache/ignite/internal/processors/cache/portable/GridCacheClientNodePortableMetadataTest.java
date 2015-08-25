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

package org.apache.ignite.internal.processors.cache.portable;

import org.apache.ignite.*;
import org.apache.ignite.cache.*;
import org.apache.ignite.cache.affinity.*;
import org.apache.ignite.configuration.*;
import org.apache.ignite.internal.processors.cache.*;
import org.apache.ignite.marshaller.portable.*;
import org.apache.ignite.portable.*;
import org.apache.ignite.spi.discovery.tcp.*;

import java.util.*;

import static org.apache.ignite.cache.CacheAtomicityMode.*;

/**
 *
 */
public class GridCacheClientNodePortableMetadataTest extends GridCacheAbstractSelfTest {
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
    @Override protected IgniteConfiguration getConfiguration(String gridName) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(gridName);

        PortableMarshaller marsh = new PortableMarshaller();

        marsh.setClassNames(Arrays.asList(TestObject1.class.getName(), TestObject2.class.getName()));

        PortableTypeConfiguration typeCfg = new PortableTypeConfiguration();

        typeCfg.setClassName(TestObject1.class.getName());
        typeCfg.setAffinityKeyFieldName("val2");

        marsh.setTypeConfigurations(Arrays.asList(typeCfg));

        if (gridName.equals(getTestGridName(gridCount() - 1)))
            cfg.setClientMode(true);

        cfg.setMarshaller(marsh);

        ((TcpDiscoverySpi)cfg.getDiscoverySpi()).setForceServerMode(true);

        return cfg;
    }

    /**
     * @throws Exception If failed.
     */
    public void testPortableMetadataOnClient() throws Exception {
        Ignite ignite0 = ignite(gridCount() - 1);

        assertTrue(ignite0.configuration().isClientMode());

        Ignite ignite1 = ignite(0);

        assertFalse(ignite1.configuration().isClientMode());

        Affinity<Object> aff0 = ignite0.affinity(null);
        Affinity<Object> aff1 = ignite1.affinity(null);

        for (int i = 0 ; i < 100; i++) {
            TestObject1 obj1 = new TestObject1(i, i + 1);

            assertEquals(aff1.mapKeyToPrimaryAndBackups(obj1),
                aff0.mapKeyToPrimaryAndBackups(obj1));

            TestObject2 obj2 = new TestObject2(i, i + 1);

            assertEquals(aff1.mapKeyToPrimaryAndBackups(obj2),
                aff0.mapKeyToPrimaryAndBackups(obj2));
        }

        {
            PortableBuilder builder = ignite0.portables().builder("TestObject3");

            builder.setField("f1", 1);

            ignite0.cache(null).put(0, builder.build());

            IgniteCache<Integer, PortableObject> cache = ignite0.cache(null).withKeepPortable();

            PortableObject obj = cache.get(0);

            PortableMetadata meta = obj.metaData();

            assertNotNull(meta);
            assertEquals(1, meta.fields().size());

            meta = ignite0.portables().metadata(TestObject1.class);

            assertNotNull(meta);
            assertEquals("val2", meta.affinityKeyFieldName());

            meta = ignite0.portables().metadata(TestObject2.class);

            assertNotNull(meta);
            assertNull(meta.affinityKeyFieldName());
        }

        {
            PortableBuilder builder = ignite1.portables().builder("TestObject3");

            builder.setField("f2", 2);

            ignite1.cache(null).put(1, builder.build());

            IgniteCache<Integer, PortableObject> cache = ignite1.cache(null).withKeepPortable();

            PortableObject obj = cache.get(0);

            PortableMetadata meta = obj.metaData();

            assertNotNull(meta);
            assertEquals(2, meta.fields().size());

            meta = ignite1.portables().metadata(TestObject1.class);

            assertNotNull(meta);
            assertEquals("val2", meta.affinityKeyFieldName());

            meta = ignite1.portables().metadata(TestObject2.class);

            assertNotNull(meta);
            assertNull(meta.affinityKeyFieldName());
        }

        PortableMetadata meta = ignite0.portables().metadata("TestObject3");

        assertNotNull(meta);
        assertEquals(2, meta.fields().size());

        IgniteCache<Integer, PortableObject> cache = ignite0.cache(null).withKeepPortable();

        PortableObject obj = cache.get(1);

        assertEquals(Integer.valueOf(2), obj.field("f2"));
        assertNull(obj.field("f1"));

        meta = obj.metaData();

        assertNotNull(meta);
        assertEquals(2, meta.fields().size());

        Collection<PortableMetadata> meta1 = ignite1.portables().metadata();
        Collection<PortableMetadata> meta2 = ignite1.portables().metadata();

        assertEquals(meta1.size(), meta2.size());

        for (PortableMetadata m1 : meta1) {
            boolean found = false;

            for (PortableMetadata m2 : meta1) {
                if (m1.typeName().equals(m2.typeName())) {
                    assertEquals(m1.affinityKeyFieldName(), m2.affinityKeyFieldName());
                    assertEquals(m1.fields(), m2.fields());

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
