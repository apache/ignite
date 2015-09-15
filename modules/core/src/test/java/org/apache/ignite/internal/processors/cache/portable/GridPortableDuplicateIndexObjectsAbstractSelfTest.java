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

import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.cache.CacheAtomicityMode;
import org.apache.ignite.cache.CacheMode;
import org.apache.ignite.cache.CacheTypeMetadata;
import org.apache.ignite.cache.query.SqlFieldsQuery;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.processors.cache.GridCacheAbstractSelfTest;
import org.apache.ignite.internal.util.typedef.F;
import org.apache.ignite.marshaller.portable.PortableMarshaller;
import org.apache.ignite.portable.PortableObject;

/**
 * Tests that portable object is the same in cache entry and in index.
 */
public abstract class GridPortableDuplicateIndexObjectsAbstractSelfTest extends GridCacheAbstractSelfTest {
    /** {@inheritDoc} */
    @Override protected int gridCount() {
        return 1;
    }

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String gridName) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(gridName);

        PortableMarshaller marsh = new PortableMarshaller();

        marsh.setClassNames(Collections.singletonList(TestPortable.class.getName()));

        cfg.setMarshaller(marsh);

        return cfg;
    }

    /** {@inheritDoc} */
    @Override protected CacheConfiguration cacheConfiguration(String gridName) throws Exception {
        CacheConfiguration ccfg = super.cacheConfiguration(gridName);

        ccfg.setCopyOnRead(false);

        CacheTypeMetadata meta = new CacheTypeMetadata();

        meta.setKeyType(Integer.class);
        meta.setValueType(TestPortable.class.getName());

        Map<String, Class<?>> idx = new HashMap<>();

        idx.put("fieldOne", String.class);
        idx.put("fieldTwo", Integer.class);

        meta.setAscendingFields(idx);

        ccfg.setTypeMetadata(Collections.singletonList(meta));

        return ccfg;
    }

    /** {@inheritDoc} */
    @Override public abstract CacheAtomicityMode atomicityMode();

    /** {@inheritDoc} */
    @Override public abstract CacheMode cacheMode();

    /**
     * @throws Exception If failed.
     */
    public void testIndexReferences() throws Exception {
        IgniteCache<Integer, TestPortable> cache = grid(0).cache(null);

        String fieldOneVal = "123";
        int fieldTwoVal = 123;
        int key = 0;

        cache.put(key, new TestPortable(fieldOneVal, fieldTwoVal));

        IgniteCache<Integer, PortableObject> prj = grid(0).cache(null).withKeepPortable();

        PortableObject cacheVal = prj.get(key);

        assertEquals(fieldOneVal, cacheVal.field("fieldOne"));
        assertEquals(new Integer(fieldTwoVal), cacheVal.field("fieldTwo"));

        List<?> row = F.first(prj.query(new SqlFieldsQuery("select _val from " +
            "TestPortable where _key = ?").setArgs(key)).getAll());

        assertEquals(1, row.size());

        PortableObject qryVal = (PortableObject)row.get(0);

        assertEquals(fieldOneVal, qryVal.field("fieldOne"));
        assertEquals(new Integer(fieldTwoVal), qryVal.field("fieldTwo"));
        assertSame(cacheVal, qryVal);
    }

    /**
     * Test portable object.
     */
    private static class TestPortable {
        /** */
        private String fieldOne;

        /** */
        private int fieldTwo;

        /**
         *
         */
        private TestPortable() {
            // No-op.
        }

        /**
         * @param fieldOne Field one.
         * @param fieldTwo Field two.
         */
        private TestPortable(String fieldOne, int fieldTwo) {
            this.fieldOne = fieldOne;
            this.fieldTwo = fieldTwo;
        }

        /**
         * @return Field one.
         */
        public String fieldOne() {
            return fieldOne;
        }

        /**
         * @return Field two.
         */
        public int fieldTwo() {
            return fieldTwo;
        }
    }
}