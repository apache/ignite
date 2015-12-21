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

package org.apache.ignite.internal.processors.cache;

import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.Ignition;
import org.apache.ignite.binary.BinaryUtils;
import org.apache.ignite.cache.CacheAtomicityMode;
import org.apache.ignite.cache.CacheMode;
import org.apache.ignite.cache.CacheRebalanceMode;
import org.apache.ignite.cache.CacheTypeMetadata;
import org.apache.ignite.cache.CacheWriteSynchronizationMode;
import org.apache.ignite.cache.query.SqlFieldsQuery;
import org.apache.ignite.cache.query.SqlQuery;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.binary.BinaryMarshaller;
import org.apache.ignite.internal.util.typedef.G;
import org.apache.ignite.spi.discovery.tcp.TcpDiscoverySpi;
import org.apache.ignite.spi.discovery.tcp.ipfinder.vm.TcpDiscoveryVmIpFinder;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;

import javax.cache.Cache;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

/**
 * Tests for class with duplicate field names.
 */
public class BinaryDuplicateFieldsQuerySelfTest extends GridCommonAbstractTest {
    /** Field 1. */
    private static final String FIELD_1 = BinaryUtils.qualifiedFieldName(Entity1.class, "x");

    /** Field 2. */
    private static final String FIELD_2 = BinaryUtils.qualifiedFieldName(Entity2.class, "x");

    /** Ignite instance. */
    private Ignite ignite;

    /** Cache. */
    private IgniteCache<Integer, Entity2> cache;

    /** {@inheritDoc} */
    @SuppressWarnings("deprecation")
    @Override protected void beforeTest() throws Exception {
        IgniteConfiguration cfg = new IgniteConfiguration();

        cfg.setLocalHost("127.0.0.1");

        TcpDiscoverySpi discoSpi = new TcpDiscoverySpi();
        discoSpi.setIpFinder(new TcpDiscoveryVmIpFinder(true));
        cfg.setDiscoverySpi(discoSpi);

        cfg.setMarshaller(new BinaryMarshaller());

        CacheConfiguration cacheCfg = new CacheConfiguration();

        cacheCfg.setName(null);
        cacheCfg.setCacheMode(CacheMode.PARTITIONED);
        cacheCfg.setAtomicityMode(CacheAtomicityMode.ATOMIC);
        cacheCfg.setWriteSynchronizationMode(CacheWriteSynchronizationMode.FULL_SYNC);
        cacheCfg.setRebalanceMode(CacheRebalanceMode.SYNC);

        CacheTypeMetadata meta = new CacheTypeMetadata();

        meta.setKeyType(Integer.class);
        meta.setValueType(Entity2.class);

        Map<String, Class<?>> qryFields = new HashMap<>();

        qryFields.put(FIELD_1, Integer.class);
        qryFields.put(FIELD_2, Integer.class);

        meta.setQueryFields(qryFields);

        cacheCfg.setTypeMetadata(Collections.singleton(meta));

        cfg.setCacheConfiguration(cacheCfg);

        ignite = Ignition.start(cfg);

        cache = ignite.cache(null);
    }

    /** {@inheritDoc} */
    @Override protected void afterTest() throws Exception {
        G.stopAll(true);

        ignite = null;
        cache = null;
    }

    /**
     * Test duplicate fields querying.
     *
     * @throws Exception If failed.
     */
    @SuppressWarnings("unchecked")
    public void testDuplicateFields() throws Exception {
        cache.put(1, new Entity2(10, 11));
        cache.put(2, new Entity2(20, 21));
        cache.put(3, new Entity2(30, 31));

        // Test first field.
        Iterator iter = cache.query(new SqlQuery(Entity2.class, FIELD_1 + "=20")).iterator();

        assert iter.hasNext();

        Cache.Entry<Integer, Entity2> res = (Cache.Entry)iter.next();

        assertEquals(2, (int)res.getKey());
        assertEquals(20, res.getValue().get1());
        assertEquals(21, res.getValue().get2());

        assert !iter.hasNext();

        // Test second field.
        iter = cache.query(new SqlQuery(Entity2.class, FIELD_2 + "=21")).iterator();

        assert iter.hasNext();

        res = (Cache.Entry)iter.next();

        assertEquals(2, (int)res.getKey());
        assertEquals(20, res.getValue().get1());
        assertEquals(21, res.getValue().get2());

        assert !iter.hasNext();

        iter = cache.query(
            new SqlFieldsQuery("SELECT p." + FIELD_1 + ", p." + FIELD_2 + " " +
                "FROM " + Entity2.class.getSimpleName() + " p " +
                "WHERE p." + FIELD_1 + "=20 AND p." + FIELD_2 + "=21")).iterator();

        assert iter.hasNext();

        List<Object> fieldsRes = (List<Object>)iter.next();

        assertEquals(20, fieldsRes.get(0));
        assertEquals(21, fieldsRes.get(1));

        assert !iter.hasNext();
    }

    /**
     * First entity.
     */
    private static class Entity1 {
        /** Value. */
        private int x;

        /**
         * Default constructor.
         */
        protected Entity1() {
            // No-op.
        }

        /**
         * Constructor.
         *
         * @param x Value.
         */
        protected Entity1(int x) {
            this.x = x;
        }

        /**
         * @return Value.
         */
        public int get1() {
            return x;
        }
    }

    private static class Entity2 extends Entity1 {
        /** Value. */
        private int x;

        /**
         * Default ctor.
         */
        public Entity2() {
            // No-op.
        }

        /**
         * Constructor.
         *
         * @param x1 X1.
         * @param x2 X2.
         */
        public Entity2(int x1, int x2) {
            super(x1);

            x = x2;
        }

        /**
         * @return Value.
         */
        public int get2() {
            return x;
        }
    }
}
