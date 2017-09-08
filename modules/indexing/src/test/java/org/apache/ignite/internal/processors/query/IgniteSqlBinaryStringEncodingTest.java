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

package org.apache.ignite.internal.processors.query;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.cache.QueryEntity;
import org.apache.ignite.cache.query.QueryCursor;
import org.apache.ignite.cache.query.SqlFieldsQuery;
import org.apache.ignite.configuration.BinaryConfiguration;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.binary.BinaryMarshaller;
import org.apache.ignite.internal.processors.cache.CacheBinaryStringEncodingPersistenceTest;
import org.apache.ignite.spi.discovery.tcp.TcpDiscoverySpi;
import org.apache.ignite.spi.discovery.tcp.ipfinder.TcpDiscoveryIpFinder;
import org.apache.ignite.spi.discovery.tcp.ipfinder.vm.TcpDiscoveryVmIpFinder;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;

import static org.apache.ignite.binary.BinaryStringEncoding.ENC_NAME_WINDOWS_1251;

/**
 * SQL tests for data with custom binary string encoding.
 */
public class IgniteSqlBinaryStringEncodingTest extends GridCommonAbstractTest {
    /** */
    private static final TcpDiscoveryIpFinder IP_FINDER = new TcpDiscoveryVmIpFinder(true);

    /** */
    private static final String CACHE_NAME = "city";

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String igniteInstanceName) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(igniteInstanceName);

        cfg.setDiscoverySpi(new TcpDiscoverySpi().setIpFinder(IP_FINDER));

        cfg.setClientMode(false);

        QueryEntity entity = new QueryEntity();

        entity.setKeyType(Integer.class.getName());
        entity.setValueType(City.class.getName());

        LinkedHashMap<String, String> fields = new LinkedHashMap<>();
        fields.put("id", Integer.class.getName());
        fields.put("name", String.class.getName());

        entity.setFields(fields);

        cfg.setCacheConfiguration(
            new CacheConfiguration<Integer, CacheBinaryStringEncodingPersistenceTest.City>(CACHE_NAME)
                .setQueryEntities(Collections.singletonList(entity))
        );

        BinaryConfiguration bCfg = cfg.getBinaryConfiguration();

        if (bCfg == null)
            bCfg = new BinaryConfiguration();

        cfg.setBinaryConfiguration(bCfg
            .setEncoding(ENC_NAME_WINDOWS_1251)
            .setCompactFooter(false)
        );

        return cfg.setMarshaller(new BinaryMarshaller());
    }

    /** */
    public void testReversiblePersistAndRestore() throws Exception {
        List<String> names = Arrays.asList("Новгород", "Chicago");

        assertEqualsCollections(names, putThenGet(names));
    }

    /** */
    public void testIrreversiblePersistAndRestore() throws Exception {
        List<String> names = Arrays.asList("Düsseldorf", "北京市");

        for (Iterator<String> iter1 = names.iterator(), iter2 = putThenGet(names).iterator(); iter1.hasNext();)
            assertFalse(iter1.next().equals(iter2.next()));
    }

    /** */
    private List<String> putThenGet(List<String> cityNames) throws Exception {
        try {
            Ignite ignite = startGrid(0);

            IgniteCache<Integer, CacheBinaryStringEncodingPersistenceTest.City> cache = ignite.cache(CACHE_NAME);

            for (int i = 0; i < cityNames.size(); i++)
                cache.query(new SqlFieldsQuery("insert into City (_key, _val) values (?, ?)")
                    .setArgs(i, new City(i, cityNames.get(i))));

            List<String> result = new ArrayList<>(0);

            QueryCursor<List<?>> cur = cache.query(new SqlFieldsQuery("select id, name from City"));

            for (List<?> rec : cur) {
                assertEquals(2, rec.size());

                assertTrue(rec.get(1) instanceof String);

                result.add((String)rec.get(1));
            }

            return result;
        } finally {
            stopAllGrids();
        }
    }

    public static class City {
        /** */
        private int id;

        /** */
        private String name;

        /**
         * Required for binary deserialization.
         */
        public City() {
            // No-op.
        }

        /** */
        public City(int id, String name) {
            this.id = id;
            this.name = name;
        }

        /** */
        public int id() {
            return id;
        }

        /** */
        public void id(int id) {
            this.id = id;
        }

        /** */
        public String name() {
            return name;
        }

        /** */
        public void name(String name) {
            this.name = name;
        }

        /** */
        @Override public String toString() {
            return "City [id=" + id + ", name=" + name + ']';
        }
    }
}
