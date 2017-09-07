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

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.configuration.BinaryConfiguration;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.configuration.PersistentStoreConfiguration;
import org.apache.ignite.internal.binary.BinaryMarshaller;
import org.apache.ignite.spi.discovery.tcp.TcpDiscoverySpi;
import org.apache.ignite.spi.discovery.tcp.ipfinder.TcpDiscoveryIpFinder;
import org.apache.ignite.spi.discovery.tcp.ipfinder.vm.TcpDiscoveryVmIpFinder;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;

import static org.apache.ignite.binary.BinaryStringEncoding.ENC_NAME_WINDOWS_1251;
import static org.apache.ignite.cache.CacheAtomicityMode.TRANSACTIONAL;
import static org.apache.ignite.cache.CacheWriteSynchronizationMode.FULL_SYNC;

/**
 * Persistence tests for data with custom binary string encoding.
 */
public class CacheBinaryStringEncodingPersistenceTest extends GridCommonAbstractTest {
    /** */
    private static final TcpDiscoveryIpFinder IP_FINDER = new TcpDiscoveryVmIpFinder(true);

    /** */
    private static final String CACHE_NAME = "organization";

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String igniteInstanceName) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(igniteInstanceName);

        cfg.setDiscoverySpi(new TcpDiscoverySpi().setIpFinder(IP_FINDER));

        cfg.setClientMode(false);

        cfg.setPersistentStoreConfiguration(new PersistentStoreConfiguration());

        cfg.setCacheConfiguration(
            new CacheConfiguration<Integer, City>(CACHE_NAME)
                .setBackups(1)
                .setAtomicityMode(TRANSACTIONAL)
                .setWriteSynchronizationMode(FULL_SYNC)
                .setIndexedTypes(Integer.class, City.class)
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
    public void testPersistence() throws Exception {
        Ignite ignite = startGrid(0);

        ignite.active(true);

        IgniteCache<Integer, City> cache = ignite.cache(CACHE_NAME).withKeepBinary();

        cache.put(1, new City(1, "Новгород"));
        cache.put(2, new City(2, "Chicago"));
        cache.put(3, new City(3, "Düsseldorf"));
        cache.put(4, new City(4, "北京市"));

        ignite.close();

        ignite = startGrid(0);

        ignite.active(true);

        cache = ignite.cache(CACHE_NAME);

        System.out.println(cache.get(1));
        System.out.println(cache.get(2));
        System.out.println(cache.get(3));
        System.out.println(cache.get(4));

        cache.clear();

        ignite.close();
    }

    /** */
    public void testReversiblePersistAndRestore() throws Exception {
        List<String> names = Arrays.asList("Новгород", "Chicago");

        assertEqualsCollections(names, persistAndRestore(names));
    }

    /** */
    public void testIrreversiblePersistAndRestore() throws Exception {
        List<String> names = Arrays.asList("Düsseldorf", "北京市");

        for (Iterator<String> iter1 = names.iterator(), iter2 = persistAndRestore(names).iterator(); iter1.hasNext();)
            assertFalse(iter1.next().equals(iter2.next()));
    }

    /** */
    private List<String> persistAndRestore(List<String> cityNames) throws Exception {
        Ignite ignite = startGrid(0);

        ignite.active(true);

        IgniteCache<Integer, City> cache = ignite.cache(CACHE_NAME).withKeepBinary();

        for (int i = 0; i < cityNames.size(); i++)
            cache.put(i, new City(i, cityNames.get(i)));

        ignite.close();

        ignite = startGrid(0);

        ignite.active(true);

        cache = ignite.cache(CACHE_NAME);

        List<String> result = new ArrayList<>(0);

        for (int i = 0; i < cityNames.size(); i++) {
            City city = cache.get(i);

            assertNotNull(city);
            assertNotNull(city.name());

            result.add(city.name());
        }

        cache.clear();

        ignite.close();

        return result;
    }

    // TODO IGNITE-5655: SQL tests

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
