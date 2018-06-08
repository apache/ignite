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

package org.apache.ignite.compatibility.persistence;

import java.io.Serializable;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.cache.CacheAtomicityMode;
import org.apache.ignite.cache.CacheWriteSynchronizationMode;
import org.apache.ignite.cache.query.SqlFieldsQuery;
import org.apache.ignite.cache.query.annotations.QuerySqlField;
import org.apache.ignite.compatibility.testframework.junits.Dependency;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.DataRegionConfiguration;
import org.apache.ignite.configuration.DataStorageConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.processors.cache.GridCacheAbstractFullApiSelfTest;
import org.apache.ignite.lang.IgniteInClosure;
import org.apache.ignite.spi.discovery.tcp.TcpDiscoverySpi;
import org.jetbrains.annotations.NotNull;

/** Test to check that starting node with secondary index of the old format present doesn't break anything. */
public class IgniteSecondaryIndexesMigrationToLinksComparisonTest extends IgnitePersistenceCompatibilityAbstractTest {
    /** */
    private static final String TEST_CACHE_NAME = "testcache";

    /** Entries count. */
    private static final int ENTRIES = 100;

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String igniteInstanceName) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(igniteInstanceName);

        cfg.setPeerClassLoadingEnabled(false);

        DataStorageConfiguration memCfg = new DataStorageConfiguration()
            .setDefaultDataRegionConfiguration(
                new DataRegionConfiguration().setPersistenceEnabled(true)
                    .setInitialSize(1024 * 1024 * 10).setMaxSize(1024 * 1024 * 15))
            .setWalSegmentSize(1024 * 1024)
            .setSystemRegionInitialSize(1024 * 1024 * 10)
            .setSystemRegionMaxSize(1024 * 1024 * 15)
            .setWalHistorySize(200);

        cfg.setDataStorageConfiguration(memCfg);

        return cfg;
    }

    /** {@inheritDoc} */
    @NotNull @Override protected Collection<Dependency> getDependencies(String igniteVer) {
        Collection<Dependency> res = super.getDependencies(igniteVer);

        res.add(new Dependency("indexing", "ignite-indexing"));
        res.add(new Dependency("indexing", "ignite-indexing", true));

        return res;
    }

    /**
     * Tests opportunity to read data from previous Ignite DB version.
     *
     * @throws Exception If failed.
     */
    public void testSecondaryIndexesMigration_2_4() throws Exception {
        doTestStartupWithOldVersion("2.4.0");
    }

    /**
     * Tests opportunity to read data from previous Ignite DB version.
     *
     * @throws Exception If failed.
     */
    public void testSecondaryIndexesMigration_2_5() throws Exception {
        doTestStartupWithOldVersion("2.5.0");
    }

    /**
     * Tests opportunity to read data from previous Ignite DB version.
     *
     * @throws Exception If failed.
     */
    public void testSecondaryIndexesMigration_2_3() throws Exception {
        doTestStartupWithOldVersion("2.3.0");
    }

    /**
     * Tests opportunity to read data from previous Ignite DB version.
     *
     * @param ver 3-digits version of ignite
     * @throws Exception If failed.
     */
    @SuppressWarnings("unchecked")
    private void doTestStartupWithOldVersion(String ver) throws Exception {
        try {
            startGrid(1, ver, new ConfigurationClosure(), new PostStartupClosure());

            stopAllGrids();

            Ignite ignite = startGrid(0);

            ignite.active(true);

            IgniteCache<?, ?> cache = ignite.cache(TEST_CACHE_NAME);

            assertEquals((long)ENTRIES, query(cache, "select count(*) from Person").get(0).get(0));

            assertEquals(
                Arrays.asList(Arrays.asList("FirstName0", ENTRIES / 2L),
                Arrays.asList("FirstName1", ENTRIES / 2L)),
                query(cache, "select firstName, count(*) from Person group by firstName"));

            assertEquals(ENTRIES / 2L, query(cache, "select count(*) from Person where age >= 50").get(0).get(0));

            assertEquals(ENTRIES, query(cache, "select * from Person where firstName <> 'missing'").size());

            assertEquals(0, query(cache, "select * from Person where firstName = 'missing'").size());
        }
        finally {
            stopAllGrids();
        }
    }

    /** */
    private static List<List<?>> query(IgniteCache<?, ?> cache, String qry) {
        return cache.query(new SqlFieldsQuery(qry)).getAll();
    }

    /** */
    private static class PostStartupClosure implements IgniteInClosure<Ignite> {
        /** {@inheritDoc} */
        @Override public void apply(Ignite ignite) {
            ignite.active(true);

            CacheConfiguration<Integer, Person> cacheCfg = new CacheConfiguration<>(TEST_CACHE_NAME);
            cacheCfg.setAtomicityMode(CacheAtomicityMode.ATOMIC);
            cacheCfg.setBackups(0);
            cacheCfg.setWriteSynchronizationMode(CacheWriteSynchronizationMode.FULL_SYNC);

            cacheCfg.setIndexedTypes(Integer.class, Person.class);

            IgniteCache<Integer, Person> cache = ignite.getOrCreateCache(cacheCfg);

            for (int i = 0; i < ENTRIES; i++) {
                Person p = new Person();

                p.age(i);

                p.firstName("FirstName" + (i / (ENTRIES / 2)));

                p.secondName("SecondName" + (i / (ENTRIES / 2)));

                cache.put(i, p);
            }
        }
    }

    /** */
    private static class ConfigurationClosure implements IgniteInClosure<IgniteConfiguration> {
        /** {@inheritDoc} */
        @Override public void apply(IgniteConfiguration cfg) {
            cfg.setLocalHost("127.0.0.1");

            TcpDiscoverySpi disco = new TcpDiscoverySpi();
            disco.setIpFinder(GridCacheAbstractFullApiSelfTest.LOCAL_IP_FINDER);

            cfg.setDiscoverySpi(disco);

            cfg.setPeerClassLoadingEnabled(false);

            DataStorageConfiguration memCfg = new DataStorageConfiguration()
                .setDefaultDataRegionConfiguration(
                    new DataRegionConfiguration().setPersistenceEnabled(true)
                        .setInitialSize(1024 * 1024 * 10).setMaxSize(1024 * 1024 * 15))
                .setSystemRegionInitialSize(1024 * 1024 * 10)
                .setSystemRegionMaxSize(1024 * 1024 * 15);

            cfg.setDataStorageConfiguration(memCfg);
        }
    }

    /** */
    private final static class Person implements Serializable {
        /** */
        @QuerySqlField(orderedGroups = {@QuerySqlField.Group(name = "full_name_idx", order = 0)})
        private String firstName;

        /** */
        @QuerySqlField(orderedGroups = {@QuerySqlField.Group(name = "full_name_idx", order = 1)})
        private String secondName;

        /** */
        @QuerySqlField(index = true)
        private int age;

        /** */
        public String firstName() {
            return firstName;
        }

        /** */
        public void firstName(String firstName) {
            this.firstName = firstName;
        }

        /** */
        public String secondName() {
            return secondName;
        }

        /** */
        public void secondName(String secondName) {
            this.secondName = secondName;
        }

        /** */
        public int age() {
            return age;
        }

        /** */
        public void age(int age) {
            this.age = age;
        }
    }
}
