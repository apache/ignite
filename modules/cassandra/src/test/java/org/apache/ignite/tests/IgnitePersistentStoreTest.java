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

package org.apache.ignite.tests;

import java.util.Collection;
import java.util.Map;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.Ignition;
import org.apache.ignite.cache.CachePeekMode;
import org.apache.ignite.cache.store.CacheStore;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.internal.processors.cache.CacheEntryImpl;
import org.apache.ignite.tests.pojos.Person;
import org.apache.ignite.tests.pojos.PersonId;
import org.apache.ignite.tests.utils.CacheStoreHelper;
import org.apache.ignite.tests.utils.CassandraHelper;
import org.apache.ignite.tests.utils.TestsHelper;
import org.apache.log4j.Logger;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.springframework.core.io.ClassPathResource;

/**
 * Unit tests for Ignite caches which utilizing {@link org.apache.ignite.cache.store.cassandra.CassandraCacheStore}
 * to store cache data into Cassandra tables
 */
public class IgnitePersistentStoreTest {
    /** */
    private static final Logger LOGGER = Logger.getLogger(IgnitePersistentStoreTest.class.getName());

    /** */
    @BeforeClass
    public static void setUpClass() {
        if (CassandraHelper.useEmbeddedCassandra()) {
            try {
                CassandraHelper.startEmbeddedCassandra(LOGGER);
            }
            catch (Throwable e) {
                throw new RuntimeException("Failed to start embedded Cassandra instance", e);
            }
        }

        LOGGER.info("Testing admin connection to Cassandra");
        CassandraHelper.testAdminConnection();

        LOGGER.info("Testing regular connection to Cassandra");
        CassandraHelper.testRegularConnection();

        LOGGER.info("Dropping all artifacts from previous tests execution session");
        CassandraHelper.dropTestKeyspaces();

        LOGGER.info("Start tests execution");
    }

    /** */
    @AfterClass
    public static void tearDownClass() {
        try {
            CassandraHelper.dropTestKeyspaces();
        }
        finally {
            CassandraHelper.releaseCassandraResources();

            if (CassandraHelper.useEmbeddedCassandra()) {
                try {
                    CassandraHelper.stopEmbeddedCassandra();
                }
                catch (Throwable e) {
                    LOGGER.error("Failed to stop embedded Cassandra instance", e);
                }
            }
        }
    }

    /** */
    @Test
    public void primitiveStrategyTest() {
        Ignition.stopAll(true);

        Map<Long, Long> longMap = TestsHelper.generateLongsMap();
        Map<String, String> strMap = TestsHelper.generateStringsMap();

        LOGGER.info("Running PRIMITIVE strategy write tests");

        try (Ignite ignite = Ignition.start("org/apache/ignite/tests/persistence/primitive/ignite-config.xml")) {
            IgniteCache<Long, Long> longCache = ignite.getOrCreateCache(new CacheConfiguration<Long, Long>("cache1"));
            IgniteCache<String, String> strCache = ignite.getOrCreateCache(new CacheConfiguration<String, String>("cache2"));

            LOGGER.info("Running single operation write tests");
            longCache.put(1L, 1L);
            strCache.put("1", "1");
            LOGGER.info("Single operation write tests passed");

            LOGGER.info("Running bulk operation write tests");
            longCache.putAll(longMap);
            strCache.putAll(strMap);
            LOGGER.info("Bulk operation write tests passed");
        }

        LOGGER.info("PRIMITIVE strategy write tests passed");

        Ignition.stopAll(true);

        try (Ignite ignite = Ignition.start("org/apache/ignite/tests/persistence/primitive/ignite-config.xml")) {
            LOGGER.info("Running PRIMITIVE strategy read tests");

            IgniteCache<Long, Long> longCache = ignite.getOrCreateCache(new CacheConfiguration<Long, Long>("cache1"));
            IgniteCache<String, String> strCache = ignite.getOrCreateCache(new CacheConfiguration<String, String>("cache2"));

            LOGGER.info("Running single operation read tests");

            Long longVal = longCache.get(1L);
            if (!longVal.equals(longMap.get(1L)))
                throw new RuntimeException("Long value was incorrectly deserialized from Cassandra");

            String strVal = strCache.get("1");
            if (!strVal.equals(strMap.get("1")))
                throw new RuntimeException("String value was incorrectly deserialized from Cassandra");

            LOGGER.info("Single operation read tests passed");

            LOGGER.info("Running bulk operation read tests");

            Map<Long, Long> longMap1 = longCache.getAll(longMap.keySet());
            if (!TestsHelper.checkMapsEqual(longMap, longMap1))
                throw new RuntimeException("Long values batch was incorrectly deserialized from Cassandra");

            Map<String, String> strMap1 = strCache.getAll(strMap.keySet());
            if (!TestsHelper.checkMapsEqual(strMap, strMap1))
                throw new RuntimeException("String values batch was incorrectly deserialized from Cassandra");

            LOGGER.info("Bulk operation read tests passed");

            LOGGER.info("PRIMITIVE strategy read tests passed");

            LOGGER.info("Running PRIMITIVE strategy delete tests");

            longCache.remove(1L);
            longCache.removeAll(longMap.keySet());

            strCache.remove("1");
            strCache.removeAll(strMap.keySet());

            LOGGER.info("PRIMITIVE strategy delete tests passed");
        }
    }

    /** */
    @Test
    public void blobStrategyTest() {
        Ignition.stopAll(true);

        Map<Long, Long> longMap = TestsHelper.generateLongsMap();
        Map<Long, Person> personMap = TestsHelper.generateLongsPersonsMap();

        LOGGER.info("Running BLOB strategy write tests");

        try (Ignite ignite = Ignition.start("org/apache/ignite/tests/persistence/blob/ignite-config.xml")) {
            IgniteCache<Long, Long> longCache = ignite.getOrCreateCache(new CacheConfiguration<Long, Long>("cache1"));
            IgniteCache<Long, Person> personCache = ignite.getOrCreateCache(new CacheConfiguration<Long, Person>("cache2"));

            LOGGER.info("Running single operation write tests");
            longCache.put(1L, 1L);
            personCache.put(1L, TestsHelper.generateRandomPerson());
            LOGGER.info("Single operation write tests passed");

            LOGGER.info("Running bulk operation write tests");
            longCache.putAll(longMap);
            personCache.putAll(personMap);
            LOGGER.info("Bulk operation write tests passed");
        }

        LOGGER.info("BLOB strategy write tests passed");

        Ignition.stopAll(true);

        try (Ignite ignite = Ignition.start("org/apache/ignite/tests/persistence/blob/ignite-config.xml")) {
            LOGGER.info("Running BLOB strategy read tests");

            IgniteCache<Long, Long> longCache = ignite.getOrCreateCache(new CacheConfiguration<Long, Long>("cache1"));
            IgniteCache<Long, Person> personCache = ignite.getOrCreateCache(new CacheConfiguration<Long, Person>("cache2"));

            LOGGER.info("Running single operation read tests");

            Long longVal = longCache.get(1L);
            if (!longVal.equals(longMap.get(1L)))
                throw new RuntimeException("Long value was incorrectly deserialized from Cassandra");

            Person person = personCache.get(1L);
            if (!person.equals(personMap.get(1L)))
                throw new RuntimeException("Person value was incorrectly deserialized from Cassandra");

            LOGGER.info("Single operation read tests passed");

            LOGGER.info("Running bulk operation read tests");

            Map<Long, Long> longMap1 = longCache.getAll(longMap.keySet());
            if (!TestsHelper.checkMapsEqual(longMap, longMap1))
                throw new RuntimeException("Long values batch was incorrectly deserialized from Cassandra");

            Map<Long, Person> personMap1 = personCache.getAll(personMap.keySet());
            if (!TestsHelper.checkPersonMapsEqual(personMap, personMap1, false))
                throw new RuntimeException("Person values batch was incorrectly deserialized from Cassandra");

            LOGGER.info("Bulk operation read tests passed");

            LOGGER.info("BLOB strategy read tests passed");

            LOGGER.info("Running BLOB strategy delete tests");

            longCache.remove(1L);
            longCache.removeAll(longMap.keySet());

            personCache.remove(1L);
            personCache.removeAll(personMap.keySet());

            LOGGER.info("BLOB strategy delete tests passed");
        }
    }

    /** */
    @Test
    public void pojoStrategyTest() {
        Ignition.stopAll(true);

        LOGGER.info("Running POJO strategy write tests");

        Map<Long, Person> personMap1 = TestsHelper.generateLongsPersonsMap();
        Map<PersonId, Person> personMap2 = TestsHelper.generatePersonIdsPersonsMap();

        try (Ignite ignite = Ignition.start("org/apache/ignite/tests/persistence/pojo/ignite-config.xml")) {
            IgniteCache<Long, Person> personCache1 = ignite.getOrCreateCache(new CacheConfiguration<Long, Person>("cache1"));
            IgniteCache<PersonId, Person> personCache2 = ignite.getOrCreateCache(new CacheConfiguration<PersonId, Person>("cache2"));
            IgniteCache<PersonId, Person> personCache3 = ignite.getOrCreateCache(new CacheConfiguration<PersonId, Person>("cache3"));

            LOGGER.info("Running single operation write tests");
            personCache1.put(1L, TestsHelper.generateRandomPerson());
            personCache2.put(TestsHelper.generateRandomPersonId(), TestsHelper.generateRandomPerson());
            personCache3.put(TestsHelper.generateRandomPersonId(), TestsHelper.generateRandomPerson());
            LOGGER.info("Single operation write tests passed");

            LOGGER.info("Running bulk operation write tests");
            personCache1.putAll(personMap1);
            personCache2.putAll(personMap2);
            personCache3.putAll(personMap2);
            LOGGER.info("Bulk operation write tests passed");
        }

        LOGGER.info("POJO strategy write tests passed");

        Ignition.stopAll(true);

        try (Ignite ignite = Ignition.start("org/apache/ignite/tests/persistence/pojo/ignite-config.xml")) {
            LOGGER.info("Running POJO strategy read tests");

            IgniteCache<Long, Person> personCache1 = ignite.getOrCreateCache(new CacheConfiguration<Long, Person>("cache1"));
            IgniteCache<PersonId, Person> personCache2 = ignite.getOrCreateCache(new CacheConfiguration<PersonId, Person>("cache2"));
            IgniteCache<PersonId, Person> personCache3 = ignite.getOrCreateCache(new CacheConfiguration<PersonId, Person>("cache3"));

            LOGGER.info("Running single operation read tests");
            Person person = personCache1.get(1L);
            if (!person.equalsPrimitiveFields(personMap1.get(1L)))
                throw new RuntimeException("Person value was incorrectly deserialized from Cassandra");

            PersonId id = personMap2.keySet().iterator().next();

            person = personCache2.get(id);
            if (!person.equalsPrimitiveFields(personMap2.get(id)))
                throw new RuntimeException("Person value was incorrectly deserialized from Cassandra");

            person = personCache3.get(id);
            if (!person.equals(personMap2.get(id)))
                throw new RuntimeException("Person value was incorrectly deserialized from Cassandra");

            LOGGER.info("Single operation read tests passed");

            LOGGER.info("Running bulk operation read tests");

            Map<Long, Person> persons1 = personCache1.getAll(personMap1.keySet());
            if (!TestsHelper.checkPersonMapsEqual(persons1, personMap1, true))
                throw new RuntimeException("Persons values batch was incorrectly deserialized from Cassandra");

            Map<PersonId, Person> persons2 = personCache2.getAll(personMap2.keySet());
            if (!TestsHelper.checkPersonMapsEqual(persons2, personMap2, true))
                throw new RuntimeException("Person values batch was incorrectly deserialized from Cassandra");

            Map<PersonId, Person> persons3 = personCache3.getAll(personMap2.keySet());
            if (!TestsHelper.checkPersonMapsEqual(persons3, personMap2, false))
                throw new RuntimeException("Person values batch was incorrectly deserialized from Cassandra");

            LOGGER.info("Bulk operation read tests passed");

            LOGGER.info("POJO strategy read tests passed");

            LOGGER.info("Running POJO strategy delete tests");

            personCache1.remove(1L);
            personCache1.removeAll(personMap1.keySet());

            personCache2.remove(id);
            personCache2.removeAll(personMap2.keySet());

            personCache3.remove(id);
            personCache3.removeAll(personMap2.keySet());

            LOGGER.info("POJO strategy delete tests passed");
        }
    }

    /** */
    @Test
    public void loadCacheTest() {
        Ignition.stopAll(true);

        LOGGER.info("Running loadCache test");

        LOGGER.info("Filling Cassandra table with test data");

        CacheStore store = CacheStoreHelper.createCacheStore("personTypes",
            new ClassPathResource("org/apache/ignite/tests/persistence/pojo/persistence-settings-3.xml"),
            CassandraHelper.getAdminDataSrc());

        Collection<CacheEntryImpl<PersonId, Person>> entries = TestsHelper.generatePersonIdsPersonsEntries();

        store.writeAll(entries);

        LOGGER.info("Cassandra table filled with test data");

        LOGGER.info("Running loadCache test");

        try (Ignite ignite = Ignition.start("org/apache/ignite/tests/persistence/pojo/ignite-config.xml")) {
            IgniteCache<PersonId, Person> personCache3 = ignite.getOrCreateCache(new CacheConfiguration<PersonId, Person>("cache3"));
            int size = personCache3.size(CachePeekMode.ALL);

            LOGGER.info("Initial cache size " + size);

            LOGGER.info("Loading cache data from Cassandra table");

            personCache3.loadCache(null, new String[] {"select * from test1.pojo_test3 limit 3"});

            size = personCache3.size(CachePeekMode.ALL);
            if (size != 3) {
                throw new RuntimeException("Cache data was incorrectly loaded from Cassandra. " +
                    "Expected number of records is 3, but loaded number of records is " + size);
            }

            LOGGER.info("Cache data loaded from Cassandra table");
        }

        LOGGER.info("loadCache test passed");
    }
}
