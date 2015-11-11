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
    private static final Logger LOGGER = Logger.getLogger(IgnitePersistentStoreTest.class.getName());

    @BeforeClass
    public static void setUpClass() {
        if (CassandraHelper.getAdminPassword().isEmpty() || CassandraHelper.getRegularPassword().isEmpty()) {
            return;
        }

        LOGGER.info("Testing admin connection to Cassandra");
        CassandraHelper.testAdminConnection();
        LOGGER.info("Testing regular connection to Cassandra");
        CassandraHelper.testRegularConnection();
        LOGGER.info("Dropping all artifacts from previous tests execution session");
        CassandraHelper.dropTestKeyspaces();
        LOGGER.info("Start tests execution");
    }

    @AfterClass
    public static void tearDownClass() {
        if (CassandraHelper.getAdminPassword().isEmpty() || CassandraHelper.getRegularPassword().isEmpty()) {
            return;
        }

        try {
            CassandraHelper.dropTestKeyspaces();
        }
        finally {
            CassandraHelper.releaseCassandraResources();
        }
    }

    @Test
    public void primitiveStrategyTest() {
        if (CassandraHelper.getAdminPassword().isEmpty() || CassandraHelper.getRegularPassword().isEmpty()) {
            LOGGER.info("Cassandra passwords weren't specified thus skipping primitiveStrategyTest test");
            return;
        }

        Ignition.stopAll(true);

        Map<Integer, Integer> intMap = TestsHelper.generateIntegersMap();
        Map<String, String> strMap = TestsHelper.generateStringsMap();

        LOGGER.info("Running PRIMITIVE strategy write tests");

        try (Ignite ignite = Ignition.start("org/apache/ignite/tests/persistence/primitive/ignite-config.xml")) {
            IgniteCache<Integer, Integer> intCache = ignite.getOrCreateCache(new CacheConfiguration<Integer, Integer>("cache1"));
            IgniteCache<String, String> strCache = ignite.getOrCreateCache(new CacheConfiguration<String, String>("cache2"));

            LOGGER.info("Running single operation write tests");
            intCache.put(1, 1);
            strCache.put("1", "1");
            LOGGER.info("Single operation write tests passed");

            LOGGER.info("Running bulk operation write tests");
            intCache.putAll(intMap);
            strCache.putAll(strMap);
            LOGGER.info("Bulk operation write tests passed");
        }

        LOGGER.info("PRIMITIVE strategy write tests passed");

        Ignition.stopAll(true);

        try (Ignite ignite = Ignition.start("org/apache/ignite/tests/persistence/primitive/ignite-config.xml")) {
            LOGGER.info("Running PRIMITIVE strategy read tests");

            IgniteCache<Integer, Integer> intCache = ignite.getOrCreateCache(new CacheConfiguration<Integer, Integer>("cache1"));
            IgniteCache<String, String> strCache = ignite.getOrCreateCache(new CacheConfiguration<String, String>("cache2"));

            LOGGER.info("Running single operation read tests");

            Integer intVal = intCache.get(1);
            if (!intVal.equals(intMap.get(1)))
                throw new RuntimeException("Integer value was incorrectly deserialized from Cassandra");

            String strVal = strCache.get("1");
            if (!strVal.equals(strMap.get("1")))
                throw new RuntimeException("String value was incorrectly deserialized from Cassandra");

            LOGGER.info("Single operation read tests passed");

            LOGGER.info("Running bulk operation read tests");

            Map<Integer, Integer> intMap1 = intCache.getAll(intMap.keySet());
            if (!TestsHelper.checkMapsEqual(intMap, intMap1))
                throw new RuntimeException("Integer values batch was incorrectly deserialized from Cassandra");

            Map<String, String> strMap1 = strCache.getAll(strMap.keySet());
            if (!TestsHelper.checkMapsEqual(strMap, strMap1))
                throw new RuntimeException("String values batch was incorrectly deserialized from Cassandra");

            LOGGER.info("Bulk operation read tests passed");

            LOGGER.info("PRIMITIVE strategy read tests passed");

            LOGGER.info("Running PRIMITIVE strategy delete tests");

            intCache.remove(1);
            intCache.removeAll(intMap.keySet());

            strCache.remove("1");
            strCache.removeAll(strMap.keySet());

            LOGGER.info("PRIMITIVE strategy delete tests passed");
        }
    }

    @Test
    public void blobStrategyTest() {
        if (CassandraHelper.getAdminPassword().isEmpty() || CassandraHelper.getRegularPassword().isEmpty()) {
            LOGGER.info("Cassandra passwords weren't specified thus skipping blobStrategyTest test");
            return;
        }

        Ignition.stopAll(true);

        Map<Integer, Integer> intMap = TestsHelper.generateIntegersMap();
        Map<Integer, Person> personMap = TestsHelper.generateIntegersPersonsMap();

        LOGGER.info("Running BLOB strategy write tests");

        try (Ignite ignite = Ignition.start("org/apache/ignite/tests/persistence/blob/ignite-config.xml")) {
            IgniteCache<Integer, Integer> intCache = ignite.getOrCreateCache(new CacheConfiguration<Integer, Integer>("cache1"));
            IgniteCache<Integer, Person> personCache = ignite.getOrCreateCache(new CacheConfiguration<Integer, Person>("cache2"));

            LOGGER.info("Running single operation write tests");
            intCache.put(1, 1);
            personCache.put(1, TestsHelper.generateRandomPerson());
            LOGGER.info("Single operation write tests passed");

            LOGGER.info("Running bulk operation write tests");
            intCache.putAll(intMap);
            personCache.putAll(personMap);
            LOGGER.info("Bulk operation write tests passed");
        }

        LOGGER.info("BLOB strategy write tests passed");

        Ignition.stopAll(true);

        try (Ignite ignite = Ignition.start("org/apache/ignite/tests/persistence/blob/ignite-config.xml")) {
            LOGGER.info("Running BLOB strategy read tests");

            IgniteCache<Integer, Integer> intCache = ignite.getOrCreateCache(new CacheConfiguration<Integer, Integer>("cache1"));
            IgniteCache<Integer, Person> personCache = ignite.getOrCreateCache(new CacheConfiguration<Integer, Person>("cache2"));

            LOGGER.info("Running single operation read tests");

            Integer intVal = intCache.get(1);
            if (!intVal.equals(intMap.get(1)))
                throw new RuntimeException("Integer value was incorrectly deserialized from Cassandra");

            Person person = personCache.get(1);
            if (!person.equals(personMap.get(1)))
                throw new RuntimeException("Person value was incorrectly deserialized from Cassandra");

            LOGGER.info("Single operation read tests passed");

            LOGGER.info("Running bulk operation read tests");

            Map<Integer, Integer> intMap1 = intCache.getAll(intMap.keySet());
            if (!TestsHelper.checkMapsEqual(intMap, intMap1))
                throw new RuntimeException("Integer values batch was incorrectly deserialized from Cassandra");

            Map<Integer, Person> personMap1 = personCache.getAll(personMap.keySet());
            if (!TestsHelper.checkPersonMapsEqual(personMap, personMap1, false))
                throw new RuntimeException("Person values batch was incorrectly deserialized from Cassandra");

            LOGGER.info("Bulk operation read tests passed");

            LOGGER.info("BLOB strategy read tests passed");

            LOGGER.info("Running BLOB strategy delete tests");

            intCache.remove(1);
            intCache.removeAll(intMap.keySet());

            personCache.remove(1);
            personCache.removeAll(personMap.keySet());

            LOGGER.info("BLOB strategy delete tests passed");
        }
    }

    @Test
    public void pojoStrategyTest() {
        if (CassandraHelper.getAdminPassword().isEmpty() || CassandraHelper.getRegularPassword().isEmpty()) {
            LOGGER.info("Cassandra passwords weren't specified thus skipping pojoStrategyTest test");
            return;
        }

        Ignition.stopAll(true);

        LOGGER.info("Running POJO strategy write tests");

        Map<Integer, Person> personMap1 = TestsHelper.generateIntegersPersonsMap();
        Map<PersonId, Person> personMap2 = TestsHelper.generatePersonIdsPersonsMap();

        try (Ignite ignite = Ignition.start("org/apache/ignite/tests/persistence/pojo/ignite-config.xml")) {
            IgniteCache<Integer, Person> personCache1 = ignite.getOrCreateCache(new CacheConfiguration<Integer, Person>("cache1"));
            IgniteCache<PersonId, Person> personCache2 = ignite.getOrCreateCache(new CacheConfiguration<PersonId, Person>("cache2"));
            IgniteCache<PersonId, Person> personCache3 = ignite.getOrCreateCache(new CacheConfiguration<PersonId, Person>("cache3"));

            LOGGER.info("Running single operation write tests");
            personCache1.put(1, TestsHelper.generateRandomPerson());
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

            IgniteCache<Integer, Person> personCache1 = ignite.getOrCreateCache(new CacheConfiguration<Integer, Person>("cache1"));
            IgniteCache<PersonId, Person> personCache2 = ignite.getOrCreateCache(new CacheConfiguration<PersonId, Person>("cache2"));
            IgniteCache<PersonId, Person> personCache3 = ignite.getOrCreateCache(new CacheConfiguration<PersonId, Person>("cache3"));

            LOGGER.info("Running single operation read tests");
            Person person = personCache1.get(1);
            if (!person.equalsPrimitiveFields(personMap1.get(1)))
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

            Map<Integer, Person> persons1 = personCache1.getAll(personMap1.keySet());
            if (!TestsHelper.checkPersonMapsEqual(persons1, personMap1, true))
                throw new RuntimeException("Integer values batch was incorrectly deserialized from Cassandra");

            Map<PersonId, Person> persons2 = personCache2.getAll(personMap2.keySet());
            if (!TestsHelper.checkPersonMapsEqual(persons2, personMap2, true))
                throw new RuntimeException("Person values batch was incorrectly deserialized from Cassandra");

            Map<PersonId, Person> persons3 = personCache3.getAll(personMap2.keySet());
            if (!TestsHelper.checkPersonMapsEqual(persons3, personMap2, false))
                throw new RuntimeException("Person values batch was incorrectly deserialized from Cassandra");

            LOGGER.info("Bulk operation read tests passed");

            LOGGER.info("POJO strategy read tests passed");

            LOGGER.info("Running POJO strategy delete tests");

            personCache1.remove(1);
            personCache1.removeAll(personMap1.keySet());

            personCache2.remove(id);
            personCache2.removeAll(personMap2.keySet());

            personCache3.remove(id);
            personCache3.removeAll(personMap2.keySet());

            LOGGER.info("POJO strategy delete tests passed");
        }
    }

    @Test
    public void loadCacheTest() {
        if (CassandraHelper.getAdminPassword().isEmpty() || CassandraHelper.getRegularPassword().isEmpty()) {
            LOGGER.info("Cassandra passwords weren't specified thus skipping pojoStrategyTest test");
            return;
        }

        Ignition.stopAll(true);

        LOGGER.info("Running loadCache test");

        LOGGER.info("Filling Cassandra table with test data");

        CacheStore store = CacheStoreHelper.createCacheStore("personTypes",
            new ClassPathResource("org/apache/ignite/tests/persistence/pojo/persistence-settings-3.xml"),
            CassandraHelper.getAdminDataSource());

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
